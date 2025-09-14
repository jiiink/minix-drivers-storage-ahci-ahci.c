/* Advanced Host Controller Interface (AHCI) driver, by D.C. van Moolenbroek
 * - Multithreading support by Arne Welzel
 * - Native Command Queuing support by Raja Appuswamy
 */
/*
 * This driver is based on the following specifications:
 * - Serial ATA Advanced Host Controller Interface (AHCI) 1.3
 * - Serial ATA Revision 2.6
 * - AT Attachment with Packet Interface 7 (ATA/ATAPI-7)
 * - ATAPI Removable Rewritable Media Devices 1.3 (SFF-8070)
 *
 * The driver supports device hot-plug, active device status tracking,
 * nonremovable ATA and removable ATAPI devices, custom logical sector sizes,
 * sector-unaligned reads, native command queuing and parallel requests to
 * different devices.
 *
 * It does not implement transparent failure recovery, power management, or
 * port multiplier support.
 */
/*
 * An AHCI controller exposes a number of ports (up to 32), each of which may
 * or may not have one device attached (port multipliers are not supported).
 * Each port is maintained independently.
 *
 * The following figure depicts the possible transitions between port states.
 * The NO_PORT state is not included; no transitions can be made from or to it.
 *
 *   +----------+                      +----------+
 *   | SPIN_UP  | ------+      +-----> | BAD_DEV  | ------------------+
 *   +----------+       |      |       +----------+                   |
 *        |             |      |            ^                         |
 *        v             v      |            |                         |
 *   +----------+     +----------+     +----------+     +----------+  |
 *   |  NO_DEV  | --> | WAIT_DEV | --> | WAIT_ID  | --> | GOOD_DEV |  |
 *   +----------+     +----------+     +----------+     +----------+  |
 *        ^                |                |                |        |
 *        +----------------+----------------+----------------+--------+
 *
 * At driver startup, all physically present ports are put in SPIN_UP state.
 * This state differs from NO_DEV in that BDEV_OPEN calls will be deferred
 * until either the spin-up timer expires, or a device has been identified on
 * that port. This prevents early BDEV_OPEN calls from failing erroneously at
 * startup time if the device has not yet been able to announce its presence.
 *
 * If a device is detected, either at startup time or after hot-plug, its
 * signature is checked and it is identified, after which it may be determined
 * to be a usable ("good") device, which means that the device is considered to
 * be in a working state. If these steps fail, the device is marked as unusable
 * ("bad"). At any point in time, the device may be disconnected; the port is
 * then put back into NO_DEV state.
 *
 * A device in working state (GOOD_DEV) may or may not have a medium. All ATA
 * devices are assumed to be fixed; all ATAPI devices are assumed to have
 * removable media. To prevent erroneous access to switched devices and media,
 * the driver makes devices inaccessible until they are fully closed (the open
 * count is zero) when a device (hot-plug) or medium change is detected.
 * For hot-plug changes, access is prevented by setting the BARRIER flag until
 * the device is fully closed and then reopened. For medium changes, access is
 * prevented by not acknowledging the medium change until the device is fully
 * closed and reopened. Removable media are not locked in the drive while
 * opened, because the driver author is uncomfortable with that concept.
 *
 * Ports may leave the group of states where a device is connected (that is,
 * WAIT_ID, GOOD_DEV, and BAD_DEV) in two ways: either due to a hot-unplug
 * event, or due to a hard reset after a serious failure. For simplicity, we
 * we perform a hard reset after a hot-unplug event as well, so that the link
 * to the device is broken. Thus, in both cases, a transition to NO_DEV is
 * made, after which the link to the device may or may not be reestablished.
 * In both cases, ongoing requests are cancelled and the BARRIER flag is set.
 *
 * The following table lists for each state, whether the port is started
 * (PxCMD.ST is set), whether a timer is running, what the PxIE mask is to be
 * set to, and what BDEV_OPEN calls on this port should return.
 *
 *   State       Started     Timer       PxIE        BDEV_OPEN
 *   ---------   ---------   ---------   ---------   ---------
 *   NO_PORT     no          no          (none)      ENXIO
 *   SPIN_UP     no          yes         PCE         (wait)
 *   NO_DEV      no          no          PCE         ENXIO
 *   WAIT_DEV    no          yes         PCE         (wait)
 *   BAD_DEV     no          no          PRCE        ENXIO
 *   WAIT_ID     yes         yes         PRCE+       (wait)
 *   GOOD_DEV    yes         per-command PRCE+       OK
 *
 * In order to continue deferred BDEV_OPEN calls, the BUSY flag must be unset
 * when changing from SPIN_UP to any state but WAIT_DEV, and when changing from
 * WAIT_DEV to any state but WAIT_ID, and when changing from WAIT_ID to any
 * other state.
 */
/*
 * The maximum byte size of a single transfer (MAX_TRANSFER) is currently set
 * to 4MB. This limit has been chosen for a number of reasons:
 * - The size that can be specified in a Physical Region Descriptor (PRD) is
 *   limited to 4MB for AHCI. Limiting the total transfer size to at most this
 *   size implies that no I/O vector element needs to be split up across PRDs.
 *   This means that the maximum number of needed PRDs can be predetermined.
 * - The limit is below what can be transferred in a single ATA request, namely
 *   64k sectors (i.e., at least 32MB). This means that transfer requests need
 *   never be split up into smaller chunks, reducing implementation complexity.
 * - A single, static timeout can be used for transfers. Very large transfers
 *   can legitimately take up to several minutes -- well beyond the appropriate
 *   timeout range for small transfers. The limit obviates the need for a
 *   timeout scheme that takes into account the transfer size.
 * - Similarly, the transfer limit reduces the opportunity for buggy/malicious
 *   clients to keep the driver busy for a long time with a single request.
 * - The limit is high enough for all practical purposes. The transfer setup
 *   overhead is already relatively negligible at this size, and even larger
 *   requests will not help maximize throughput. As NR_IOREQS is currently set
 *   to 64, the limit still allows file systems to perform I/O requests with
 *   vectors completely filled with 64KB-blocks.
 */
#include <minix/drivers.h>
#include <minix/blockdriver_mt.h>
#include <minix/drvlib.h>
#include <machine/pci.h>
#include <sys/ioc_disk.h>
#include <sys/mman.h>
#include <assert.h>

#include "ahci.h"

/* Host Bus Adapter (HBA) state. */
static struct {
	volatile u32_t *base;	/* base address of memory-mapped registers */
	size_t size;		/* size of memory-mapped register area */

	int nr_ports;		/* addressable number of ports (1..NR_PORTS) */
	int nr_cmds;		/* maximum number of commands per port */
	int has_ncq;		/* NCQ support flag */
	int has_clo;		/* CLO support flag */

	int irq;		/* IRQ number */
	int hook_id;		/* IRQ hook ID */
} hba_state;

#define hba_read(r)		(hba_state.base[r])
#define hba_write(r, v)		(hba_state.base[r] = (v))

/* Port state. */
static struct port_state {
	int state;		/* port state */
	unsigned int flags;	/* port flags */

	volatile u32_t *reg;	/* memory-mapped port registers */

	u8_t *mem_base;		/* primary memory buffer virtual address */
	phys_bytes mem_phys;	/* primary memory buffer physical address */
	vir_bytes mem_size;	/* primary memory buffer size */

	/* the FIS, CL, CT[0] and TMP buffers are all in the primary buffer */
	u32_t *fis_base;	/* FIS receive buffer virtual address */
	phys_bytes fis_phys;	/* FIS receive buffer physical address */
	u32_t *cl_base;		/* command list buffer virtual address */
	phys_bytes cl_phys;	/* command list buffer physical address */
	u8_t *ct_base[NR_CMDS];	/* command table virtual address */
	phys_bytes ct_phys[NR_CMDS];	/* command table physical address */
	u8_t *tmp_base;		/* temporary storage buffer virtual address */
	phys_bytes tmp_phys;	/* temporary storage buffer physical address */

	u8_t *pad_base;		/* sector padding buffer virtual address */
	phys_bytes pad_phys;	/* sector padding buffer physical address */
	vir_bytes pad_size;	/* sector padding buffer size */

	u64_t lba_count;	/* number of valid Logical Block Addresses */
	u32_t sector_size;	/* medium sector size in bytes */

	int open_count;		/* number of times this port is opened */

	int device;		/* associated device number, or NO_DEVICE */
	struct device part[DEV_PER_DRIVE];	/* partition bases and sizes */
	struct device subpart[SUB_PER_DRIVE];	/* same for subpartitions */

	minix_timer_t timer;		/* port-specific timeout timer */
	int left;		/* number of tries left before giving up */
				/* (only used for signature probing) */

	int queue_depth;	/* NCQ queue depth */
	u32_t pend_mask;	/* commands not yet complete */
	struct {
		thread_id_t tid;/* ID of the worker thread */
		minix_timer_t timer;	/* timer associated with each request */
		int result;	/* success/failure result of the commands */
	} cmd_info[NR_CMDS];
} port_state[NR_PORTS];

#define port_read(ps, r)	((ps)->reg[r])
#define port_write(ps, r, v)	((ps)->reg[r] = (v))

static int ahci_instance;			/* driver instance number */

static int ahci_verbose;			/* verbosity level (0..4) */

/* Timeout-related values. */
static clock_t ahci_spinup_timeout;
static clock_t ahci_device_timeout;
static clock_t ahci_device_delay;
static unsigned int ahci_device_checks;
static clock_t ahci_command_timeout;
static clock_t ahci_transfer_timeout;
static clock_t ahci_flush_timeout;

/* Timeout environment variable names and default values. */
static struct {
	char *name;				/* environment variable name */
	u32_t default_ms;			/* default in milliseconds */
	clock_t *ptr;				/* clock ticks value pointer */
} ahci_timevar[] = {
	{ "ahci_init_timeout",   SPINUP_TIMEOUT,    &ahci_spinup_timeout   },
	{ "ahci_device_timeout", DEVICE_TIMEOUT,    &ahci_device_timeout   },
	{ "ahci_cmd_timeout",    COMMAND_TIMEOUT,   &ahci_command_timeout  },
	{ "ahci_io_timeout",     TRANSFER_TIMEOUT,  &ahci_transfer_timeout },
	{ "ahci_flush_timeout",  FLUSH_TIMEOUT,     &ahci_flush_timeout    }
};

static int ahci_map[MAX_DRIVES];		/* device-to-port mapping */

static int ahci_exiting = FALSE;		/* exit after last close? */

#define BUILD_ARG(port, tag)	(((port) << 8) | (tag))
#define GET_PORT(arg)		((arg) >> 8)
#define GET_TAG(arg)		((arg) & 0xFF)

#define dprintf(v,s) do {		\
	if (ahci_verbose >= (v))	\
		printf s;		\
} while (0)

/* Convert milliseconds to clock ticks. Round up. */
#define millis_to_hz(ms)	(((ms) * sys_hz() + 999) / 1000)

static void port_set_cmd(struct port_state *ps, int cmd, cmd_fis_t *fis,
	u8_t packet[ATAPI_PACKET_SIZE], prd_t *prdt, int nr_prds, int write);
static void port_issue(struct port_state *ps, int cmd, clock_t timeout);
static int port_exec(struct port_state *ps, int cmd, clock_t timeout);
static void port_timeout(int arg);
static void port_disconnect(struct port_state *ps);

static char *ahci_portname(struct port_state *ps);
static int ahci_open(devminor_t minor, int access);
static int ahci_close(devminor_t minor);
static ssize_t ahci_transfer(devminor_t minor, int do_write, u64_t position,
	endpoint_t endpt, iovec_t *iovec, unsigned int count, int flags);
static struct device *ahci_part(devminor_t minor);
static void ahci_alarm(clock_t stamp);
static int ahci_ioctl(devminor_t minor, unsigned long request,
	endpoint_t endpt, cp_grant_id_t grant, endpoint_t user_endpt);
static void ahci_intr(unsigned int mask);
static int ahci_device(devminor_t minor, device_id_t *id);
static struct port_state *ahci_get_port(devminor_t minor);

/* AHCI driver table. */
static struct blockdriver ahci_dtab = {
	.bdr_type	= BLOCKDRIVER_TYPE_DISK,
	.bdr_open	= ahci_open,
	.bdr_close	= ahci_close,
	.bdr_transfer	= ahci_transfer,
	.bdr_ioctl	= ahci_ioctl,
	.bdr_part	= ahci_part,
	.bdr_intr	= ahci_intr,
	.bdr_alarm	= ahci_alarm,
	.bdr_device	= ahci_device
};

/*===========================================================================*
 *				atapi_exec				     *
 *===========================================================================*/
static int atapi_exec(struct port_state *ps, int cmd, u8_t packet[ATAPI_PACKET_SIZE], size_t size, int write) {
    if (size > AHCI_TMP_SIZE) {
        return ERROR_CODE; // Define and use an appropriate error code
    }

    cmd_fis_t fis;
    memset(&fis, 0, sizeof(fis));
    fis.cf_cmd = ATA_CMD_PACKET;

    prd_t prd[1] = {0};
    int nr_prds = 0;

    if (size > 0) {
        fis.cf_feat = ATA_FEAT_PACKET_DMA;
        if (!write && (ps->flags & FLAG_USE_DMADIR)) {
            fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;
        }

        prd[0].vp_addr = ps->tmp_phys;
        prd[0].vp_size = size;
        nr_prds = 1;
    }

    if (port_set_cmd(ps, cmd, &fis, packet, prd, nr_prds, write) != OK) {
        return ERROR_CODE; // Define and use an appropriate error code
    }

    return port_exec(ps, cmd, ahci_command_timeout);
}

/*===========================================================================*
 *				atapi_test_unit				     *
 *===========================================================================*/
#include <string.h>
#include <stdint.h>

#define ATAPI_PACKET_SIZE 12
#define ATAPI_CMD_TEST_UNIT 0x00
#define FALSE 0

struct port_state;

int atapi_exec(struct port_state *ps, int cmd, uint8_t *packet, int param, int flag);

static int atapi_test_unit(struct port_state *ps, int cmd)
{
    uint8_t packet[ATAPI_PACKET_SIZE] = {0};
    packet[0] = ATAPI_CMD_TEST_UNIT;
    return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_request_sense			     *
 *===========================================================================*/
#include <string.h>
#include <stdio.h>

static int atapi_request_sense(struct port_state *ps, int cmd, int *sense) {
    u8_t packet[ATAPI_PACKET_SIZE] = {0};

    packet[0] = ATAPI_CMD_REQUEST_SENSE;
    packet[4] = ATAPI_REQUEST_SENSE_LEN;

    int result = atapi_exec(ps, cmd, packet, ATAPI_REQUEST_SENSE_LEN, 0);
    if (result != OK) {
        return result;
    }

    *sense = ps->tmp_base[2] & 0xF;
    dprintf(V_REQ, ("%s: ATAPI SENSE: sense %x ASC %x ASCQ %x\n",
                    ahci_portname(ps), *sense, ps->tmp_base[12], ps->tmp_base[13]));

    return OK;
}

/*===========================================================================*
 *				atapi_load_eject			     *
 *===========================================================================*/
int atapi_load_eject(struct port_state *ps, int cmd, int load) {
    u8_t packet[ATAPI_PACKET_SIZE] = {0};

    packet[0] = ATAPI_CMD_START_STOP;
    packet[4] = load ? ATAPI_START_STOP_LOAD : ATAPI_START_STOP_EJECT;

    return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_read_capacity			     *
 *===========================================================================*/
static int atapi_read_capacity(struct port_state *ps, int cmd) {
    u8_t packet[ATAPI_PACKET_SIZE] = {0}, *buf;
    int r;

    packet[0] = ATAPI_CMD_READ_CAPACITY;

    r = atapi_exec(ps, cmd, packet, ATAPI_READ_CAPACITY_LEN, FALSE);
    if (r != OK) {
        return r;
    }

    buf = ps->tmp_base;
    ps->lba_count = ((u64_t)buf[0] << 24) | ((u64_t)buf[1] << 16) | ((u64_t)buf[2] << 8) | buf[3] + 1;
    ps->sector_size = (buf[4] << 24) | (buf[5] << 16) | (buf[6] << 8) | buf[7];

    if (ps->sector_size == 0 || (ps->sector_size & 1)) {
        dprintf(V_ERR, "%s: invalid medium sector size %u\n", ahci_portname(ps), ps->sector_size);
        return EINVAL;
    }

    dprintf(V_INFO, "%s: medium detected (%u byte sectors, %llu MB size)\n", ahci_portname(ps), ps->sector_size,
        (unsigned long long)(ps->lba_count * ps->sector_size / (1024 * 1024)));

    return OK;
}

/*===========================================================================*
 *				atapi_check_medium			     *
 *===========================================================================*/
static int atapi_check_medium(struct port_state *ps, int cmd)
{
	int sense;
	
	/* Perform readiness check. */
	if (atapi_test_unit(ps, cmd) != OK) {
		ps->flags &= ~FLAG_HAS_MEDIUM;

		if (atapi_request_sense(ps, cmd, &sense) == OK && sense == ATAPI_SENSE_UNIT_ATT) {
			if (atapi_read_capacity(ps, cmd) == OK) {
				ps->flags |= FLAG_HAS_MEDIUM;
				return OK;
			}
			return EIO;
		}

		return ENXIO;
	}

	/* Check if a medium is already detected. */
	if (!(ps->flags & FLAG_HAS_MEDIUM)) {
		if (atapi_read_capacity(ps, cmd) != OK) {
			return EIO;
		}
		ps->flags |= FLAG_HAS_MEDIUM;
	}

	return OK;
}

/*===========================================================================*
 *				atapi_id_check				     *
 *===========================================================================*/
static int atapi_id_check(struct port_state *ps, u16_t *buf) {
    if ((buf[ATA_ID_GCAP] & ATA_ID_GCAP_ATAPI_MASK) != ATA_ID_GCAP_ATAPI ||
        !(buf[ATA_ID_GCAP] & ATA_ID_GCAP_REMOVABLE) ||
        (buf[ATA_ID_CAP] & ATA_ID_CAP_DMA) != ATA_ID_CAP_DMA &&
        (buf[ATA_ID_DMADIR] & (ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA)) != (ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA)) {
        
        dprintf(V_ERR, "%s: unsupported ATAPI device\n", ahci_portname(ps));
        dprintf(V_DEV, "%s: GCAP %04x CAP %04x DMADIR %04x\n", ahci_portname(ps), buf[ATA_ID_GCAP], buf[ATA_ID_CAP], buf[ATA_ID_DMADIR]);
        return FALSE;
    }

    if (buf[ATA_ID_DMADIR] & ATA_ID_DMADIR_DMADIR) {
        ps->flags |= FLAG_USE_DMADIR;
    }

    if (((buf[ATA_ID_GCAP] & ATA_ID_GCAP_TYPE_MASK) >> ATA_ID_GCAP_TYPE_SHIFT) == ATAPI_TYPE_CDROM) {
        ps->flags |= FLAG_READONLY;
    }

    if ((buf[ATA_ID_SUP1] & ATA_ID_SUP1_VALID_MASK) == ATA_ID_SUP1_VALID && !(ps->flags & FLAG_READONLY)) {
        if (buf[ATA_ID_SUP0] & ATA_ID_SUP0_WCACHE) {
            ps->flags |= FLAG_HAS_WCACHE;
        }
        if (buf[ATA_ID_SUP1] & ATA_ID_SUP1_FLUSH) {
            ps->flags |= FLAG_HAS_FLUSH;
        }
    }

    return TRUE;
}

/*===========================================================================*
 *				atapi_transfer				     *
 *===========================================================================*/
int atapi_transfer(struct port_state *ps, int cmd, u64_t start_lba,
                   unsigned int count, int write, prd_t *prdt, int nr_prds) {
    cmd_fis_t fis = {0};
    u8_t packet[ATAPI_PACKET_SIZE] = {0};

    fis.cf_cmd = ATA_CMD_PACKET;
    fis.cf_feat = ATA_FEAT_PACKET_DMA;
    if (!write && (ps->flags & FLAG_USE_DMADIR)) {
        fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;
    }

    packet[0] = write ? ATAPI_CMD_WRITE : ATAPI_CMD_READ;

    for (int i = 0; i < 4; ++i) {
        packet[2 + i] = (start_lba >> (24 - 8 * i)) & 0xFF;
        packet[6 + i] = (count >> (24 - 8 * i)) & 0xFF;
    }

    if (port_set_cmd(ps, cmd, &fis, packet, prdt, nr_prds, write) != 0) {
        return -1;
    }

    return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				ata_id_check				     *
 *===========================================================================*/
#include <stdbool.h>
#include <stdint.h>

#define ATA_ID_GCAP 0
#define ATA_ID_GCAP_ATA_MASK 0x0001
#define ATA_ID_GCAP_REMOVABLE 0x0002
#define ATA_ID_GCAP_INCOMPLETE 0x0004
#define ATA_ID_GCAP_ATA 0x0001

#define ATA_ID_CAP 1
#define ATA_ID_CAP_LBA 0x0002
#define ATA_ID_CAP_DMA 0x0004

#define ATA_ID_SUP1 2
#define ATA_ID_SUP1_VALID_MASK 0x0008
#define ATA_ID_SUP1_FLUSH 0x0010
#define ATA_ID_SUP1_LBA48 0x0020

#define ATA_ID_LBA0 3
#define ATA_ID_LBA1 4
#define ATA_ID_LBA2 5
#define ATA_ID_LBA3 6

#define ATA_ID_SATA_CAP 7
#define ATA_ID_SATA_CAP_NCQ 0x0040

#define ATA_ID_QDEPTH 8
#define ATA_ID_QDEPTH_MASK 0x001F

#define ATA_ID_PLSS 9
#define ATA_ID_PLSS_VALID_MASK 0x0100
#define ATA_ID_PLSS_LLS 0x0200

#define ATA_ID_LSS0 10
#define ATA_ID_LSS1 11

#define ATA_SECTOR_SIZE 512

#define ATA_ID_SUP0 12
#define ATA_ID_SUP0_WCACHE 0x2000

#define ATA_ID_ENA2 13
#define ATA_ID_ENA2_VALID_MASK 0x0400
#define ATA_ID_ENA2_FUA 0x0800

#define FLAG_HAS_NCQ 0x01
#define FLAG_HAS_MEDIUM 0x02
#define FLAG_HAS_FLUSH 0x04
#define FLAG_HAS_WCACHE 0x08
#define FLAG_HAS_FUA 0x10

struct port_state {
    uint64_t lba_count;
    unsigned int queue_depth;
    unsigned int sector_size;
    unsigned int flags;
};

struct hba_state {
    bool has_ncq;
    unsigned int nr_cmds;
} hba_state;

static const char *ahci_portname(struct port_state *ps);

static int ata_id_check(struct port_state *ps, uint16_t *buf) {
    if ((buf[ATA_ID_GCAP] & (ATA_ID_GCAP_ATA_MASK | ATA_ID_GCAP_REMOVABLE | ATA_ID_GCAP_INCOMPLETE)) != ATA_ID_GCAP_ATA ||
        (buf[ATA_ID_CAP] & (ATA_ID_CAP_LBA | ATA_ID_CAP_DMA)) != (ATA_ID_CAP_LBA | ATA_ID_CAP_DMA) ||
        (buf[ATA_ID_SUP1] & (ATA_ID_SUP1_VALID_MASK | ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48)) != (ATA_ID_SUP1_VALID | ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48)) {
        
        dprintf(V_ERR, "%s: unsupported ATA device\n", ahci_portname(ps));
        dprintf(V_DEV, "%s: GCAP %04x CAP %04x SUP1 %04x\n", ahci_portname(ps), buf[ATA_ID_GCAP], buf[ATA_ID_CAP], buf[ATA_ID_SUP1]);
        return false;
    }

    ps->lba_count = ((uint64_t)buf[ATA_ID_LBA3] << 48) | ((uint64_t)buf[ATA_ID_LBA2] << 32) | ((uint64_t)buf[ATA_ID_LBA1] << 16) | (uint64_t)buf[ATA_ID_LBA0];

    if (hba_state.has_ncq && (buf[ATA_ID_SATA_CAP] & ATA_ID_SATA_CAP_NCQ)) {
        ps->flags |= FLAG_HAS_NCQ;
        ps->queue_depth = ((buf[ATA_ID_QDEPTH] & ATA_ID_QDEPTH_MASK) + 1 < hba_state.nr_cmds) ? ((buf[ATA_ID_QDEPTH] & ATA_ID_QDEPTH_MASK) + 1) : hba_state.nr_cmds;
    }

    ps->sector_size = ((buf[ATA_ID_PLSS] & (ATA_ID_PLSS_VALID_MASK | ATA_ID_PLSS_LLS)) == (ATA_ID_PLSS_VALID | ATA_ID_PLSS_LLS)) ?
                      (((buf[ATA_ID_LSS1] << 16) | buf[ATA_ID_LSS0]) << 1) : ATA_SECTOR_SIZE;

    if (ps->sector_size < ATA_SECTOR_SIZE) {
        dprintf(V_ERR, "%s: invalid sector size %u\n", ahci_portname(ps), ps->sector_size);
        return false;
    }

    ps->flags |= FLAG_HAS_MEDIUM | FLAG_HAS_FLUSH;

    if (buf[ATA_ID_SUP0] & ATA_ID_SUP0_WCACHE) {
        ps->flags |= FLAG_HAS_WCACHE;
    }

    if ((buf[ATA_ID_ENA2] & (ATA_ID_ENA2_VALID_MASK | ATA_ID_ENA2_FUA)) == (ATA_ID_ENA2_VALID | ATA_ID_ENA2_FUA)) {
        ps->flags |= FLAG_HAS_FUA;
    }

    return true;
}

/*===========================================================================*
 *				ata_transfer				     *
 *===========================================================================*/
#include <assert.h>
#include <string.h>

static int ata_transfer(struct port_state *ps, int cmd, u64_t start_lba,
                        unsigned int count, int write, int force, prd_t *prdt, int nr_prds) {
    cmd_fis_t fis;
    unsigned long lba_mask = 0x00FFFFFFUL;

    assert(ps != NULL);
    assert(count <= ATA_MAX_SECTORS);
    
    if (count == ATA_MAX_SECTORS) {
        count = 0;
    }

    memset(&fis, 0, sizeof(fis));
    fis.cf_dev = ATA_DEV_LBA;

    if (ps->flags & FLAG_HAS_NCQ) {
        fis.cf_cmd = write ? ATA_CMD_WRITE_FPDMA_QUEUED : ATA_CMD_READ_FPDMA_QUEUED;
        if (write && force && (ps->flags & FLAG_HAS_FUA)) {
            fis.cf_dev |= ATA_DEV_FUA;
        }
    } else {
        if (write) {
            fis.cf_cmd = force && (ps->flags & FLAG_HAS_FUA) ? ATA_CMD_WRITE_DMA_FUA_EXT : ATA_CMD_WRITE_DMA_EXT;
        } else {
            fis.cf_cmd = ATA_CMD_READ_DMA_EXT;
        }
    }

    fis.cf_lba = start_lba & lba_mask;
    fis.cf_lba_exp = (start_lba >> 24) & lba_mask;
    fis.cf_sec = count & 0xFF;
    fis.cf_sec_exp = (count >> 8) & 0xFF;

    if (port_set_cmd(ps, cmd, &fis, NULL, prdt, nr_prds, write) != 0) {
        return -1;
    }

    return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				gen_identify				     *
 *===========================================================================*/
#include <string.h>
#include <errno.h>

#define ATA_CMD_IDENTIFY 0xEC
#define ATA_CMD_IDENTIFY_PACKET 0xA1
#define ATA_ID_SIZE 512
#define FLAG_ATAPI 0x1
#define OK 0
#define FALSE 0
#define ahci_command_timeout 1000

typedef struct {
    int cf_cmd;
} cmd_fis_t;

typedef struct {
    void *vp_addr;
    int vp_size;
} prd_t;

struct port_state {
    int flags;
    void *tmp_phys;
};

void port_set_cmd(struct port_state *ps, int command_slot, cmd_fis_t *fis, void *packet, prd_t *prd, int prd_count, int write);
int port_exec(struct port_state *ps, int command_slot, int timeout);
int port_issue(struct port_state *ps, int command_slot, int timeout);

static int gen_identify(struct port_state *ps, int blocking) {
    if (!ps || !ps->tmp_phys) return -EINVAL;

    cmd_fis_t fis = {0};
    prd_t prd;

    fis.cf_cmd = (ps->flags & FLAG_ATAPI) ? ATA_CMD_IDENTIFY_PACKET : ATA_CMD_IDENTIFY;

    prd.vp_addr = ps->tmp_phys;
    prd.vp_size = ATA_ID_SIZE;

    port_set_cmd(ps, 0, &fis, NULL, &prd, 1, FALSE);

    if (blocking) {
        return port_exec(ps, 0, ahci_command_timeout);
    }

    return port_issue(ps, 0, ahci_command_timeout);
}

/*===========================================================================*
 *				gen_flush_wcache			     *
 *===========================================================================*/
static int gen_flush_wcache(struct port_state *ps) {
    if (!(ps->flags & FLAG_HAS_FLUSH)) {
        return EINVAL;
    }

    cmd_fis_t fis = {0};
    fis.cf_cmd = ATA_CMD_FLUSH_CACHE;

    port_set_cmd(ps, 0, &fis, NULL, NULL, 0, FALSE);

    return port_exec(ps, 0, ahci_flush_timeout);
}

/*===========================================================================*
 *				gen_get_wcache				     *
 *===========================================================================*/
int gen_get_wcache(struct port_state *ps, int *val) {
    if (val == NULL || ps == NULL) {
        return EINVAL;
    }
   
    if (!(ps->flags & FLAG_HAS_WCACHE)) {
        return EINVAL;
    }

    int result = gen_identify(ps, TRUE);
    if (result != OK) {
        return result;
    }

    *val = (((u16_t *)ps->tmp_base)[ATA_ID_ENA0] & ATA_ID_ENA0_WCACHE) ? 1 : 0;

    return OK;
}

/*===========================================================================*
 *				gen_set_wcache				     *
 *===========================================================================*/
#include <string.h>
#include <errno.h>
#include <time.h>

#define ATA_CMD_SET_FEATURES 0xEF
#define ATA_SF_EN_WCACHE 0x02
#define ATA_SF_DI_WCACHE 0x82
#define FALSE 0

struct port_state {
	int flags;
};

typedef struct {
	unsigned char cf_cmd;
	unsigned char cf_feat;
} cmd_fis_t;

extern clock_t ahci_command_timeout;
extern clock_t ahci_flush_timeout;
extern void port_set_cmd(struct port_state *ps, int a, cmd_fis_t *fis, void *packet, void *prdt, int b, int write);
extern int port_exec(struct port_state *ps, int a, clock_t timeout);

#define FLAG_HAS_WCACHE 0x01

static int gen_set_wcache(struct port_state *ps, int enable)
{
	cmd_fis_t fis;
	clock_t timeout;

	if ((ps->flags & FLAG_HAS_WCACHE) == 0) {
		return EINVAL;
	}

	timeout = enable ? ahci_command_timeout : ahci_flush_timeout;

	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_SET_FEATURES;
	fis.cf_feat = enable ? ATA_SF_EN_WCACHE : ATA_SF_DI_WCACHE;

	port_set_cmd(ps, 0, &fis, NULL, NULL, 0, FALSE);

	return port_exec(ps, 0, timeout);
}

/*===========================================================================*
 *				ct_set_fis				     *
 *===========================================================================*/
#include <string.h>
#include <stdint.h>

#define ATA_H2D_SIZE 20
#define ATA_FIS_TYPE 0
#define ATA_H2D_FLAGS 1
#define ATA_H2D_CMD 2
#define ATA_H2D_LBA_LOW 3
#define ATA_H2D_LBA_MID 4
#define ATA_H2D_LBA_HIGH 5
#define ATA_H2D_DEV 6
#define ATA_H2D_LBA_LOW_EXP 7
#define ATA_H2D_LBA_MID_EXP 8
#define ATA_H2D_LBA_HIGH_EXP 9
#define ATA_H2D_CTL 10
#define ATA_H2D_FEAT 11
#define ATA_H2D_FEAT_EXP 12
#define ATA_H2D_SEC 13
#define ATA_H2D_SEC_EXP 14

#define ATA_FIS_TYPE_H2D 0x27
#define ATA_H2D_FLAGS_C (1 << 7)
#define ATA_SEC_TAG_SHIFT 3

static int ATA_IS_FPDMA_CMD(uint8_t cmd) {
    // Example implementation for the macro/function
    return (cmd == 0x60 || cmd == 0x61); // FPDMA_READ or FPDMA_WRITE
}

typedef uint8_t u8_t;
typedef uint64_t vir_bytes;

typedef struct {
    uint8_t cf_cmd;
    uint8_t cf_dev;
    uint8_t cf_ctl;
    uint8_t cf_feat;
    uint8_t cf_feat_exp;
    uint8_t cf_sec;
    uint8_t cf_sec_exp;
    uint32_t cf_lba;
    uint32_t cf_lba_exp;
} cmd_fis_t;

static vir_bytes ct_set_fis(u8_t *ct, const cmd_fis_t *fis, unsigned int tag) {
    memset(ct, 0, ATA_H2D_SIZE);

    ct[ATA_FIS_TYPE] = ATA_FIS_TYPE_H2D;
    ct[ATA_H2D_FLAGS] = ATA_H2D_FLAGS_C;
    ct[ATA_H2D_CMD] = fis->cf_cmd;
    ct[ATA_H2D_LBA_LOW] = fis->cf_lba & 0xFF;
    ct[ATA_H2D_LBA_MID] = (fis->cf_lba >> 8) & 0xFF;
    ct[ATA_H2D_LBA_HIGH] = (fis->cf_lba >> 16) & 0xFF;
    ct[ATA_H2D_DEV] = fis->cf_dev;
    ct[ATA_H2D_LBA_LOW_EXP] = fis->cf_lba_exp & 0xFF;
    ct[ATA_H2D_LBA_MID_EXP] = (fis->cf_lba_exp >> 8) & 0xFF;
    ct[ATA_H2D_LBA_HIGH_EXP] = (fis->cf_lba_exp >> 16) & 0xFF;
    ct[ATA_H2D_CTL] = fis->cf_ctl;

    if (ATA_IS_FPDMA_CMD(fis->cf_cmd)) {
        ct[ATA_H2D_FEAT] = fis->cf_sec;
        ct[ATA_H2D_FEAT_EXP] = fis->cf_sec_exp;
        ct[ATA_H2D_SEC] = tag << ATA_SEC_TAG_SHIFT;
        ct[ATA_H2D_SEC_EXP] = 0;
    } else {
        ct[ATA_H2D_FEAT] = fis->cf_feat;
        ct[ATA_H2D_FEAT_EXP] = fis->cf_feat_exp;
        ct[ATA_H2D_SEC] = fis->cf_sec;
        ct[ATA_H2D_SEC_EXP] = fis->cf_sec_exp;
    }

    return ATA_H2D_SIZE;
}

/*===========================================================================*
 *				ct_set_packet				     *
 *===========================================================================*/
#include <string.h>

static void ct_set_packet(u8_t *ct, const u8_t packet[ATAPI_PACKET_SIZE]) {
    if (ct == NULL || packet == NULL) {
        return;
    }

    memcpy(&ct[AHCI_CT_PACKET_OFF], packet, ATAPI_PACKET_SIZE);
}

/*===========================================================================*
 *				ct_set_prdt				     *
 *===========================================================================*/
#include <stdint.h>

#define AHCI_CT_PRDT_OFF 0 // Assuming offset defined elsewhere

typedef struct {
	uint32_t vp_addr;   // Physical address
	uint32_t vp_size;   // Size of data
} prd_t;

static void ct_set_prdt(uint8_t *ct, prd_t *prdt, int nr_prds) {
	if (!ct || !prdt || nr_prds <= 0) return;

	uint32_t *p = (uint32_t *)(ct + AHCI_CT_PRDT_OFF);

	for (int i = 0; i < nr_prds; i++, prdt++) {
		uint32_t size_minus_one = (prdt->vp_size) ? prdt->vp_size - 1 : 0;
		*p++ = prdt->vp_addr;
		*p++ = 0;
		*p++ = 0;
		*p++ = size_minus_one;
	}
}

/*===========================================================================*
 *				port_set_cmd				     *
 *===========================================================================*/
static void port_set_cmd(struct port_state *ps, int cmd, cmd_fis_t *fis, u8_t packet[ATAPI_PACKET_SIZE], prd_t *prdt, int nr_prds, int write) {
    u8_t *ct;
    u32_t *cl;
    vir_bytes size;
    
    bool is_ncq_cmd = ATA_IS_FPDMA_CMD(fis->cf_cmd);
    if (is_ncq_cmd) {
        ps->flags |= FLAG_NCQ_MODE;
    } else {
        assert(!ps->pend_mask);
        ps->flags &= ~FLAG_NCQ_MODE;
    }

    ct = ps->ct_base[cmd];
    
    if (!ct) return;
    assert(nr_prds <= NR_PRDS);

    size = ct_set_fis(ct, fis, cmd);
    
    if (packet) {
        ct_set_packet(ct, packet);
    }

    ct_set_prdt(ct, prdt, nr_prds);

    cl = &ps->cl_base[cmd * AHCI_CL_ENTRY_DWORDS];
    memset(cl, 0, AHCI_CL_ENTRY_SIZE);

    cl[0] = (nr_prds << AHCI_CL_PRDTL_SHIFT) |
            ((!is_ncq_cmd && (nr_prds > 0 || packet)) ? AHCI_CL_PREFETCHABLE : 0) |
            (write ? AHCI_CL_WRITE : 0) |
            (packet ? AHCI_CL_ATAPI : 0) |
            ((size / sizeof(u32_t)) << AHCI_CL_CFL_SHIFT);
    
    cl[2] = ps->ct_phys[cmd];
}

/*===========================================================================*
 *				port_finish_cmd				     *
 *===========================================================================*/
#include <assert.h>

static void port_finish_cmd(struct port_state *ps, int cmd, int result) {
    if (cmd >= ps->queue_depth || !(ps->pend_mask & (1 << cmd))) {
        return;
    }

    dprintf(V_REQ, "%s: command %d %s\n", ahci_portname(ps), cmd, 
            (result == RESULT_SUCCESS) ? "succeeded" : "failed");

    ps->cmd_info[cmd].result = result;
    ps->pend_mask &= ~(1 << cmd);

    if (ps->state != STATE_WAIT_ID && ps->cmd_info[cmd].tid != 0) {
        blockdriver_mt_wakeup(ps->cmd_info[cmd].tid);
    }
}

/*===========================================================================*
 *				port_fail_cmds				     *
 *===========================================================================*/
static void port_fail_cmds(struct port_state *ps)
{
	int position;
	while (ps->pend_mask != 0) {
		position = __builtin_ctz(ps->pend_mask);
		if (position >= ps->queue_depth) {
			break;
		}
		port_finish_cmd(ps, position, RESULT_FAILURE);
		ps->pend_mask &= ~(1 << position);
	}
}

/*===========================================================================*
 *				port_check_cmds				     *
 *===========================================================================*/
static void port_check_cmds(struct port_state *ps) {
    u32_t mask, done;
    
    if (ps == NULL) {
        return;
    }

    mask = (ps->flags & FLAG_NCQ_MODE) ? port_read(ps, AHCI_PORT_SACT) : port_read(ps, AHCI_PORT_CI);
    
    done = ps->pend_mask & ~mask;

    for (int i = 0; i < ps->queue_depth; i++) {
        if (done & (1U << i)) {
            port_finish_cmd(ps, i, RESULT_SUCCESS);
        }
    }
}

/*===========================================================================*
 *				port_find_cmd				     *
 *===========================================================================*/
#include <assert.h>

static int port_find_cmd(struct port_state *ps) {
    int free_slot = -1;
    unsigned int mask = ps->pend_mask;
    
    for (int i = 0; i < ps->queue_depth; i++) {
        if (!(mask & 1)) {
            free_slot = i;
            break;
        }
        mask >>= 1;
    }
    
    assert(free_slot != -1);
    return free_slot;
}

/*===========================================================================*
 *				port_get_padbuf				     *
 *===========================================================================*/
static int port_get_padbuf(struct port_state *ps, size_t size)
{
	if (ps->pad_base != NULL) {
		if (ps->pad_size >= size) {
			return OK;
		} else {
			free_contig(ps->pad_base, ps->pad_size);
		}
	}

	ps->pad_size = size;
	ps->pad_base = alloc_contig(ps->pad_size, 0, &ps->pad_phys);

	if (ps->pad_base == NULL) {
		dprintf(V_ERR, "%s: unable to allocate a padding buffer of size %lu\n",
			ahci_portname(ps), (unsigned long) size);
		return ENOMEM;
	}

	dprintf(V_INFO, "%s: allocated padding buffer of size %lu\n",
		ahci_portname(ps), (unsigned long) size);

	return OK;
}

/*===========================================================================*
 *				sum_iovec				     *
 *===========================================================================*/
static int sum_iovec(struct port_state *ps, endpoint_t endpt, iovec_s_t *iovec, int nr_req, vir_bytes *total) {
    if (!ps || !iovec || !total || nr_req < 0) {
        return EINVAL;
    }

    vir_bytes bytes = 0;

    for (int i = 0; i < nr_req; i++) {
        vir_bytes size = iovec[i].iov_size;

        if (size == 0 || (size & 1) || size > LONG_MAX || bytes > LONG_MAX - size) {
            dprintf(V_ERR, ("%s: bad size %lu or overflow in iovec from %d\n", ahci_portname(ps), size, endpt));
            return EINVAL;
        }

        bytes += size;
    }

    *total = bytes;
    return OK;
}

/*===========================================================================*
 *				setup_prdt				     *
 *===========================================================================*/
static int setup_prdt(struct port_state *ps, endpoint_t endpt,
    iovec_s_t *iovec, int nr_req, vir_bytes size, vir_bytes lead,
    int write, prd_t *prdt)
{
    struct vumap_vir vvec[NR_PRDS];
    size_t bytes, trail;
    int i, r, pcount, nr_prds = 0;

    if (lead > 0) {
        if ((r = port_get_padbuf(ps, ps->sector_size)) != OK) {
            return r;
        }
        prdt[nr_prds++] = (prd_t){ .vp_addr = ps->pad_phys, .vp_size = lead };
    }

    trail = (ps->sector_size - (lead + size) % ps->sector_size) % ps->sector_size;

    for (i = 0; i < nr_req && size > 0; i++) {
        bytes = MIN(iovec[i].iov_size, size);
        vvec[i] = (struct vumap_vir){ 
            .vv_addr = (endpt == SELF ? (vir_bytes)iovec[i].iov_grant : 0), 
            .vv_grant = (endpt != SELF ? iovec[i].iov_grant : 0), 
            .vv_size = bytes 
        };
        size -= bytes;
    }
    
    pcount = i;

    if ((r = sys_vumap(endpt, vvec, pcount, 0, write ? VUA_READ : VUA_WRITE,
         &prdt[nr_prds], &pcount)) != OK) {
        dprintf(V_ERR, ("%s: unable to map memory from %d (%d)\n",
            ahci_portname(ps), endpt, r));
        return r;
    }

    if (pcount <= 0 || pcount > i) {
        return EINVAL;
    }

    for (i = 0; i < pcount; i++) {
        if (vvec[i].vv_size != prdt[nr_prds].vp_size || prdt[nr_prds].vp_addr & 1) {
            return EINVAL;
        }
        nr_prds++;
    }

    if (trail > 0 && nr_prds < NR_PRDS) {
        prdt[nr_prds++] = (prd_t){ .vp_addr = ps->pad_phys + lead, .vp_size = trail };
    }

    return nr_prds;
}

/*===========================================================================*
 *				port_transfer				     *
 *===========================================================================*/
static ssize_t port_transfer(struct port_state *ps, u64_t pos, u64_t eof, 
    endpoint_t endpt, iovec_s_t *iovec, int nr_req, int write, int flags) {
    prd_t prdt[NR_PRDS];
    vir_bytes size, lead;
    unsigned int count, nr_prds;
    u64_t start_lba;
    int r, cmd;

    // Get the total request size from the I/O vector.
    r = sum_iovec(ps, endpt, iovec, nr_req, &size);
    if (r != OK) return r;

    dprintf(V_REQ, ("%s: %s for %lu bytes at pos %llx\n",
        ahci_portname(ps), write ? "write" : "read", size, pos));

    if (ps->state != STATE_GOOD_DEV || !(ps->flags & FLAG_HAS_MEDIUM) || ps->sector_size <= 0) {
        dprintf(V_ERR, ("Invalid port or sector state.\n"));
        return EINVAL;
    }

    // Limit transfer size
    size = size > MAX_TRANSFER ? MAX_TRANSFER : size;

    // Adjust request size to not exceed partition end
    if (pos + size > eof) size = (vir_bytes)(eof - pos);

    start_lba = pos / ps->sector_size;
    lead = (vir_bytes)(pos % ps->sector_size);
    count = (lead + size + ps->sector_size - 1) / ps->sector_size;

    // Alignment checks
    if ((lead & 1) || (write && lead != 0)) {
        dprintf(V_ERR, ("%s: unaligned position from %d\n", ahci_portname(ps), endpt));
        return EINVAL;
    }
    
    if (write && (size % ps->sector_size) != 0) {
        dprintf(V_ERR, ("%s: unaligned size %lu from %d\n", ahci_portname(ps), size, endpt));
        return EINVAL;
    }

    // Setup physical addresses vector
    nr_prds = setup_prdt(ps, endpt, iovec, nr_req, size, lead, write, prdt);
    if (nr_prds < 0) return nr_prds;

    // Perform the transfer
    cmd = port_find_cmd(ps);
    if (ps->flags & FLAG_ATAPI)
        r = atapi_transfer(ps, cmd, start_lba, count, write, prdt, nr_prds);
    else
        r = ata_transfer(ps, cmd, start_lba, count, write, !!(flags & BDEV_FORCEWRITE), prdt, nr_prds);

    if (r != OK) return r;

    return size;
}

/*===========================================================================*
 *				port_hardreset				     *
 *===========================================================================*/
#include <errno.h>

static int port_hardreset(struct port_state *ps) {
    if (!ps) {
        return EINVAL;
    }

    int ret;
    ret = port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_INIT);
    if (ret) {
        return ret;
    }

    micro_delay(COMRESET_DELAY * 1000);

    ret = port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_NONE);
    return ret;
}

/*===========================================================================*
 *				port_override				     *
 *===========================================================================*/
void port_override(struct port_state *ps) {
    uint32_t cmd = port_read(ps, AHCI_PORT_CMD);
    port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_CLO);

    while (port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CLO) {
        // Busy-wait loop
    }

    dprintf(V_INFO, "%s: overridden\n", ahci_portname(ps));
}

/*===========================================================================*
 *				port_start				     *
 *===========================================================================*/
static int port_start(struct port_state *ps) {
    if (ps == NULL) return -1;

    u32_t cmd;
    
    if (port_write(ps, AHCI_PORT_SERR, ~0) != 0 || port_write(ps, AHCI_PORT_IS, ~0) != 0) {
        return -1;
    }
    
    cmd = port_read(ps, AHCI_PORT_CMD);
    port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_ST);
    
    dprintf(V_INFO, ("%s: started\n", ahci_portname(ps)));
    return 0;
}

/*===========================================================================*
 *				port_stop				     *
 *===========================================================================*/
#include <stdbool.h>

static bool stop_port_command(struct port_state *ps) {
    uint32_t cmd = port_read(ps, AHCI_PORT_CMD);
    if (!(cmd & (AHCI_PORT_CMD_CR | AHCI_PORT_CMD_ST))) {
        return false;
    }
    port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_ST);
    return true;
}

static void wait_for_port_ready(struct port_state *ps) {
    SPIN_UNTIL(!(port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CR), PORTREG_DELAY);
}

static void log_port_stopped(const struct port_state *ps) {
    dprintf(V_INFO, ("%s: stopped\n", ahci_portname(ps)));
}

static void port_stop(struct port_state *ps) {
    if (stop_port_command(ps)) {
        wait_for_port_ready(ps);
        log_port_stopped(ps);
    }
}

/*===========================================================================*
 *				port_restart				     *
 *===========================================================================*/
void port_restart(struct port_state *ps) {
    port_fail_cmds(ps);
    port_stop(ps);

    if (port_read(ps, AHCI_PORT_TFD) & (AHCI_PORT_TFD_STS_BSY | AHCI_PORT_TFD_STS_DRQ)) {
        dprintf(V_ERR, "%s: port reset\n", ahci_portname(ps));
        port_disconnect(ps);
        port_hardreset(ps);
    } else {
        port_start(ps);
    }
}

/*===========================================================================*
 *				print_string				     *
 *===========================================================================*/
#include <stdio.h>
#include <errno.h>

static void print_string(u16_t *buf, int start, int end) {
    if (buf == NULL || start > end) {
        fprintf(stderr, "Invalid input: %s\n", strerror(EINVAL));
        return;
    }

    while (end >= start && buf[end] == 0x2020) {
        end--;
    }

    int print_last = 0;
    if (end >= start && (buf[end] & 0xFF) == 0x20) {
        end--;
        print_last = 1;
    }

    for (int i = start; i <= end; i++) {
        printf("%c%c", buf[i] >> 8, buf[i] & 0xFF);
    }

    if (print_last) {
        printf("%c", buf[end + 1] >> 8);
    }
}

/*===========================================================================*
 *				port_id_check				     *
 *===========================================================================*/
static void port_id_check(struct port_state *ps, int success) {
    u16_t *buf;

    assert(ps->state == STATE_WAIT_ID);

    ps->flags &= ~FLAG_BUSY;
    cancel_timer(&ps->cmd_info[0].timer);

    if (!success) {
        if (!(ps->flags & FLAG_ATAPI) && port_read(ps, AHCI_PORT_SIG) != ATA_SIG_ATA) {
            dprintf(V_INFO, ("%s: may not be ATA, trying ATAPI\n", ahci_portname(ps)));
            ps->flags |= FLAG_ATAPI;
            gen_identify(ps, FALSE);
            return;
        }
        dprintf(V_ERR, ("%s: unable to identify\n", ahci_portname(ps)));
    }

    if (success) {
        buf = (u16_t *) ps->tmp_base;
        success = (ps->flags & FLAG_ATAPI) ? atapi_id_check(ps, buf) : ata_id_check(ps, buf);
    }

    if (!success) {
        port_stop(ps);
        ps->state = STATE_BAD_DEV;
        port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PRCE);
        return;
    }

    ps->state = STATE_GOOD_DEV;

    if (ahci_verbose >= V_INFO) {
        printf("%s: ATA%s, ", ahci_portname(ps), (ps->flags & FLAG_ATAPI) ? "PI" : "");
        print_string(buf, 27, 46);
        if (ahci_verbose >= V_DEV) {
            printf(" (");
            print_string(buf, 10, 19);
            printf(", ");
            print_string(buf, 23, 26);
            printf(")");
        }
        if (ps->flags & FLAG_HAS_MEDIUM) {
            printf(", %u byte sectors, %llu MB size", ps->sector_size,
                   ps->lba_count * ps->sector_size / (1024*1024));
        }
        printf("\n");
    }
}

/*===========================================================================*
 *				port_connect				     *
 *===========================================================================*/
void port_connect(struct port_state *ps) {
    if (ps == NULL) return;

    u32_t status = port_read(ps, AHCI_PORT_SSTS) & AHCI_PORT_SSTS_DET_MASK;
    if (status != AHCI_PORT_SSTS_DET_PHY) {
        port_stop(ps);
        ps->state = STATE_NO_DEV;
        ps->flags &= ~FLAG_BUSY;
        return;
    }

    ps->flags &= (FLAG_BUSY | FLAG_BARRIER | FLAG_SUSPENDED);

    if ((port_read(ps, AHCI_PORT_SIG) & 0xFFFF) == ATA_SIG_ATAPI) {
        ps->flags |= FLAG_ATAPI;
    } 

    ps->state = STATE_WAIT_ID;
    port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_MASK);
    (void) gen_identify(ps, FALSE);
}

/*===========================================================================*
 *				port_disconnect				     *
 *===========================================================================*/
static void port_disconnect(struct port_state *ps)
{
	if (!ps) {
		return;
	}

	dprintf(V_INFO, "%s: device disconnected\n", ahci_portname(ps));

	ps->state = STATE_NO_DEV;
	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);
	ps->flags &= ~FLAG_BUSY;

	port_fail_cmds(ps);

	ps->flags |= FLAG_BARRIER;

	if (blockdriver_mt_set_workers(ps->device, 1) != 0) {
		// Handle error, possibly log it
	}
}

/*===========================================================================*
 *				port_dev_check				     *
 *===========================================================================*/
#include <assert.h>

static void port_dev_check(struct port_state *ps) {
    u32_t status, tfd;
    
    assert(ps->state == STATE_WAIT_DEV);
    
    status = port_read(ps, AHCI_PORT_SSTS) & AHCI_PORT_SSTS_DET_MASK;
    dprintf(V_DEV, ("%s: polled status %u\n", ahci_portname(ps), status));
    
    if (status == AHCI_PORT_SSTS_DET_PHY) {
        tfd = port_read(ps, AHCI_PORT_TFD);
        if (!(tfd & (AHCI_PORT_TFD_STS_BSY | AHCI_PORT_TFD_STS_DRQ))) {
            port_connect(ps);
            return;
        }
    }
    
    if (status == AHCI_PORT_SSTS_DET_PHY || status == AHCI_PORT_SSTS_DET_DET) {
        if (ps->left > 0) {
            ps->left--;
            set_timer(&ps->cmd_info[0].timer, ahci_device_delay, port_timeout, BUILD_ARG(ps - port_state, 0));
            return;
        }
    }
    
    dprintf(V_INFO, ("%s: device not ready\n", ahci_portname(ps)));
    
    if (status == AHCI_PORT_SSTS_DET_PHY && hba_state.has_clo) {
        port_override(ps);
        port_connect(ps);
        return;
    }
    
    ps->state = (status == AHCI_PORT_SSTS_DET_PHY) ? STATE_BAD_DEV : STATE_NO_DEV;
    
    if (status == AHCI_PORT_SSTS_DET_PHY) {
        port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PRCE);
    } else {
        ps->flags &= ~FLAG_BUSY;
    }
}

/*===========================================================================*
 *				port_intr				     *
 *===========================================================================*/
static void port_intr(struct port_state *ps) {
    u32_t smask, emask;

    if (ps->state == STATE_NO_PORT) {
        dprintf(V_ERR, "%s: interrupt for invalid port!\n", ahci_portname(ps));
        return;
    }

    smask = port_read(ps, AHCI_PORT_IS);
    emask = smask & port_read(ps, AHCI_PORT_IE);
    port_write(ps, AHCI_PORT_IS, smask);
    dprintf(V_REQ, "%s: interrupt (%08x)\n", ahci_portname(ps), smask);
    port_check_cmds(ps);

    if (emask & AHCI_PORT_IS_PCS) {
        port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_X);
        dprintf(V_DEV, "%s: device attached\n", ahci_portname(ps));

        if (ps->state == STATE_SPIN_UP) {
            cancel_timer(&ps->cmd_info[0].timer);
            ps->state = STATE_WAIT_DEV;
            ps->left = ahci_device_checks;
            port_dev_check(ps);
        } else if (ps->state == STATE_NO_DEV) {
            ps->state = STATE_WAIT_DEV;
            ps->left = ahci_device_checks;
            port_dev_check(ps);
        } else {
            assert(ps->state == STATE_WAIT_DEV);
        }
    } else if (emask & AHCI_PORT_IS_PRCS) {
        port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_N);
        dprintf(V_DEV, "%s: device detached\n", ahci_portname(ps));

        if (ps->state == STATE_WAIT_ID || ps->state == STATE_GOOD_DEV) {
            port_stop(ps);
            port_disconnect(ps);
            port_hardreset(ps);
        } else if (ps->state == STATE_BAD_DEV) {
            port_disconnect(ps);
            port_hardreset(ps);
        } else {
            assert(0);
        }
    } else if (smask & AHCI_PORT_IS_MASK) {
        int tfdStatus = port_read(ps, AHCI_PORT_TFD);
        int errorCondition = tfdStatus & (AHCI_PORT_TFD_STS_ERR | AHCI_PORT_TFD_STS_DF);

        if (errorCondition || (smask & AHCI_PORT_IS_RESTART)) {
            port_restart(ps);
        }

        if (ps->state == STATE_WAIT_ID) {
            port_id_check(ps, !errorCondition);
        }
    }
}

/*===========================================================================*
 *				port_timeout				     *
 *===========================================================================*/
static void port_timeout(int arg) {
    struct port_state *ps;
    int port = GET_PORT(arg);
    int cmd = GET_TAG(arg);

    if (port < 0 || port >= hba_state.nr_ports) {
        return; // Invalid port, exit early
    }

    ps = &port_state[port];

    if (ps->flags & FLAG_SUSPENDED) {
        if (cmd == 0) {
            blockdriver_mt_wakeup(ps->cmd_info[0].tid);
        }
    }

    switch (ps->state) {
        case STATE_SPIN_UP:
            if (port_read(ps, AHCI_PORT_IS) & AHCI_PORT_IS_PCS) {
                dprintf(V_INFO, ("%s: bad controller, no interrupt\n", ahci_portname(ps)));
                ps->state = STATE_WAIT_DEV;
                ps->left = ahci_device_checks;
                port_dev_check(ps);
            } else {
                dprintf(V_INFO, ("%s: spin-up timeout\n", ahci_portname(ps)));
                ps->state = STATE_NO_DEV;
                ps->flags &= ~FLAG_BUSY;
            }
            break;
        case STATE_WAIT_DEV:
            port_dev_check(ps);
            break;
        case STATE_WAIT_ID:
            port_id_check(ps, FALSE);
            // No break needed; state is finalized during port_restart
        default:
            dprintf(V_ERR, ("%s: timeout\n", ahci_portname(ps)));
            port_restart(ps);
            break;
    }
}

/*===========================================================================*
 *				port_wait				     *
 *===========================================================================*/
#include <stdbool.h>

static void port_wait(struct port_state *ps)
{
    if (ps == NULL) return; // Basic error handling for null pointer

    ps->flags |= FLAG_SUSPENDED;

    while (true)
    {
        if (!(ps->flags & FLAG_BUSY))
        {
            break;
        }
        blockdriver_mt_sleep();
    }

    ps->flags &= ~FLAG_SUSPENDED;
}

/*===========================================================================*
 *				port_issue				     *
 *===========================================================================*/
#include <stdint.h>
#include <time.h>

#define COMMAND_BIT(cmd) (1U << (cmd))

static void port_issue(struct port_state *ps, int cmd, clock_t timeout) {
    uint32_t command_mask = COMMAND_BIT(cmd);

    if (ps->flags & FLAG_HAS_NCQ) {
        port_write(ps, AHCI_PORT_SACT, command_mask);
    }

    __insn_barrier();

    port_write(ps, AHCI_PORT_CI, command_mask);

    ps->pend_mask |= command_mask;

    if (!set_timer(&ps->cmd_info[cmd].timer, timeout, port_timeout, BUILD_ARG(ps - port_state, cmd))) {
        // Handle timer setup failure, if necessary
    }
}

/*===========================================================================*
 *				port_exec				     *
 *===========================================================================*/
#include <assert.h>
#include <errno.h>
#include <stdio.h>

#define FLAG_BUSY 0x01
#define RESULT_FAILURE -1
#define OK 0
#define EIO 5

struct thread_info {
	int result;
	int tid;
	int timer;
};

struct port_state {
	struct thread_info *cmd_info;
	int flags;
};

void port_issue(struct port_state *ps, int cmd, clock_t timeout);
int blockdriver_mt_get_tid(void);
void blockdriver_mt_sleep(void);
void cancel_timer(int *timer);
const char *ahci_portname(struct port_state *ps);
void dprintf(int level, const char *format, ...);

#define V_REQ 0

static int validate_port_cmd(struct port_state *ps, int cmd) {
	if (!ps || cmd < 0) {
		return EINVAL;
	}
	return OK;
}

static int handle_result(int result) {
	return (result == RESULT_FAILURE) ? EIO : OK;
}

static int port_exec(struct port_state *ps, int cmd, clock_t timeout) {
	int validation;

	validation = validate_port_cmd(ps, cmd);
	if (validation != OK) {
		return validation;
	}

	port_issue(ps, cmd, timeout);

	ps->cmd_info[cmd].tid = blockdriver_mt_get_tid();

	blockdriver_mt_sleep();

	cancel_timer(&ps->cmd_info[cmd].timer);

	assert(!(ps->flags & FLAG_BUSY));

	dprintf(V_REQ, "%s: end of command -- %s\n", ahci_portname(ps),
		ps->cmd_info[cmd].result == RESULT_FAILURE ? 
		"failure" : "success");

	return handle_result(ps->cmd_info[cmd].result);
}

/*===========================================================================*
 *				port_alloc				     *
 *===========================================================================*/
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define NR_CMDS 32
#define AHCI_CL_SIZE 1024
#define AHCI_FIS_SIZE 256
#define AHCI_TMP_ALIGN 128
#define AHCI_TMP_SIZE 512
#define AHCI_CT_ALIGN 128
#define AHCI_CT_SIZE 1024
#define AC_ALIGN4K 4096
#define AHCI_PORT_FBU 0
#define AHCI_PORT_FB 4
#define AHCI_PORT_CLBU 8
#define AHCI_PORT_CLB 12
#define AHCI_PORT_CMD 16
#define AHCI_PORT_CMD_FRE 0x10

struct port_state {
    void *mem_base;
    size_t mem_size;
    uintptr_t mem_phys;
    uint32_t *cl_base;
    uintptr_t cl_phys;
    uint32_t *fis_base;
    uintptr_t fis_phys;
    uint8_t *tmp_base;
    uintptr_t tmp_phys;
    void *ct_base[NR_CMDS];
    uintptr_t ct_phys[NR_CMDS];
    void *pad_base;
    size_t pad_size;
};

void* alloc_contig(size_t size, int align, uintptr_t *phys_addr);
void panic(const char *msg);
uint32_t port_read(struct port_state *ps, int reg);
void port_write(struct port_state *ps, int reg, uint32_t val);

static void port_alloc(struct port_state *ps) {
    size_t fis_off, tmp_off, ct_off;
    size_t ct_offs[NR_CMDS];
    uint32_t cmd;

    fis_off = (AHCI_CL_SIZE + AHCI_FIS_SIZE - 1) & ~(AHCI_FIS_SIZE - 1);
    tmp_off = (fis_off + AHCI_FIS_SIZE + AHCI_TMP_ALIGN - 1) & ~(AHCI_TMP_ALIGN - 1);
    ct_off = tmp_off + AHCI_TMP_SIZE;

    for (int i = 0; i < NR_CMDS; i++) {
        ct_off = (ct_off + AHCI_CT_ALIGN - 1) & ~(AHCI_CT_ALIGN - 1);
        ct_offs[i] = ct_off;
        ct_off += AHCI_CT_SIZE;
    }
    ps->mem_size = ct_off;

    ps->mem_base = alloc_contig(ps->mem_size, AC_ALIGN4K, &ps->mem_phys);
    if (ps->mem_base == NULL) {
        panic("unable to allocate port memory");
    }
    memset(ps->mem_base, 0, ps->mem_size);

    ps->cl_base = ps->mem_base;
    ps->cl_phys = ps->mem_phys;
    assert(ps->cl_phys % AHCI_CL_SIZE == 0);

    ps->fis_base = (uint32_t *)(ps->mem_base + fis_off);
    ps->fis_phys = ps->mem_phys + fis_off;
    assert(ps->fis_phys % AHCI_FIS_SIZE == 0);

    ps->tmp_base = (uint8_t *)(ps->mem_base + tmp_off);
    ps->tmp_phys = ps->mem_phys + tmp_off;
    assert(ps->tmp_phys % AHCI_TMP_ALIGN == 0);

    for (int i = 0; i < NR_CMDS; i++) {
        ps->ct_base[i] = ps->mem_base + ct_offs[i];
        ps->ct_phys[i] = ps->mem_phys + ct_offs[i];
        assert(ps->ct_phys[i] % AHCI_CT_ALIGN == 0);
    }

    port_write(ps, AHCI_PORT_FBU, 0);
    port_write(ps, AHCI_PORT_FB, ps->fis_phys);

    port_write(ps, AHCI_PORT_CLBU, 0);
    port_write(ps, AHCI_PORT_CLB, ps->cl_phys);

    cmd = port_read(ps, AHCI_PORT_CMD);
    port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_FRE);

    ps->pad_base = NULL;
    ps->pad_size = 0;
}

/*===========================================================================*
 *				port_free				     *
 *===========================================================================*/
static void port_free(struct port_state *ps) {
    if (!ps) return;

    u32_t cmd = port_read(ps, AHCI_PORT_CMD);

    if (cmd & AHCI_PORT_CMD_FRE) {
        port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_FRE);
        
        if (!SPIN_UNTIL(!(port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_FR), PORTREG_DELAY)) {
            return; 
        }
    }

    if (ps->pad_base) {
        free_contig(ps->pad_base, ps->pad_size);
        ps->pad_base = NULL;
    }

    free_contig(ps->mem_base, ps->mem_size);
    ps->mem_base = NULL;
}

/*===========================================================================*
 *				port_init				     *
 *===========================================================================*/
static int initialize_port_state(struct port_state *ps) {
    if (!ps) return -1;

    ps->queue_depth = 1;
    ps->state = STATE_SPIN_UP;
    ps->flags = FLAG_BUSY;
    ps->sector_size = 0;
    ps->open_count = 0;
    ps->pend_mask = 0;
    for (int i = 0; i < NR_CMDS; i++) {
        init_timer(&ps->cmd_info[i].timer);
    }
    return 0;
}

static int configure_hba(struct port_state *ps) {
    if (!ps) return -1;

    u32_t cmd = port_read(ps, AHCI_PORT_CMD);
    port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_SUD);

    return 0;
}

static void port_init(struct port_state *ps) {
    if (!ps || port_alloc(ps) != 0) return;

    ps->reg = (u32_t *)((u8_t *)hba_state.base + AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * (ps - port_state));

    initialize_port_state(ps);
    port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);
    configure_hba(ps);
    port_hardreset(ps);

    set_timer(&ps->cmd_info[0].timer, ahci_spinup_timeout, port_timeout, BUILD_ARG(ps - port_state, 0));
}

/*===========================================================================*
 *				ahci_probe				     *
 *===========================================================================*/
#include <errno.h>

static int ahci_probe(int skip)
{
	int r, devind;
	u16_t vid, did;

	pci_init();

	while ((r = pci_first_dev(&devind, &vid, &did)) > 0) {
		if (skip-- <= 0) {
			if (pci_reserve(devind) < 0) {
				return -EIO;
			}
			return devind;
		}
		r = pci_next_dev(&devind, &vid, &did);
	}

	return -ENODEV;
}

/*===========================================================================*
 *				ahci_reset				     *
 *===========================================================================*/
#include <errno.h>

#define RESET_SUCCESS 0
#define RESET_FAILURE (-1)

static int ahci_reset(void)
{
    u32_t ghc = hba_read(AHCI_HBA_GHC);

    hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE);
    hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE | AHCI_HBA_GHC_HR);

    if (!SPIN_UNTIL(!(hba_read(AHCI_HBA_GHC) & AHCI_HBA_GHC_HR), RESET_DELAY)) {
        return RESET_FAILURE;
    }

    return RESET_SUCCESS;
}

/*===========================================================================*
 *				ahci_init				     *
 *===========================================================================*/
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <pci/pci.h>

#define OK 0
#define NR_PORTS 32
#define MAP_FAILED ((void *) -1)
#define SELF 0
#define NO_DEVICE 0
#define STATE_NO_PORT 0
#define AHCI_MEM_BASE_SIZE 1024
#define AHCI_MEM_PORT_SIZE 128
#define AHCI_HBA_GHC 0x04
#define AHCI_HBA_GHC_AE 0x80000000
#define AHCI_HBA_GHC_IE 0x2
#define AHCI_HBA_CAP 0x00
#define AHCI_HBA_CAP_SNCQ 0x1
#define AHCI_HBA_CAP_SCLO 0x2
#define AHCI_HBA_CAP_NCS_SHIFT 8
#define AHCI_HBA_CAP_NCS_MASK 0x1F
#define NR_CMDS 32
#define AHCI_HBA_CAP_NP_SHIFT 20
#define AHCI_HBA_CAP_NP_MASK 0x1F
#define V_INFO 2

typedef uint32_t u32_t;

struct hba_state_t {
    u32_t *base;
    u32_t size;
    int irq;
    int hook_id;
    int has_ncq;
    int has_clo;
    int nr_ports;
    int nr_cmds;
} hba_state;

struct port_state_t {
    int device;
    int state;
} port_state[NR_PORTS];

static int pci_get_bar(int devind, int bar_nr, u32_t *base, u32_t *size, int *ioflag);
static int panic(const char *fmt, ...);
static void *vm_map_phys(int process, void *addr, size_t len);
static int sys_irqsetpolicy(int irq, int policy, int *hook_id);
static int sys_irqenable(int *hook_id);
static void ahci_reset(void);
static u32_t hba_read(u32_t reg);
static void hba_write(u32_t reg, u32_t val);
static void dprintf(int level, const char *fmt, ...);
static void port_init(struct port_state_t *port);
static int ahci_instance;

static void ahci_init(int devind) {
    u32_t base, size, cap, ghc, mask;
    int r, port, ioflag;

    if ((r = pci_get_bar(devind, PCI_BAR_6, &base, &size, &ioflag)) != OK) {
        panic("unable to retrieve BAR: %d", r);
    }

    if (ioflag) {
        panic("invalid BAR type");
    }

    if (size < AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE) {
        panic("HBA memory size too small: %u", size);
    }

    size = size < (AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * NR_PORTS) ? size : (AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * NR_PORTS);
    hba_state.nr_ports = (size - AHCI_MEM_BASE_SIZE) / AHCI_MEM_PORT_SIZE;

    hba_state.base = (u32_t *)vm_map_phys(SELF, (void *)base, size);
    if (hba_state.base == MAP_FAILED) {
        panic("unable to map HBA memory");
    }

    hba_state.irq = pci_attr_r8(devind, PCI_ILR);

    if ((r = sys_irqsetpolicy(hba_state.irq, 0, &hba_state.hook_id)) != OK) {
        panic("unable to register IRQ: %d", r);
    }

    if ((r = sys_irqenable(&hba_state.hook_id)) != OK) {
        panic("unable to enable IRQ: %d", r);
    }

    ahci_reset();

    ghc = hba_read(AHCI_HBA_GHC);
    hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE | AHCI_HBA_GHC_IE);

    cap = hba_read(AHCI_HBA_CAP);
    hba_state.has_ncq = (cap & AHCI_HBA_CAP_SNCQ) ? 1 : 0;
    hba_state.has_clo = (cap & AHCI_HBA_CAP_SCLO) ? 1 : 0;
    hba_state.nr_cmds = (((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1) < NR_CMDS ? (((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1) : NR_CMDS;

    dprintf(V_INFO, "AHCI%u: HBA v%d.%d%d, %ld ports, %ld commands, %s queuing, IRQ %d\n",
        ahci_instance,
        (int)(hba_read(AHCI_HBA_VS) >> 16),
        (int)((hba_read(AHCI_HBA_VS) >> 8) & 0xFF),
        (int)(hba_read(AHCI_HBA_VS) & 0xFF),
        (cap >> AHCI_HBA_CAP_NP_SHIFT & AHCI_HBA_CAP_NP_MASK) + 1,
        (cap >> AHCI_HBA_CAP_NCS_SHIFT & AHCI_HBA_CAP_NCS_MASK) + 1,
        hba_state.has_ncq ? "supports" : "no", hba_state.irq);

    dprintf(V_INFO, "AHCI%u: CAP %08x, CAP2 %08x, PI %08x\n",
        ahci_instance, cap, hba_read(AHCI_HBA_CAP2),
        hba_read(AHCI_HBA_PI));

    mask = hba_read(AHCI_HBA_PI);

    for (port = 0; port < hba_state.nr_ports; port++) {
        port_state[port].device = NO_DEVICE;
        port_state[port].state = STATE_NO_PORT;

        if (mask & (1 << port)) {
            port_init(&port_state[port]);
        }
    }
}

/*===========================================================================*
 *				ahci_stop				     *
 *===========================================================================*/
static void ahci_stop(void) {
    struct port_state *ps;
    int r, port;

    for (port = 0; port < hba_state.nr_ports; port++) {
        ps = &port_state[port];

        if (ps->state == STATE_NO_PORT) {
            continue;
        }

        port_stop(ps);
        port_free(ps);
    }

    ahci_reset();

    r = vm_unmap_phys(SELF, (void *) hba_state.base, hba_state.size);
    if (r != OK) {
        log_error("Unable to unmap HBA memory: %d", r);
        safely_terminate("Unrecoverable error during AHCI stop.");
    }

    r = sys_irqrmpolicy(&hba_state.hook_id);
    if (r != OK) {
        log_error("Unable to deregister IRQ: %d", r);
        safely_terminate("Unrecoverable error during AHCI stop.");
    }
}

void safely_terminate(const char *message) {
    // Safely terminate or reset resources here
    panic(message);
}

void log_error(const char *format, int error_code) {
    // Log the error; could be to a file, stderr, etc.
    printf(format, error_code);
}

/*===========================================================================*
 *				ahci_alarm				     *
 *===========================================================================*/
#include <stdbool.h>

static void ahci_alarm(clock_t stamp) {
    expire_timers(stamp);
}

/*===========================================================================*
 *				ahci_intr				     *
 *===========================================================================*/
#include <errno.h>

static void ahci_intr(unsigned int UNUSED(mask)) {
    struct port_state *ps;
    u32_t mask;
    int port, r;

    mask = hba_read(AHCI_HBA_IS);

    for (port = 0; port < hba_state.nr_ports; port++) {
        if ((mask & (1U << port)) != 0) {
            ps = &port_state[port];
            port_intr(ps);

            if ((ps->flags & (FLAG_SUSPENDED | FLAG_BUSY)) == FLAG_SUSPENDED) {
                blockdriver_mt_wakeup(ps->cmd_info[0].tid);
            }
        }
    }

    hba_write(AHCI_HBA_IS, mask);

    r = sys_irqenable(&hba_state.hook_id);
    if (r != OK) {
        panic("unable to enable IRQ", r);
    }
}

/*===========================================================================*
 *				ahci_get_params				     *
 *===========================================================================*/
#include <limits.h>

static void ahci_get_params(void) {
    long v;
    unsigned int i;

    if (env_parse("instance", "d", 0, &v, 0, 255) == -1) {
        ahci_instance = 0;
    } else {
        ahci_instance = (int) v;
    }

    v = V_ERR;
    if (env_parse("ahci_verbose", "d", 0, &v, V_NONE, V_REQ) == -1) {
        ahci_verbose = V_ERR;
    } else {
        ahci_verbose = (int) v;
    }

    for (i = 0; i < sizeof(ahci_timevar) / sizeof(ahci_timevar[0]); i++) {
        v = ahci_timevar[i].default_ms;
        if (env_parse(ahci_timevar[i].name, "d", 0, &v, 1, LONG_MAX) != -1) {
            *ahci_timevar[i].ptr = millis_to_hz(v);
        } else {
            *ahci_timevar[i].ptr = millis_to_hz(ahci_timevar[i].default_ms);
        }
    }

    ahci_device_delay = millis_to_hz(DEVICE_DELAY);
    ahci_device_checks = (ahci_device_timeout + ahci_device_delay - 1) / ahci_device_delay;
}

/*===========================================================================*
 *				ahci_set_mapping			     *
 *===========================================================================*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define STATE_NO_PORT 0
#define NO_PORT -1
#define NR_PORTS 32
#define MAX_DRIVES 16
#define OK 0

typedef struct {
    int state;
    int device;
} PortState;

PortState port_state[NR_PORTS];
int ahci_map[MAX_DRIVES];
int ahci_instance;

int env_get_param(const char* key, char* val, size_t size);

static void ahci_set_mapping(void) {
    char key[16], val[32];
    unsigned int port;
    int i, j;

    snprintf(key, sizeof(key), "ahci%d_map", ahci_instance);

    for (i = j = 0; i < NR_PORTS && j < MAX_DRIVES; i++) {
        if (port_state[i].state != STATE_NO_PORT) {
            ahci_map[j++] = i;
        }
    }

    for (; j < MAX_DRIVES; j++) {
        ahci_map[j] = NO_PORT;
    }

    if (env_get_param(key, val, sizeof(val)) == OK) {
        char* p = val;

        for (i = 0; i < MAX_DRIVES && *p; i++) {
            port = (unsigned int) strtoul(p, &p, 0);
            if (*p == ',') p++;
            ahci_map[i] = port < NR_PORTS ? port : NO_PORT;
        }

        for (; i < MAX_DRIVES; i++) {
            ahci_map[i] = NO_PORT;
        }
    }

    for (i = 0; i < MAX_DRIVES; i++) {
        if ((j = ahci_map[i]) != NO_PORT) {
            port_state[j].device = i;
        }
    }
}

/*===========================================================================*
 *				sef_cb_init_fresh			     *
 *===========================================================================*/
#include <errno.h>

static int sef_cb_init_fresh(int type, sef_init_info_t *UNUSED(info)) {
    int devind, result;

    result = ahci_get_params();
    if (result != OK) {
        return result;
    }

    devind = ahci_probe(ahci_instance);
    if (devind < 0) {
        return ENODEV; // Return a more descriptive error code
    }

    result = ahci_init(devind);
    if (result != OK) {
        return result; 
    }

    result = ahci_set_mapping();
    if (result != OK) {
        return result;
    }

    blockdriver_announce(type);

    return OK;
}

/*===========================================================================*
 *				sef_cb_signal_handler			     *
 *===========================================================================*/
#include <signal.h>
#include <stdlib.h>

static volatile sig_atomic_t ahci_exiting = 0;

static void sef_cb_signal_handler(int signo) {
    if (signo == SIGTERM) {
        ahci_exiting = 1;
        for (int port = 0; port < hba_state.nr_ports; port++) {
            if (port_state[port].open_count > 0) {
                return;
            }
        }
        ahci_stop();
        exit(EXIT_SUCCESS);
    }
}

/*===========================================================================*
 *				sef_local_startup			     *
 *===========================================================================*/
void sef_local_startup(void)
{
	sef_setcb_init_fresh(sef_cb_init_fresh);
	sef_setcb_signal_handler(sef_cb_signal_handler);
	blockdriver_mt_support_lu();
	sef_startup();
}

/*===========================================================================*
 *				ahci_portname				     *
 *===========================================================================*/
#include <stdio.h>
#include <limits.h>

#define MAX_DEVICE_NUMBER 9
#define NO_DEVICE -1

static char *ahci_portname(struct port_state *ps)
{
    static char name[11];
    int port_number;

    if (!ps) {
        snprintf(name, sizeof(name), "Error");
        return name;
    }

    snprintf(name, sizeof(name), "AHCI%d-", ahci_instance);

    if (ps->device == NO_DEVICE) {
        port_number = ps - port_state;
        snprintf(name + 6, sizeof(name) - 6, "P%02d", port_number);
    } else if (ps->device >= 0 && ps->device <= MAX_DEVICE_NUMBER) {
        snprintf(name + 6, sizeof(name) - 6, "D%d", ps->device);
    } else {
        snprintf(name, sizeof(name), "Invalid");
    }

    return name;
}

/*===========================================================================*
 *				ahci_map_minor				     *
 *===========================================================================*/
#include <stddef.h>

static struct port_state *ahci_map_minor(devminor_t minor, struct device **dvp) {
    struct port_state *ps = NULL;
    int port;

    if (minor >= 0 && minor < NR_MINORS) {
        port = ahci_map[minor / DEV_PER_DRIVE];
        if (port != NO_PORT) {
            ps = &port_state[port];
            *dvp = &ps->part[minor % DEV_PER_DRIVE];
        }
    } else {
        unsigned adjusted_minor = (unsigned)(minor - MINOR_d0p0s0);
        if (adjusted_minor < NR_SUBDEVS) {
            port = ahci_map[adjusted_minor / SUB_PER_DRIVE];
            if (port != NO_PORT) {
                ps = &port_state[port];
                *dvp = &ps->subpart[adjusted_minor % SUB_PER_DRIVE];
            }
        }
    }

    return ps;
}

/*===========================================================================*
 *				ahci_part				     *
 *===========================================================================*/
static struct device *ahci_part(devminor_t minor)
{
	struct device *dv = NULL;

	if (ahci_map_minor(minor, &dv) != NULL) {
		return dv;
	}

	return NULL;
}

/*===========================================================================*
 *				ahci_open				     *
 *===========================================================================*/
#include <errno.h>
#include <string.h>

static int ahci_open(devminor_t minor, int access)
{
	struct port_state *ps = ahci_get_port(minor);
	int r;

	if (!ps)
		return ENODEV;

	ps->cmd_info[0].tid = blockdriver_mt_get_tid();

	while (ps->flags & FLAG_BUSY)
		port_wait(ps);

	if (ps->state != STATE_GOOD_DEV)
		return ENXIO;

	if ((ps->flags & FLAG_READONLY) && (access & BDEV_W_BIT))
		return EACCES;

	if (!ps->open_count) {
		ps->flags &= ~FLAG_BARRIER;

		if ((ps->flags & FLAG_ATAPI) && (r = atapi_check_medium(ps, 0)) != OK)
			return r;

		memset(ps->part, 0, sizeof(ps->part));
		memset(ps->subpart, 0, sizeof(ps->subpart));

		ps->part[0].dv_size = ps->lba_count * ps->sector_size;

		partition(&ahci_dtab, ps->device * DEV_PER_DRIVE, P_PRIMARY,
			ps->flags & FLAG_ATAPI);

		blockdriver_mt_set_workers(ps->device, ps->queue_depth);
	} else if (ps->flags & FLAG_BARRIER) {
		return ENXIO;
	}

	ps->open_count++;

	return OK;
}

/*===========================================================================*
 *				ahci_close				     *
 *===========================================================================*/
static int ahci_close(devminor_t minor) {
	struct port_state *ps = ahci_get_port(minor);

	if (ps->open_count <= 0) {
		dprintf(V_ERR, ("%s: closing already-closed port\n", ahci_portname(ps)));
		return EINVAL;
	}

	ps->open_count--;

	if (ps->open_count > 0) {
		return OK;
	}

	blockdriver_mt_set_workers(ps->device, 1);

	if (ps->state == STATE_GOOD_DEV && !(ps->flags & FLAG_BARRIER)) {
		dprintf(V_INFO, ("%s: flushing write cache\n", ahci_portname(ps)));
		(void) gen_flush_wcache(ps);
	}

	if (ahci_exiting) {
		int port;
		for (port = 0; port < hba_state.nr_ports; port++) {
			if (port_state[port].open_count > 0) {
				return OK;
			}
		}
		ahci_stop();
		blockdriver_mt_terminate();
	}

	return OK;
}

/*===========================================================================*
 *				ahci_transfer				     *
 *===========================================================================*/
#include <errno.h>

static ssize_t ahci_transfer(devminor_t minor, int do_write, u64_t position,
	endpoint_t endpt, iovec_t *iovec, unsigned int count, int flags)
{
	struct port_state *ps = ahci_get_port(minor);
	struct device *dv = ahci_part(minor);
	u64_t pos, eof;

	if (!ps || ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER))
		return -EIO;

	if (count > NR_IOREQS)
		return -EINVAL;

	if (position >= dv->dv_size)
		return 0;

	pos = dv->dv_base + position;
	eof = dv->dv_base + dv->dv_size;

	return port_transfer(ps, pos, eof, endpt, (iovec_s_t *) iovec, count, do_write, flags);
}

/*===========================================================================*
 *				ahci_ioctl				     *
 *===========================================================================*/
static int ahci_ioctl(devminor_t minor, unsigned long request,
                      endpoint_t endpt, cp_grant_id_t grant, endpoint_t UNUSED(user_endpt)) {
    struct port_state *ps = ahci_get_port(minor);
    if (!ps) return EIO; // Check to ensure port state is not null
    
    int r, val;

    if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER)) return EIO;

    switch (request) {
        case DIOCEJECT:
            if (!(ps->flags & FLAG_ATAPI)) return EINVAL;
            return atapi_load_eject(ps, 0, FALSE);

        case DIOCOPENCT:
            return sys_safecopyto(endpt, grant, 0,
                                  (vir_bytes) &ps->open_count, sizeof(ps->open_count));

        case DIOCFLUSH:
            return gen_flush_wcache(ps);

        case DIOCSETWC:
            r = sys_safecopyfrom(endpt, grant, 0, (vir_bytes) &val, sizeof(val));
            if (r != OK) return r;
            return gen_set_wcache(ps, val);

        case DIOCGETWC:
            r = gen_get_wcache(ps, &val);
            if (r != OK) return r;
            return sys_safecopyto(endpt, grant, 0, (vir_bytes) &val, sizeof(val));

        default:
            return ENOTTY;
    }
}

/*===========================================================================*
 *				ahci_device				     *
 *===========================================================================*/
#include <errno.h>

static int ahci_device(devminor_t minor, device_id_t *id) {
    struct port_state *ps;
    struct device *dv;

    if (!id) 
        return EINVAL;

    ps = ahci_map_minor(minor, &dv);
    if (ps == NULL) 
        return ENXIO;

    *id = ps->device;
    return OK;
}

/*===========================================================================*
 *				ahci_get_port				     *
 *===========================================================================*/
static struct port_state *ahci_get_port(devminor_t minor)
{
	struct port_state *ps;
	struct device *dv;

	ps = ahci_map_minor(minor, &dv);
	if (ps == NULL) {
		fprintf(stderr, "device mapping for minor %d disappeared\n", minor);
		exit(EXIT_FAILURE);
	}

	return ps;
}

/*===========================================================================*
 *				main					     *
 *===========================================================================*/
#include <stdlib.h>

int main(int argc, char **argv) {
    if(argc < 1 || argv == NULL) {
        // Handle error
        return EXIT_FAILURE;
    }

    if(env_setargs(argc, argv) != 0) {
        // Handle error
        return EXIT_FAILURE;
    }

    sef_local_startup();

    if(blockdriver_mt_task(&ahci_dtab) != 0) {
        // Handle error
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
