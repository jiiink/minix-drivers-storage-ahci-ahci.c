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
static int atapi_exec(struct port_state *ps, int cmd,
	u8_t packet[ATAPI_PACKET_SIZE], size_t size, int write)
{
	cmd_fis_t fis;
	prd_t prd[1];
	int nr_prds = 0;

	if (size > AHCI_TMP_SIZE) {
		return -1;
	}

	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_PACKET;

	if (size > 0) {
		fis.cf_feat = ATA_FEAT_PACKET_DMA;
		if (!write && (ps->flags & FLAG_USE_DMADIR)) {
			fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;
		}

		prd[0].vp_addr = ps->tmp_phys;
		prd[0].vp_size = size;
		nr_prds = 1;
	}

	port_set_cmd(ps, cmd, &fis, packet, prd, nr_prds, write);

	return port_exec(ps, cmd, ahci_command_timeout);
}

/*===========================================================================*
 *				atapi_test_unit				     *
 *===========================================================================*/
static int atapi_test_unit(struct port_state *ps, int cmd)
{
	if (ps == NULL) {
		return -1;
	}

	u8_t packet[ATAPI_PACKET_SIZE];

	memset(packet, 0, sizeof(packet));
	packet[0] = ATAPI_CMD_TEST_UNIT;

	return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_request_sense			     *
 *===========================================================================*/
static int atapi_request_sense(struct port_state *ps, int cmd, int *sense)
{
    const int cdb_cmd_offset = 0;
    const int cdb_alloc_len_offset = 4;
    const int sense_data_key_offset = 2;
    const int sense_data_asc_offset = 12;
    const int sense_data_ascq_offset = 13;
    const int sense_key_mask = 0xF;

    u8_t packet[ATAPI_PACKET_SIZE];
    int ret_val;

    memset(packet, 0, sizeof(packet));
    packet[cdb_cmd_offset] = ATAPI_CMD_REQUEST_SENSE;
    packet[cdb_alloc_len_offset] = ATAPI_REQUEST_SENSE_LEN;

    ret_val = atapi_exec(ps, cmd, packet, ATAPI_REQUEST_SENSE_LEN, FALSE);

    if (ret_val != OK) {
        return ret_val;
    }

    dprintf(V_REQ, ("%s: ATAPI SENSE: sense %x ASC %x ASCQ %x\n",
        ahci_portname(ps),
        ps->tmp_base[sense_data_key_offset] & sense_key_mask,
        ps->tmp_base[sense_data_asc_offset],
        ps->tmp_base[sense_data_ascq_offset]));

    *sense = ps->tmp_base[sense_data_key_offset] & sense_key_mask;

    return OK;
}

/*===========================================================================*
 *				atapi_load_eject			     *
 *===========================================================================*/
static int atapi_load_eject(struct port_state *ps, int cmd, int load)
{
	u8_t packet[ATAPI_PACKET_SIZE];

	const int ATAPI_CDB_OPCODE_OFFSET = 0;
	const int ATAPI_CDB_LOAD_EJECT_BYTE_OFFSET = 4;

	memset(packet, 0, sizeof(packet));

	packet[ATAPI_CDB_OPCODE_OFFSET] = ATAPI_CMD_START_STOP;
	packet[ATAPI_CDB_LOAD_EJECT_BYTE_OFFSET] = load ? ATAPI_START_STOP_LOAD : ATAPI_START_STOP_EJECT;

	return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_read_capacity			     *
 *===========================================================================*/
static int atapi_read_capacity(struct port_state *ps, int cmd)
{
	u8_t packet[ATAPI_PACKET_SIZE];
	int r;

	memset(packet, 0, sizeof(packet));
	packet[0] = ATAPI_CMD_READ_CAPACITY;

	r = atapi_exec(ps, cmd, packet, ATAPI_READ_CAPACITY_LEN, FALSE);
	if (r != OK) {
		return r;
	}

	const u8_t * const buf = ps->tmp_base;

	// Extract LBA count (Last LBA + 1) from bytes 0-3 (big-endian).
	// Cast individual bytes to u32_t before shifting to prevent potential issues
	// if 'int' is narrower than 32 bits.
	u32_t lba_value = ((u32_t)buf[0] << 24) |
	                  ((u32_t)buf[1] << 16) |
	                  ((u32_t)buf[2] << 8)  |
	                  (u32_t)buf[3];
	ps->lba_count = (u64_t)lba_value + 1;

	// Extract sector size from bytes 4-7 (big-endian).
	u32_t sector_size_value = ((u32_t)buf[4] << 24) |
	                          ((u32_t)buf[5] << 16) |
	                          ((u32_t)buf[6] << 8)  |
	                          (u32_t)buf[7];
	ps->sector_size = sector_size_value;

	// Validate the sector size. It must not be zero and typically should be even.
	if (ps->sector_size == 0 || (ps->sector_size & 1)) {
		dprintf(V_ERR, ("%s: invalid medium sector size %u reported by device\n",
			ahci_portname(ps), ps->sector_size));
		return EINVAL;
	}

	// Log successful detection with key information.
	// Ensure capacity calculation uses 64-bit arithmetic to prevent overflow.
	dprintf(V_INFO,
		("%s: medium detected (sector size: %u bytes, capacity: %llu MB)\n",
		ahci_portname(ps), ps->sector_size,
		(ps->lba_count * (u64_t)ps->sector_size) / (1024ULL * 1024ULL)));

	return OK;
}

/*===========================================================================*
 *				atapi_check_medium			     *
 *===========================================================================*/
static int atapi_check_medium(struct port_state *ps, int cmd)
{
	int sense;
	bool capacity_needs_recheck = false;

	if (atapi_test_unit(ps, cmd) != OK) {
		ps->flags &= ~FLAG_HAS_MEDIUM;

		if (atapi_request_sense(ps, cmd, &sense) != OK ||
		    sense != ATAPI_SENSE_UNIT_ATT) {
			return ENXIO;
		}
		capacity_needs_recheck = true;
	} else {
		if (!(ps->flags & FLAG_HAS_MEDIUM)) {
			capacity_needs_recheck = true;
		}
	}

	if (capacity_needs_recheck) {
		if (atapi_read_capacity(ps, cmd) != OK) {
			ps->flags &= ~FLAG_HAS_MEDIUM;
			return EIO;
		}
		ps->flags |= FLAG_HAS_MEDIUM;
	}

	return OK;
}

/*===========================================================================*
 *				atapi_id_check				     *
 *===========================================================================*/
static int atapi_id_check(struct port_state *ps, u16_t *buf)
{
    u16_t gcap_val = buf[ATA_ID_GCAP];
    u16_t cap_val = buf[ATA_ID_CAP];
    u16_t dmadir_val = buf[ATA_ID_DMADIR];

    int is_atapi_removable_and_complete =
        ((gcap_val & (ATA_ID_GCAP_ATAPI_MASK | ATA_ID_GCAP_REMOVABLE | ATA_ID_GCAP_INCOMPLETE)) ==
         (ATA_ID_GCAP_ATAPI | ATA_ID_GCAP_REMOVABLE));

    int has_cap_dma = (cap_val & ATA_ID_CAP_DMA) == ATA_ID_CAP_DMA;
    int has_dmadir_dma_support = (dmadir_val & (ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA)) ==
                                   (ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA);

    if (!is_atapi_removable_and_complete || (!has_cap_dma && !has_dmadir_dma_support)) {
        dprintf(V_ERR, ("%s: unsupported ATAPI device\n", ahci_portname(ps)));
        dprintf(V_DEV, ("%s: GCAP %04x CAP %04x DMADIR %04x\n",
            ahci_portname(ps), gcap_val, cap_val, dmadir_val));
        return FALSE;
    }

    if (dmadir_val & ATA_ID_DMADIR_DMADIR)
        ps->flags |= FLAG_USE_DMADIR;

    if (((gcap_val & ATA_ID_GCAP_TYPE_MASK) >> ATA_ID_GCAP_TYPE_SHIFT) == ATAPI_TYPE_CDROM)
        ps->flags |= FLAG_READONLY;

    u16_t sup0_val = buf[ATA_ID_SUP0];
    u16_t sup1_val = buf[ATA_ID_SUP1];

    if ((sup1_val & ATA_ID_SUP1_VALID_MASK) == ATA_ID_SUP1_VALID &&
        !(ps->flags & FLAG_READONLY)) {
        if (sup0_val & ATA_ID_SUP0_WCACHE)
            ps->flags |= FLAG_HAS_WCACHE;

        if (sup1_val & ATA_ID_SUP1_FLUSH)
            ps->flags |= FLAG_HAS_FLUSH;
    }

    return TRUE;
}

/*===========================================================================*
 *				atapi_transfer				     *
 *===========================================================================*/
static int atapi_transfer(struct port_state *ps, int cmd, u64_t start_lba,
	unsigned int count, int write, prd_t *prdt, int nr_prds)
{
	cmd_fis_t fis;
	u8_t packet[ATAPI_PACKET_SIZE];

	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_PACKET;
	fis.cf_feat = ATA_FEAT_PACKET_DMA;
	if (!write && (ps->flags & FLAG_USE_DMADIR)) {
		fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;
	}

	memset(packet, 0, sizeof(packet));
	packet[0] = write ? ATAPI_CMD_WRITE : ATAPI_CMD_READ;

	packet[2] = (u8_t)((start_lba >> 24) & 0xFF);
	packet[3] = (u8_t)((start_lba >> 16) & 0xFF);
	packet[4] = (u8_t)((start_lba >>  8) & 0xFF);
	packet[5] = (u8_t)(start_lba & 0xFF);

	packet[6] = (u8_t)((count >> 24) & 0xFF);
	packet[7] = (u8_t)((count >> 16) & 0xFF);
	packet[8] = (u8_t)((count >>  8) & 0xFF);
	packet[9] = (u8_t)(count & 0xFF);

	port_set_cmd(ps, cmd, &fis, packet, prdt, nr_prds, write);

	return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				ata_id_check				     *
 *===========================================================================*/
static int ata_id_check(struct port_state *ps, u16_t *buf)
{
	const char *port_name = ahci_portname(ps);
	u16_t gcap = buf[ATA_ID_GCAP];
	u16_t cap = buf[ATA_ID_CAP];
	u16_t sup1 = buf[ATA_ID_SUP1];
	u16_t sata_cap = buf[ATA_ID_SATA_CAP];
	u16_t plss = buf[ATA_ID_PLSS];
	u16_t sup0 = buf[ATA_ID_SUP0];
	u16_t ena2 = buf[ATA_ID_ENA2];

	bool is_ata_device = ((gcap & ATA_ID_GCAP_ATA_MASK) == ATA_ID_GCAP_ATA);
	bool has_removable_media = ((gcap & ATA_ID_GCAP_REMOVABLE) != 0);
	bool is_incomplete = ((gcap & ATA_ID_GCAP_INCOMPLETE) != 0);

	if (!is_ata_device || has_removable_media || is_incomplete) {
		dprintf(V_ERR, ("%s: unsupported ATA device. GCAP(0x%04x): ATA=%s, Removable=%s, Incomplete=%s\n",
			port_name, gcap,
			is_ata_device ? "OK" : "FAIL",
			has_removable_media ? "FAIL" : "OK",
			is_incomplete ? "FAIL" : "OK"));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x SUP1 %04x\n", port_name, gcap, cap, sup1));
		return FALSE;
	}

	bool supports_lba_dma = ((cap & (ATA_ID_CAP_LBA | ATA_ID_CAP_DMA)) == (ATA_ID_CAP_LBA | ATA_ID_CAP_DMA));
	if (!supports_lba_dma) {
		dprintf(V_ERR, ("%s: unsupported ATA device. CAP(0x%04x): LBA and DMA support missing\n", port_name, cap));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x SUP1 %04x\n", port_name, gcap, cap, sup1));
		return FALSE;
	}

	bool supports_flush_lba48 = ((sup1 & (ATA_ID_SUP1_VALID_MASK | ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48)) == (ATA_ID_SUP1_VALID | ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48));
	if (!supports_flush_lba48) {
		dprintf(V_ERR, ("%s: unsupported ATA device. SUP1(0x%04x): Missing valid, flush, or LBA48 support\n", port_name, sup1));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x SUP1 %04x\n", port_name, gcap, cap, sup1));
		return FALSE;
	}

	ps->lba_count = ((u64_t)buf[ATA_ID_LBA3] << 48) |
			((u64_t)buf[ATA_ID_LBA2] << 32) |
			((u64_t)buf[ATA_ID_LBA1] << 16) |
			(u64_t)buf[ATA_ID_LBA0];

	if (hba_state.has_ncq && (sata_cap & ATA_ID_SATA_CAP_NCQ)) {
		ps->flags |= FLAG_HAS_NCQ;
		ps->queue_depth = (buf[ATA_ID_QDEPTH] & ATA_ID_QDEPTH_MASK) + 1;
		if (ps->queue_depth > hba_state.nr_cmds) {
			ps->queue_depth = hba_state.nr_cmds;
		}
	}

	bool long_logical_sectors_supported = ((plss & (ATA_ID_PLSS_VALID_MASK | ATA_ID_PLSS_LLS)) == (ATA_ID_PLSS_VALID | ATA_ID_PLSS_LLS));
	if (long_logical_sectors_supported) {
		ps->sector_size = (((u32_t)buf[ATA_ID_LSS1] << 16) | (u32_t)buf[ATA_ID_LSS0]) << 1;
	} else {
		ps->sector_size = ATA_SECTOR_SIZE;
	}

	if (ps->sector_size < ATA_SECTOR_SIZE) {
		dprintf(V_ERR, ("%s: invalid sector size %u (minimum %u)\n", port_name, ps->sector_size, ATA_SECTOR_SIZE));
		return FALSE;
	}

	ps->flags |= FLAG_HAS_MEDIUM | FLAG_HAS_FLUSH;

	if (sup0 & ATA_ID_SUP0_WCACHE) {
		ps->flags |= FLAG_HAS_WCACHE;
	}

	bool supports_fua = ((ena2 & (ATA_ID_ENA2_VALID_MASK | ATA_ID_ENA2_FUA)) == (ATA_ID_ENA2_VALID | ATA_ID_ENA2_FUA));
	if (supports_fua) {
		ps->flags |= FLAG_HAS_FUA;
	}

	return TRUE;
}

/*===========================================================================*
 *				ata_transfer				     *
 *===========================================================================*/
static int ata_transfer(struct port_state *ps, int cmd, u64_t start_lba,
	unsigned int count, int write, int force, prd_t *prdt, int nr_prds)
{
	cmd_fis_t fis;
	unsigned char selected_cmd;
	unsigned char dev_flags = ATA_DEV_LBA;
	int use_ncq = (ps->flags & FLAG_HAS_NCQ) != 0;
	int use_fua = force && (ps->flags & FLAG_HAS_FUA) != 0;

	assert(count <= ATA_MAX_SECTORS);

	if (count == ATA_MAX_SECTORS) {
		count = 0;
	}

	memset(&fis, 0, sizeof(fis));

	if (use_ncq) {
		if (write) {
			selected_cmd = ATA_CMD_WRITE_FPDMA_QUEUED;
			if (use_fua) {
				dev_flags |= ATA_DEV_FUA;
			}
		} else {
			selected_cmd = ATA_CMD_READ_FPDMA_QUEUED;
		}
	} else {
		if (write) {
			if (use_fua) {
				selected_cmd = ATA_CMD_WRITE_DMA_FUA_EXT;
			} else {
				selected_cmd = ATA_CMD_WRITE_DMA_EXT;
			}
		} else {
			selected_cmd = ATA_CMD_READ_DMA_EXT;
		}
	}

	fis.cf_cmd = selected_cmd;
	fis.cf_dev = dev_flags;

	fis.cf_lba = start_lba & 0x00FFFFFFUL;
	fis.cf_lba_exp = (start_lba >> 24) & 0x00FFFFFFUL;
	fis.cf_sec = count & 0xFF;
	fis.cf_sec_exp = (count >> 8) & 0xFF;

	port_set_cmd(ps, cmd, &fis, NULL, prdt, nr_prds, write);

	return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				gen_identify				     *
 *===========================================================================*/
static int gen_identify(struct port_state *ps, int blocking)
{
	cmd_fis_t fis;
	prd_t prd;
	int result;

	memset(&fis, 0, sizeof(fis));

	if (ps->flags & FLAG_ATAPI) {
		fis.cf_cmd = ATA_CMD_IDENTIFY_PACKET;
	} else {
		fis.cf_cmd = ATA_CMD_IDENTIFY;
	}

	prd.vp_addr = ps->tmp_phys;
	prd.vp_size = ATA_ID_SIZE;

	port_set_cmd(ps, 0, &fis, NULL, &prd, 1, FALSE);

	if (blocking) {
		result = port_exec(ps, 0, ahci_command_timeout);
	} else {
		result = port_issue(ps, 0, ahci_command_timeout);
	}

	return result;
}

/*===========================================================================*
 *				gen_flush_wcache			     *
 *===========================================================================*/
static int gen_flush_wcache(struct port_state *ps)
{
	if (ps == NULL) {
		return EINVAL;
	}

	if (!(ps->flags & FLAG_HAS_FLUSH)) {
		return EINVAL;
	}

	cmd_fis_t fis = { .cf_cmd = ATA_CMD_FLUSH_CACHE };

	port_set_cmd(ps, 0, &fis, NULL, NULL, 0, FALSE);

	return port_exec(ps, 0, ahci_flush_timeout);
}

/*===========================================================================*
 *				gen_get_wcache				     *
 *===========================================================================*/
static int gen_get_wcache(struct port_state *ps, int *val)
{
    if (ps == NULL || val == NULL) {
        return EINVAL;
    }

    if (!(ps->flags & FLAG_HAS_WCACHE)) {
        return EINVAL;
    }

    int result_code = gen_identify(ps, TRUE /*blocking*/);
    if (result_code != OK) {
        return result_code;
    }

    *val = !!(((u16_t *) ps->tmp_base)[ATA_ID_ENA0] & ATA_ID_ENA0_WCACHE);

    return OK;
}

/*===========================================================================*
 *				gen_set_wcache				     *
 *===========================================================================*/
static int gen_set_wcache(struct port_state *ps, int enable)
{
	cmd_fis_t fis;
	clock_t timeout;

	if (!(ps->flags & FLAG_HAS_WCACHE))
		return EINVAL;

	/* Disabling the write cache causes a (blocking) cache flush. Cache
	 * flushes may take much longer than regular commands.
	 */
	timeout = enable ? ahci_command_timeout : ahci_flush_timeout;

	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_SET_FEATURES;
	fis.cf_feat = enable ? ATA_SF_EN_WCACHE : ATA_SF_DI_WCACHE;

	port_set_cmd(ps, 0, &fis, NULL /*packet*/, NULL /*prdt*/, 0, FALSE /*write*/);

	return port_exec(ps, 0, timeout);
}

/*===========================================================================*
 *				ct_set_fis				     *
 *===========================================================================*/
static vir_bytes ct_set_fis(u8_t *ct, cmd_fis_t *fis, unsigned int tag)
{
	memset(ct, 0, ATA_H2D_SIZE);

	ct[ATA_FIS_TYPE] = (u8_t)ATA_FIS_TYPE_H2D;
	ct[ATA_H2D_FLAGS] = (u8_t)ATA_H2D_FLAGS_C;
	ct[ATA_H2D_CMD] = (u8_t)fis->cf_cmd;

	ct[ATA_H2D_LBA_LOW] = (u8_t)(fis->cf_lba & 0xFF);
	ct[ATA_H2D_LBA_MID] = (u8_t)((fis->cf_lba >> 8) & 0xFF);
	ct[ATA_H2D_LBA_HIGH] = (u8_t)((fis->cf_lba >> 16) & 0xFF);

	ct[ATA_H2D_DEV] = (u8_t)fis->cf_dev;

	ct[ATA_H2D_LBA_LOW_EXP] = (u8_t)(fis->cf_lba_exp & 0xFF);
	ct[ATA_H2D_LBA_MID_EXP] = (u8_t)((fis->cf_lba_exp >> 8) & 0xFF);
	ct[ATA_H2D_LBA_HIGH_EXP] = (u8_t)((fis->cf_lba_exp >> 16) & 0xFF);

	ct[ATA_H2D_CTL] = (u8_t)fis->cf_ctl;

	if (ATA_IS_FPDMA_CMD(fis->cf_cmd)) {
		ct[ATA_H2D_FEAT] = (u8_t)fis->cf_sec;
		ct[ATA_H2D_FEAT_EXP] = (u8_t)fis->cf_sec_exp;
		ct[ATA_H2D_SEC] = (u8_t)((tag << ATA_SEC_TAG_SHIFT) & 0xFF);
		ct[ATA_H2D_SEC_EXP] = (u8_t)0;
	} else {
		ct[ATA_H2D_FEAT] = (u8_t)fis->cf_feat;
		ct[ATA_H2D_FEAT_EXP] = (u8_t)fis->cf_feat_exp;
		ct[ATA_H2D_SEC] = (u8_t)fis->cf_sec;
		ct[ATA_H2D_SEC_EXP] = (u8_t)fis->cf_sec_exp;
	}

	return ATA_H2D_SIZE;
}

/*===========================================================================*
 *				ct_set_packet				     *
 *===========================================================================*/
#include <string.h>

static void ct_set_packet(u8_t *ct, u8_t packet[ATAPI_PACKET_SIZE])
{
	if (ct == NULL || packet == NULL) {
		return;
	}

	memcpy(&ct[AHCI_CT_PACKET_OFF], packet, ATAPI_PACKET_SIZE);
}

/*===========================================================================*
 *				ct_set_prdt				     *
 *===========================================================================*/
#define AHCI_PRDT_RESERVED_DWORD_VALUE 0U

typedef struct {
    u32_t data_base_address_lower;
    u32_t data_base_address_upper;
    u32_t reserved_dw2;
    u32_t byte_count_and_dbc;
} ahci_ct_prdt_entry_t;


static void ct_set_prdt(u8_t *ct, prd_t *prdt, int nr_prds)
{
    ahci_ct_prdt_entry_t *p_entry;
    int i;

    if (ct == NULL || prdt == NULL) {
        return;
    }

    p_entry = (ahci_ct_prdt_entry_t *)(ct + AHCI_CT_PRDT_OFF);

    for (i = 0; i < nr_prds; i++) {
        p_entry->data_base_address_lower = prdt->vp_addr;
        p_entry->data_base_address_upper = AHCI_PRDT_RESERVED_DWORD_VALUE;
        p_entry->reserved_dw2            = AHCI_PRDT_RESERVED_DWORD_VALUE;
        p_entry->byte_count_and_dbc      = prdt->vp_size - 1;

        p_entry++;
        prdt++;
    }
}

/*===========================================================================*
 *				port_set_cmd				     *
 *===========================================================================*/
static void port_set_cmd(struct port_state *ps, int cmd, cmd_fis_t *fis,
	u8_t packet[ATAPI_PACKET_SIZE], prd_t *prdt, int nr_prds, int write)
{
	u8_t *ct;
	u32_t *cl;
	vir_bytes fis_size;
	u32_t prefetchable_flag = 0;
	u32_t write_flag = 0;
	u32_t atapi_flag = 0;
	u32_t fis_length_field = 0;
	u32_t prdt_length_field = 0;

	if (ATA_IS_FPDMA_CMD(fis->cf_cmd)) {
		ps->flags |= FLAG_NCQ_MODE;
	} else {
		assert(!ps->pend_mask);
		ps->flags &= ~FLAG_NCQ_MODE;
	}

	ct = ps->ct_base[cmd];

	assert(ct != NULL);
	assert(nr_prds <= NR_PRDS);

	fis_size = ct_set_fis(ct, fis, cmd);

	if (packet != NULL)
		ct_set_packet(ct, packet);

	ct_set_prdt(ct, prdt, nr_prds);

	cl = &ps->cl_base[cmd * AHCI_CL_ENTRY_DWORDS];

	memset(cl, 0, AHCI_CL_ENTRY_SIZE);

	prdt_length_field = (u32_t)nr_prds << AHCI_CL_PRDTL_SHIFT;

	if (!ATA_IS_FPDMA_CMD(fis->cf_cmd) && (nr_prds > 0 || packet != NULL)) {
		prefetchable_flag = AHCI_CL_PREFETCHABLE;
	}

	if (write) {
		write_flag = AHCI_CL_WRITE;
	}

	if (packet != NULL) {
		atapi_flag = AHCI_CL_ATAPI;
	}

	fis_length_field = (fis_size / sizeof(u32_t)) << AHCI_CL_CFL_SHIFT;

	cl[0] = prdt_length_field | prefetchable_flag | write_flag | atapi_flag | fis_length_field;
	cl[2] = ps->ct_phys[cmd];
}

/*===========================================================================*
 *				port_finish_cmd				     *
 *===========================================================================*/
static void port_finish_cmd(struct port_state *ps, int cmd, int result)
{
	assert(cmd < ps->queue_depth);

	dprintf(V_REQ, ("%s: command %d %s\n", ahci_portname(ps),
		cmd, (result == RESULT_SUCCESS) ? "succeeded" : "failed"));

	ps->cmd_info[cmd].result = result;

	assert(ps->pend_mask & (1U << cmd));
	ps->pend_mask &= ~(1U << cmd);

	if (ps->state != STATE_WAIT_ID)
		blockdriver_mt_wakeup(ps->cmd_info[cmd].tid);
}

/*===========================================================================*
 *				port_fail_cmds				     *
 *===========================================================================*/
static void port_fail_cmds(struct port_state *ps)
{
    if (ps == NULL) {
        return;
    }

    const unsigned int MAX_ALLOWED_BIT_INDEX = sizeof(unsigned int) * __CHAR_BIT__ - 1;

    unsigned int i;

    for (i = 0; i < ps->queue_depth; i++) {
        if (ps->pend_mask == 0) {
            break;
        }

        if (i > MAX_ALLOWED_BIT_INDEX) {
            break;
        }

        if (ps->pend_mask & (1U << i)) {
            port_finish_cmd(ps, i, RESULT_FAILURE);
        }
    }
}

/*===========================================================================*
 *				port_check_cmds				     *
 *===========================================================================*/
static void port_check_cmds(struct port_state *ps)
{
	u32_t mask;
	if (ps->flags & FLAG_NCQ_MODE) {
		mask = port_read(ps, AHCI_PORT_SACT);
	} else {
		mask = port_read(ps, AHCI_PORT_CI);
	}

	u32_t done = ps->pend_mask & ~mask;

	for (unsigned int i = 0; i < ps->queue_depth; ++i) {
		if (done & (1U << i)) {
			port_finish_cmd(ps, i, RESULT_SUCCESS);
		}
	}
}

/*===========================================================================*
 *				port_find_cmd				     *
 *===========================================================================*/
static int port_find_cmd(struct port_state *ps)
{
    if (ps == NULL || ps->queue_depth <= 0) {
        return -1;
    }

    int i;
    for (i = 0; i < ps->queue_depth; i++) {
        if (!((ps->pend_mask) & (1U << i))) {
            break;
        }
    }

    if (i == ps->queue_depth) {
        return -1;
    }

    return i;
}

/*===========================================================================*
 *				port_get_padbuf				     *
 *===========================================================================*/
static int port_get_padbuf(struct port_state *ps, size_t size)
{
    if (ps->pad_base != NULL && ps->pad_size >= size) {
        return OK;
    }

    if (ps->pad_base != NULL) {
        free_contig(ps->pad_base, ps->pad_size);
        ps->pad_base = NULL;
        ps->pad_size = 0;
        ps->pad_phys = 0;
    }

    if (size == 0) {
        return OK;
    }

    void *new_base;
    uintptr_t new_phys = 0;

    new_base = alloc_contig(size, 0, &new_phys);

    if (new_base == NULL) {
        dprintf(V_ERR, ("%s: unable to allocate a padding buffer of "
                "size %lu\n", ahci_portname(ps), (unsigned long) size));
        return ENOMEM;
    }

    ps->pad_base = new_base;
    ps->pad_size = size;
    ps->pad_phys = new_phys;

    dprintf(V_INFO, ("%s: allocated padding buffer of size %lu\n",
            ahci_portname(ps), (unsigned long) size));

    return OK;
}

/*===========================================================================*
 *				sum_iovec				     *
 *===========================================================================*/
static int sum_iovec(struct port_state *ps, endpoint_t endpt,
	iovec_s_t *iovec, int nr_req, vir_bytes *total)
{
	vir_bytes current_size;
	vir_bytes accumulated_bytes = 0;
	int i;

	if (total == NULL) {
		dprintf(V_ERR, ("%s: total pointer is NULL from %d\n", ahci_portname(ps), endpt));
		return EINVAL;
	}

	if (nr_req < 0) {
		dprintf(V_ERR, ("%s: negative nr_req %d from %d\n", ahci_portname(ps), nr_req, endpt));
		return EINVAL;
	}

	if (nr_req > 0 && iovec == NULL) {
		dprintf(V_ERR, ("%s: iovec pointer is NULL for %d requests from %d\n", ahci_portname(ps), nr_req, endpt));
		return EINVAL;
	}

	for (i = 0; i < nr_req; i++) {
		current_size = iovec[i].iov_size;

		if (current_size == 0 || (current_size & 1) || current_size > (vir_bytes)LONG_MAX) {
			dprintf(V_ERR, ("%s: bad size %lu in iovec from %d\n",
				ahci_portname(ps), current_size, endpt));
			return EINVAL;
		}

		if (current_size > (vir_bytes)LONG_MAX - accumulated_bytes) {
			dprintf(V_ERR, ("%s: iovec size overflow from %d\n",
				ahci_portname(ps), endpt));
			return EINVAL;
		}

		accumulated_bytes += current_size;
	}

	*total = accumulated_bytes;
	return OK;
}

/*===========================================================================*
 *				setup_prdt				     *
 *===========================================================================*/
static int setup_prdt(struct port_state *ps, endpoint_t endpt,
	iovec_s_t *iovec, int nr_req, vir_bytes total_transfer_size, vir_bytes lead_padding_size,
	int write, prd_t *prdt_out)
{
	struct vumap_vir vvec_input[NR_PRDS];
	size_t current_iovec_bytes;
	int iovec_idx, r;
	int prds_generated_by_vumap;
	int current_nr_prds = 0;

	if (lead_padding_size > 0) {
		if (current_nr_prds >= NR_PRDS) {
			dprintf(V_ERR, ("%s: PRDT buffer full for leading padding\n", ahci_portname(ps)));
			return EFBIG;
		}
		if ((r = port_get_padbuf(ps, ps->sector_size)) != OK) {
			return r;
		}

		prdt_out[current_nr_prds].vp_addr = ps->pad_phys;
		prdt_out[current_nr_prds].vp_size = lead_padding_size;
		current_nr_prds++;
	}

	vir_bytes total_data_and_lead = lead_padding_size + total_transfer_size;
	vir_bytes trail_padding_size = (ps->sector_size - (total_data_and_lead % ps->sector_size)) % ps->sector_size;

	int num_iovec_entries_prepared = 0;
	vir_bytes remaining_transfer_size = total_transfer_size;

	for (iovec_idx = 0; iovec_idx < nr_req && remaining_transfer_size > 0; iovec_idx++) {
		if (num_iovec_entries_prepared >= NR_PRDS) {
			dprintf(V_ERR, ("%s: vvec_input buffer full for I/O vector entry %d\n", ahci_portname(ps), iovec_idx));
			return EFBIG;
		}

		current_iovec_bytes = MIN(iovec[iovec_idx].iov_size, remaining_transfer_size);

		if (endpt == SELF) {
			vvec_input[num_iovec_entries_prepared].vv_addr = (vir_bytes) iovec[iovec_idx].iov_grant;
		} else {
			vvec_input[num_iovec_entries_prepared].vv_grant = iovec[iovec_idx].iov_grant;
		}

		vvec_input[num_iovec_entries_prepared].vv_size = current_iovec_bytes;

		remaining_transfer_size -= current_iovec_bytes;
		num_iovec_entries_prepared++;
	}

	int prds_start_idx_for_vumap = current_nr_prds;
	prds_generated_by_vumap = 0;

	if (num_iovec_entries_prepared > 0) {
		if (prds_start_idx_for_vumap + num_iovec_entries_prepared > NR_PRDS) {
			dprintf(V_ERR, ("%s: PRDT buffer cannot accommodate all %d I/O vector entries from vumap\n",
				ahci_portname(ps), num_iovec_entries_prepared));
			return EFBIG;
		}

		r = sys_vumap(endpt, vvec_input, num_iovec_entries_prepared, 0,
					write ? VUA_READ : VUA_WRITE,
					&prdt_out[prds_start_idx_for_vumap], &prds_generated_by_vumap);

		if (r != OK) {
			dprintf(V_ERR, ("%s: unable to map memory from %d (%d)\n",
				ahci_portname(ps), endpt, r));
			return r;
		}
	}

	if (prds_generated_by_vumap < 0 || prds_generated_by_vumap > num_iovec_entries_prepared) {
		dprintf(V_ERR, ("%s: sys_vumap returned invalid PRD count %d (expected 0..%d)\n",
			ahci_portname(ps), prds_generated_by_vumap, num_iovec_entries_prepared));
		return EIO;
	}
	if (num_iovec_entries_prepared > 0 && prds_generated_by_vumap == 0) {
		dprintf(V_ERR, ("%s: sys_vumap mapped 0 PRDs for %d input entries\n",
			ahci_portname(ps), num_iovec_entries_prepared));
		return EIO;
	}

	for (iovec_idx = 0; iovec_idx < prds_generated_by_vumap; iovec_idx++) {
		prd_t *current_prd_entry = &prdt_out[prds_start_idx_for_vumap + iovec_idx];
		struct vumap_vir *current_vvec_entry = &vvec_input[iovec_idx];

		if (current_vvec_entry->vv_size != current_prd_entry->vp_size) {
			dprintf(V_ERR, ("%s: non-contiguous memory from %d (vvec_input[%d].size=%zu, prdt_out[%d].size=%zu)\n",
				ahci_portname(ps), endpt, iovec_idx, current_vvec_entry->vv_size,
				prds_start_idx_for_vumap + iovec_idx, current_prd_entry->vp_size));
			return EINVAL;
		}

		if (current_prd_entry->vp_addr & 1) {
			dprintf(V_ERR, ("%s: bad physical address from %d (prdt_out[%d].addr=%lx)\n",
				ahci_portname(ps), endpt, prds_start_idx_for_vumap + iovec_idx,
				(unsigned long)current_prd_entry->vp_addr));
			return EINVAL;
		}
	}
	current_nr_prds += prds_generated_by_vumap;

	if (trail_padding_size > 0) {
		if (current_nr_prds >= NR_PRDS) {
			dprintf(V_ERR, ("%s: PRDT buffer full for trailing padding\n", ahci_portname(ps)));
			return EFBIG;
		}
		prdt_out[current_nr_prds].vp_addr = ps->pad_phys + lead_padding_size;
		prdt_out[current_nr_prds].vp_size = trail_padding_size;
		current_nr_prds++;
	}

	return current_nr_prds;
}

/*===========================================================================*
 *				port_transfer				     *
 *===========================================================================*/
static ssize_t port_transfer(struct port_state *ps, const u64_t pos, const u64_t eof,
	const endpoint_t endpt, iovec_s_t *iovec, const int nr_req, const int write, const int flags)
{
	prd_t prdt[NR_PRDS];
	vir_bytes size, lead;
	unsigned int count;
	int nr_prds_ret;
	u64_t start_lba;
	int r, cmd;

	assert(ps->state == STATE_GOOD_DEV);
	assert(ps->flags & FLAG_HAS_MEDIUM);
	assert(ps->sector_size > 0);

	r = sum_iovec(ps, endpt, iovec, nr_req, &size);
	if (r != OK) {
		return (ssize_t)(r > 0 ? -r : r);
	}

	dprintf(V_REQ, ("%s: %s for %lu bytes at pos %llx\n",
		ahci_portname(ps), write ? "write" : "read", (unsigned long)size, pos));

	if (size > MAX_TRANSFER) {
		size = MAX_TRANSFER;
	}

	if (pos + size > eof) {
		size = (vir_bytes) (eof - pos);
	}

    if (size == 0) {
        return 0;
    }

	start_lba = pos / ps->sector_size;
	lead = (vir_bytes) (pos % ps->sector_size);
	count = (lead + size + ps->sector_size - 1) / ps->sector_size;

	if ((lead & 1) || (write && lead != 0)) {
		dprintf(V_ERR, ("%s: unaligned position from %d\n",
			ahci_portname(ps), endpt));
		return (ssize_t)-EINVAL;
	}

	if (write && (size % ps->sector_size) != 0) {
		dprintf(V_ERR, ("%s: unaligned size %lu from %d\n",
			ahci_portname(ps), (unsigned long)size, endpt));
		return (ssize_t)-EINVAL;
	}

	nr_prds_ret = setup_prdt(ps, endpt, iovec, nr_req, size, lead, write, prdt);

	if (nr_prds_ret < 0) {
		return (ssize_t)nr_prds_ret;
	}
    unsigned int nr_prds = (unsigned int)nr_prds_ret;

	cmd = port_find_cmd(ps);

	if (ps->flags & FLAG_ATAPI) {
		r = atapi_transfer(ps, cmd, start_lba, count, write, prdt, nr_prds);
	} else {
		r = ata_transfer(ps, cmd, start_lba, count, write,
			!!(flags & BDEV_FORCEWRITE), prdt, nr_prds);
	}

	if (r != OK) {
		return (ssize_t)(r > 0 ? -r : r);
	}

	return (ssize_t)size;
}

/*===========================================================================*
 *				port_hardreset				     *
 *===========================================================================*/
static void port_hardreset(struct port_state *ps)
{
    if (ps == NULL) {
        return;
    }

    port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_INIT);

    micro_delay(COMRESET_DELAY * 1000);

    port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_NONE);
}

/*===========================================================================*
 *				port_override				     *
 *===========================================================================*/
#define AHCI_CLO_MAX_ATTEMPTS 100000

static void port_override(struct port_state *ps)
{
	u32_t cmd;
	unsigned int attempts = 0;
	volatile unsigned int delay_loop_counter;

	cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_CLO);

	while ((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CLO)) {
		if (attempts >= AHCI_CLO_MAX_ATTEMPTS) {
			dprintf(V_ERR, ("%s: CLO bit clear timeout after %u attempts\n", ahci_portname(ps), attempts));
			return;
		}
		for (delay_loop_counter = 0; delay_loop_counter < PORTREG_DELAY; ++delay_loop_counter);
		attempts++;
	}

	dprintf(V_INFO, ("%s: overridden\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_start				     *
 *===========================================================================*/
static void port_start(struct port_state *ps)
{
	u32_t cmd;

	if (ps == NULL) {
		return;
	}

	port_write(ps, AHCI_PORT_SERR, ~0U);
	port_write(ps, AHCI_PORT_IS, ~0U);

	cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_ST);

	dprintf(V_INFO, ("%s: started\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_stop				     *
 *===========================================================================*/
static int port_stop(struct port_state *ps)
{
    u32_t cmd;
    int ret = 0;
    unsigned int attempts = 0;
    bool stopped = false;

    // Define a maximum number of attempts for the busy-wait loop.
    // This prevents indefinite blocking if the hardware state doesn't change.
    // The specific value should be tuned based on hardware characteristics and PORTREG_DELAY.
    // For example, if PORTREG_DELAY is 1ms, 5000 attempts mean a 5-second timeout.
    const unsigned int MAX_STOP_ATTEMPTS = 5000; 

    cmd = port_read(ps, AHCI_PORT_CMD);

    // Check if the port is currently running (CR) or started (ST).
    // Only attempt to stop if it's in an active state.
    if (cmd & (AHCI_PORT_CMD_CR | AHCI_PORT_CMD_ST)) {
        // Clear the 'Start' (ST) bit to initiate a stop sequence.
        port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_ST);

        // Poll for the 'Command Running' (CR) bit to clear, indicating the port has stopped.
        // This loop includes a timeout to improve reliability and prevent system hangs.
        while (attempts < MAX_STOP_ATTEMPTS) {
            u32_t current_cmd_status = port_read(ps, AHCI_PORT_CMD);
            if (!(current_cmd_status & AHCI_PORT_CMD_CR)) {
                stopped = true;
                break;
            }
            // Introduce a delay to prevent excessive CPU usage during polling
            // and allow the hardware time to respond.
            // mdelay is assumed to be an available delay function (e.g., from a kernel context).
            mdelay(PORTREG_DELAY);
            attempts++;
        }

        if (!stopped) {
            // If the port failed to stop within the allotted time, report an error.
            dprintf(V_ERR, ("%s: failed to stop, AHCI_PORT_CMD_CR still set after %u attempts\n", ahci_portname(ps), attempts));
            ret = -1; // Indicate failure to the caller
        } else {
            // Log successful stop operation.
            dprintf(V_INFO, ("%s: stopped\n", ahci_portname(ps)));
        }
    }
    // If the port was not running or started, no action is needed, and it's considered a success.
    
    return ret;
}

/*===========================================================================*
 *				port_restart				     *
 *===========================================================================*/
static void port_restart(struct port_state *ps)
{
	port_fail_cmds(ps);
	port_stop(ps);

	uint32_t tfd_status = port_read(ps, AHCI_PORT_TFD);

	if (tfd_status & (AHCI_PORT_TFD_STS_BSY | AHCI_PORT_TFD_STS_DRQ)) {
		dprintf(V_ERR, ("%s: port reset\n", ahci_portname(ps)));
		port_disconnect(ps);
		port_hardreset(ps);
		return;
	}

	port_start(ps);
}

/*===========================================================================*
 *				print_string				     *
 *===========================================================================*/
#define SPACE_CHAR_VALUE 0x20
#define SPACE_WORD_VALUE 0x2020

static void print_string(u16_t *buf, int start, int end)
{
    int current_effective_end = end;
    unsigned char partial_character = 0;
    int had_partial_word_at_end = 0;

    while (current_effective_end >= start && buf[current_effective_end] == SPACE_WORD_VALUE) {
        current_effective_end--;
    }

    if (current_effective_end >= start && (buf[current_effective_end] & 0xFF) == SPACE_CHAR_VALUE) {
        partial_character = (unsigned char)(buf[current_effective_end] >> 8);
        had_partial_word_at_end = 1;
        current_effective_end--;
    }

    for (int i = start; i <= current_effective_end; i++) {
        printf("%c%c", (unsigned char)(buf[i] >> 8), (unsigned char)(buf[i] & 0xFF));
    }

    if (had_partial_word_at_end) {
        printf("%c", partial_character);
    }
}

/*===========================================================================*
 *				port_id_check				     *
 *===========================================================================*/
static void print_device_identification_info(struct port_state *ps, const u16_t *id_buffer) {
    if (ahci_verbose >= V_INFO) {
        printf("%s: ATA%s, ", ahci_portname(ps),
            (ps->flags & FLAG_ATAPI) ? "PI" : "");
        print_string(id_buffer, 27, 46); // Model number
        if (ahci_verbose >= V_DEV) {
            printf(" (");
            print_string(id_buffer, 10, 19); // Serial number
            printf(", ");
            print_string(id_buffer, 23, 26); // Firmware revision
            printf(")");
        }

        if (ps->flags & FLAG_HAS_MEDIUM)
            printf(", %u byte sectors, %llu MB size",
                ps->sector_size,
                ps->lba_count * ps->sector_size / (1024ULL * 1024ULL));

        printf("\n");
    }
}

static void port_id_check(struct port_state *ps, int cmd_completed_successfully)
{
	u16_t *id_buffer = (u16_t *) ps->tmp_base;
	int device_identified_successfully = FALSE;

	assert(ps->state == STATE_WAIT_ID);

	ps->flags &= ~FLAG_BUSY;
	cancel_timer(&ps->cmd_info[0].timer);

	if (!cmd_completed_successfully) {
		/* The device identification command timed out or failed.
		 * If it wasn't an ATAPI device and the port signature doesn't match ATA,
		 * try identifying it as an ATAPI device.
		 */
		if (!(ps->flags & FLAG_ATAPI) && port_read(ps, AHCI_PORT_SIG) != ATA_SIG_ATA) {
			dprintf(V_INFO, ("%s: may not be ATA, trying ATAPI\n", ahci_portname(ps)));
			ps->flags |= FLAG_ATAPI;
			(void) gen_identify(ps, FALSE /*blocking*/);
			return; /* Restart identification process for ATAPI */
		} else {
			dprintf(V_ERR,
				("%s: unable to identify device (command failed)\n", ahci_portname(ps)));
			/* device_identified_successfully remains FALSE */
		}
	} else {
		/* The identification command succeeded, now check the results. */
		if (ps->flags & FLAG_ATAPI) {
			device_identified_successfully = atapi_id_check(ps, id_buffer);
		} else {
			device_identified_successfully = ata_id_check(ps, id_buffer);
		}

		if (!device_identified_successfully) {
			dprintf(V_ERR,
				("%s: unable to identify device (ID check failed)\n", ahci_portname(ps)));
		}
	}

	/* Finalize port state based on identification result */
	if (device_identified_successfully) {
		ps->state = STATE_GOOD_DEV;
		print_device_identification_info(ps, id_buffer);
	} else {
		/* Device could not be identified successfully. Mark as unusable. */
		port_stop(ps);
		ps->state = STATE_BAD_DEV;
		port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PRCE);
	}
}

/*===========================================================================*
 *				port_connect				     *
 *===========================================================================*/
static void port_connect(struct port_state *ps)
{
	u32_t status, sig;

	dprintf(V_INFO, ("%s: device connected\n", ahci_portname(ps)));

	port_start(ps);

	status = port_read(ps, AHCI_PORT_SSTS) & AHCI_PORT_SSTS_DET_MASK;

	if (status != AHCI_PORT_SSTS_DET_PHY) {
		dprintf(V_ERR, ("%s: device vanished!\n", ahci_portname(ps)));

		port_stop(ps);

		ps->state = STATE_NO_DEV;
		ps->flags &= ~FLAG_BUSY;

		return;
	}

	ps->flags &= (FLAG_BUSY | FLAG_BARRIER | FLAG_SUSPENDED);

	sig = port_read(ps, AHCI_PORT_SIG);

	if (sig == ATA_SIG_ATAPI)
		ps->flags |= FLAG_ATAPI;

	ps->state = STATE_WAIT_ID;
	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_MASK);

	(void) gen_identify(ps, FALSE);
}

/*===========================================================================*
 *				port_disconnect				     *
 *===========================================================================*/
static void port_disconnect(struct port_state *ps)
{
	dprintf(V_INFO, ("%s: device disconnected\n", ahci_portname(ps)));
	ps->state = STATE_NO_DEV;
	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);
	ps->flags &= ~FLAG_BUSY;
	port_fail_cmds(ps);
	ps->flags |= FLAG_BARRIER;
	blockdriver_mt_set_workers(ps->device, 1);
}

/*===========================================================================*
 *				port_dev_check				     *
 *===========================================================================*/
static void port_dev_check(struct port_state *ps)
{
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

	if ((status == AHCI_PORT_SSTS_DET_PHY || status == AHCI_PORT_SSTS_DET_DET) && ps->left > 0) {
		ps->left--;
		set_timer(&ps->cmd_info[0].timer, ahci_device_delay,
			port_timeout, BUILD_ARG(ps - port_state, 0));
		return;
	}

	dprintf(V_INFO, ("%s: device not ready or timed out\n", ahci_portname(ps)));

	if (status == AHCI_PORT_SSTS_DET_PHY) {
		if (hba_state.has_clo) {
			port_override(ps);
			port_connect(ps);
			return;
		}

		ps->state = STATE_BAD_DEV;
		port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PRCE);
	} else {
		ps->state = STATE_NO_DEV;
		ps->flags &= ~FLAG_BUSY;
	}
}

/*===========================================================================*
 *				port_intr				     *
 *===========================================================================*/
static bool port_handle_device_attached(struct port_state *ps)
{
	port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_X);
	dprintf(V_DEV, ("%s: device attached\n", ahci_portname(ps)));

	switch (ps->state) {
	case STATE_SPIN_UP:
		cancel_timer(&ps->cmd_info[0].timer);
		// Fall through to STATE_NO_DEV logic
	case STATE_NO_DEV:
		ps->state = STATE_WAIT_DEV;
		ps->left = ahci_device_checks;
		port_dev_check(ps);
		break;
	case STATE_WAIT_DEV:
		// Nothing else to do.
		break;
	default:
		// This state should not be reached. Log and indicate failure.
		dprintf(V_ERR, ("%s: %s: Unexpected port state %d during device attached\n",
						ahci_portname(ps), __func__, ps->state));
		return false; // Indicate critical error
	}
	return true;
}

static bool port_handle_device_detached(struct port_state *ps)
{
	port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_N);
	dprintf(V_DEV, ("%s: device detached\n", ahci_portname(ps)));

	switch (ps->state) {
	case STATE_WAIT_ID:
	case STATE_GOOD_DEV:
		port_stop(ps);
		// fall-through
	case STATE_BAD_DEV:
		port_disconnect(ps);
		port_hardreset(ps);
		break;
	default:
		// This state should not be reached. Log and indicate failure.
		dprintf(V_ERR, ("%s: %s: Unexpected port state %d during device detached\n",
						ahci_portname(ps), __func__, ps->state));
		return false; // Indicate critical error
	}
	return true;
}

static void port_handle_other_interrupts(struct port_state *ps, u32_t smask)
{
	u32_t tfd_status = port_read(ps, AHCI_PORT_TFD);
	int success = !(tfd_status & (AHCI_PORT_TFD_STS_ERR | AHCI_PORT_TFD_STS_DF));

	// Check for fatal failures or TFD errors/DF.
	// In both cases, restart the port and fail all commands.
	if ((tfd_status & (AHCI_PORT_TFD_STS_ERR | AHCI_PORT_TFD_STS_DF)) ||
	    (smask & AHCI_PORT_IS_RESTART)) {
		port_restart(ps);
	}

	// If we were waiting for ID verification, check now.
	if (ps->state == STATE_WAIT_ID) {
		port_id_check(ps, success);
	}
}

static void port_intr(struct port_state *ps)
{
	u32_t smask, emask;

	if (ps->state == STATE_NO_PORT) {
		dprintf(V_ERR, ("%s: interrupt for invalid port!\n",
			ahci_portname(ps)));
		return;
	}

	smask = port_read(ps, AHCI_PORT_IS);
	emask = smask & port_read(ps, AHCI_PORT_IE);

	// Clear the interrupt flags that we saw were set.
	port_write(ps, AHCI_PORT_IS, smask);

	dprintf(V_REQ, ("%s: interrupt (%08x)\n", ahci_portname(ps), smask));

	// Check if any commands have completed.
	port_check_cmds(ps);

	if (emask & AHCI_PORT_IS_PCS) {
		if (!port_handle_device_attached(ps)) {
			// A critical error occurred during handling. Stop further processing for this interrupt.
			return;
		}
	} else if (emask & AHCI_PORT_IS_PRCS) {
		if (!port_handle_device_detached(ps)) {
			// A critical error occurred during handling. Stop further processing for this interrupt.
			return;
		}
	} else if (smask & AHCI_PORT_IS_MASK) {
		port_handle_other_interrupts(ps, smask);
	}
}

/*===========================================================================*
 *				port_timeout				     *
 *===========================================================================*/
static void port_timeout(int arg)
{
	struct port_state *ps;
	int port, cmd;

	port = GET_PORT(arg);
	cmd = GET_TAG(arg);

	assert(port >= 0 && port < hba_state.nr_ports);

	ps = &port_state[port];

	if (ps->flags & FLAG_SUSPENDED) {
		assert(cmd == 0);
		blockdriver_mt_wakeup(ps->cmd_info[0].tid);
	}

	if (ps->state == STATE_SPIN_UP) {
		if (port_read(ps, AHCI_PORT_IS) & AHCI_PORT_IS_PCS) {
			dprintf(V_INFO, ("%s: bad controller, no interrupt\n",
				ahci_portname(ps)));

			ps->state = STATE_WAIT_DEV;
			ps->left = ahci_device_checks;

			port_dev_check(ps);
		} else {
			dprintf(V_INFO, ("%s: spin-up timeout\n",
				ahci_portname(ps)));

			ps->state = STATE_NO_DEV;
			ps->flags &= ~FLAG_BUSY;
		}
		return;
	}

	if (ps->state == STATE_WAIT_DEV) {
		port_dev_check(ps);
		return;
	}

	dprintf(V_ERR, ("%s: timeout\n", ahci_portname(ps)));

	port_restart(ps);

	if (ps->state == STATE_WAIT_ID) {
		port_id_check(ps, FALSE);
	}
}

/*===========================================================================*
 *				port_wait				     *
 *===========================================================================*/
static void port_wait(struct port_state *ps)
{
	volatile unsigned int *flags_ptr = &ps->flags;

	*flags_ptr |= FLAG_SUSPENDED;

	while (*flags_ptr & FLAG_BUSY) {
		blockdriver_mt_sleep();
	}

	*flags_ptr &= ~FLAG_SUSPENDED;
}

/*===========================================================================*
 *				port_issue				     *
 *===========================================================================*/
static void port_issue(struct port_state *ps, int cmd, clock_t timeout)
{
	if (ps == ((void *)0)) {
		return;
	}

	if (cmd < 0 || cmd >= AHCI_MAX_COMMAND_SLOTS) {
		return;
	}

	unsigned int cmd_bit = 1U << cmd;

	if (ps->flags & FLAG_HAS_NCQ) {
		port_write(ps, AHCI_PORT_SACT, cmd_bit);
	}

	__insn_barrier();

	port_write(ps, AHCI_PORT_CI, cmd_bit);

	ps->pend_mask |= cmd_bit;

	set_timer(&ps->cmd_info[cmd].timer, timeout, port_timeout,
		BUILD_ARG(ps - port_state, cmd));
}

/*===========================================================================*
 *				port_exec				     *
 *===========================================================================*/
static int port_exec(struct port_state *ps, int cmd, clock_t timeout)
{
	port_issue(ps, cmd, timeout);

	ps->cmd_info[cmd].tid = blockdriver_mt_get_tid();

	blockdriver_mt_sleep();

	cancel_timer(&ps->cmd_info[cmd].timer);

	assert(!(ps->flags & FLAG_BUSY));

	dprintf(V_REQ, ("%s: end of command -- %s\n", ahci_portname(ps),
		(ps->cmd_info[cmd].result == RESULT_FAILURE) ?
		"failure" : "success"));

	if (ps->cmd_info[cmd].result == RESULT_FAILURE)
		return EIO;

	return OK;
}

/*===========================================================================*
 *				port_alloc				     *
 *===========================================================================*/
#ifndef ALIGN_UP
#define ALIGN_UP(value, alignment) (((value) + (alignment) - 1) & ~((alignment) - 1))
#endif

static void port_alloc(struct port_state *ps)
{
    size_t current_offset = 0;
    size_t fis_off, tmp_off;
    size_t ct_offsets[NR_CMDS];
    int i;
    u32_t cmd_reg_val;

    // Command List (CL) is assumed to start at offset 0.
    current_offset += AHCI_CL_SIZE;

    // FIS (Frame Information Structure) block
    fis_off = ALIGN_UP(current_offset, AHCI_FIS_SIZE);
    current_offset = fis_off + AHCI_FIS_SIZE;

    // Temporary Buffer block
    tmp_off = ALIGN_UP(current_offset, AHCI_TMP_ALIGN);
    current_offset = tmp_off + AHCI_TMP_SIZE;

    // Command Tables (CT) blocks
    for (i = 0; i < NR_CMDS; i++) {
        ct_offsets[i] = ALIGN_UP(current_offset, AHCI_CT_ALIGN);
        current_offset = ct_offsets[i] + AHCI_CT_SIZE;
    }

    // The total memory size required is the final current_offset
    ps->mem_size = current_offset;

    ps->mem_base = alloc_contig(ps->mem_size, AC_ALIGN4K, &ps->mem_phys);
    if (ps->mem_base == NULL) {
        panic("port_alloc: Failed to allocate contiguous memory for port state.");
    }
    memset(ps->mem_base, 0, ps->mem_size);

    // Assign pointers within the allocated block and assert alignment
    ps->cl_base = (u32_t *)ps->mem_base;
    ps->cl_phys = ps->mem_phys;
    assert(ps->cl_phys % AHCI_CL_SIZE == 0);

    ps->fis_base = (u32_t *)(ps->mem_base + fis_off);
    ps->fis_phys = ps->mem_phys + fis_off;
    assert(ps->fis_phys % AHCI_FIS_SIZE == 0);

    ps->tmp_base = (u8_t *)(ps->mem_base + tmp_off);
    ps->tmp_phys = ps->mem_phys + tmp_off;
    assert(ps->tmp_phys % AHCI_TMP_ALIGN == 0);

    for (i = 0; i < NR_CMDS; i++) {
        ps->ct_base[i] = ps->mem_base + ct_offsets[i];
        ps->ct_phys[i] = ps->mem_phys + ct_offsets[i];
        assert(ps->ct_phys[i] % AHCI_CT_ALIGN == 0);
    }

    // Tell the controller about the physical addresses.
    port_write(ps, AHCI_PORT_FBU, 0);
    port_write(ps, AHCI_PORT_FB, ps->fis_phys);

    port_write(ps, AHCI_PORT_CLBU, 0);
    port_write(ps, AHCI_PORT_CLB, ps->cl_phys);

    // Enable FIS receive.
    cmd_reg_val = port_read(ps, AHCI_PORT_CMD);
    port_write(ps, AHCI_PORT_CMD, cmd_reg_val | AHCI_PORT_CMD_FRE);

    ps->pad_base = NULL;
    ps->pad_size = 0;
}

/*===========================================================================*
 *				port_free				     *
 *===========================================================================*/
static void port_free(struct port_state *ps)
{
    u32_t cmd;
    unsigned int attempts = 0;
    const unsigned int MAX_AHCI_SPIN_ATTEMPTS = 1000;
    volatile unsigned int i;

    cmd = port_read(ps, AHCI_PORT_CMD);

    if (cmd & (AHCI_PORT_CMD_FR | AHCI_PORT_CMD_FRE)) {
        port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_FRE);

        while ((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_FR) &&
               attempts < MAX_AHCI_SPIN_ATTEMPTS) {
            for (i = 0; i < PORTREG_DELAY; i++);
            attempts++;
        }
    }

    if (ps->pad_base != NULL) {
        free_contig(ps->pad_base, ps->pad_size);
    }

    free_contig(ps->mem_base, ps->mem_size);
}

/*===========================================================================*
 *				port_init				     *
 *===========================================================================*/
static void port_init(struct port_state *ps)
{
	/* Initialize the port state structure. */
	ps->queue_depth = 1;
	ps->state = STATE_SPIN_UP;
	ps->flags = FLAG_BUSY;
	ps->sector_size = 0;
	ps->open_count = 0;
	ps->pend_mask = 0;

	int i;
	for (i = 0; i < NR_CMDS; i++) {
		init_timer(&ps->cmd_info[i].timer);
	}

	ps->reg = (u32_t *) ((u8_t *) hba_state.base +
		AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * (ps - port_state));

	/* Allocate memory for the port. */
	/* If port_alloc fails, set an error state and abort initialization
	 * to prevent further operations on an unallocated or improperly allocated port.
	 * (Assumes port_alloc returns 0 on success, non-zero on failure) */
	if (port_alloc(ps) != 0) {
		ps->state = STATE_FAILED; /* Indicate allocation failure */
		return;
	}

	/* Just listen for device connection events for now. */
	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);

	/* Enable device spin-up for HBAs that support staggered spin-up.
	 * This is a no-op for HBAs that do not support it.
	 */
	u32_t cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_SUD);

	/* Trigger a port reset. */
	port_hardreset(ps);

	set_timer(&ps->cmd_info[0].timer, ahci_spinup_timeout,
		port_timeout, BUILD_ARG(ps - port_state, 0));
}

/*===========================================================================*
 *				ahci_probe				     *
 *===========================================================================*/
static int ahci_probe(int skip)
{
	if (skip < 0) {
		return -1;
	}

	int result_code;
	int device_index;
	u16_t vendor_id, device_id;

	pci_init();

	result_code = pci_first_dev(&device_index, &vendor_id, &device_id);
	if (result_code <= 0) {
		return -1;
	}

	while (skip--) {
		result_code = pci_next_dev(&device_index, &vendor_id, &device_id);
		if (result_code <= 0) {
			return -1;
		}
	}

	pci_reserve(device_index);

	return device_index;
}

/*===========================================================================*
 *				ahci_reset				     *
 *===========================================================================*/
static void ahci_reset(void)
{
	u32_t ghc_base;
	u32_t ghc_current;

	ghc_base = hba_read(AHCI_HBA_GHC);

	hba_write(AHCI_HBA_GHC, ghc_base | AHCI_HBA_GHC_AE);

	hba_write(AHCI_HBA_GHC, ghc_base | AHCI_HBA_GHC_AE | AHCI_HBA_GHC_HR);

	SPIN_UNTIL(!(hba_read(AHCI_HBA_GHC) & AHCI_HBA_GHC_HR), RESET_DELAY);

	ghc_current = hba_read(AHCI_HBA_GHC);

	if (ghc_current & AHCI_HBA_GHC_HR) {
		panic("unable to reset HBA");
	}
}

/*===========================================================================*
 *				ahci_init				     *
 *===========================================================================*/
static void ahci_init(int devind)
{
	u32_t base = 0;
	u32_t size = 0;
	u32_t cap, ghc, mask;
	int r, ioflag;
	u32_t version_reg;
	int port;

	hba_state.base = MAP_FAILED;
	hba_state.hook_id = 0;

	if ((r = pci_get_bar(devind, PCI_BAR_6, &base, &size, &ioflag)) != OK) {
		goto fail_bar_retrieve;
	}

	if (ioflag) {
		goto fail_bar_type;
	}

	if (size < AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE) {
		goto fail_mem_size_check;
	}

	size = MIN(size, AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * NR_PORTS);
	hba_state.nr_ports = (size - AHCI_MEM_BASE_SIZE) / AHCI_MEM_PORT_SIZE;

	hba_state.base = (u32_t *) vm_map_phys(SELF, (void *) base, size);
	hba_state.size = size;
	if (hba_state.base == MAP_FAILED) {
		goto fail_hba_mem_map;
	}

	hba_state.irq = pci_attr_r8(devind, PCI_ILR);
	if ((r = sys_irqsetpolicy(hba_state.irq, 0, &hba_state.hook_id)) != OK) {
		goto fail_irq_policy;
	}

	if ((r = sys_irqenable(&hba_state.hook_id)) != OK) {
		goto fail_irq_enable;
	}

	ahci_reset();

	ghc = hba_read(AHCI_HBA_GHC);
	hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE | AHCI_HBA_GHC_IE);

	cap = hba_read(AHCI_HBA_CAP);
	hba_state.has_ncq = !!(cap & AHCI_HBA_CAP_SNCQ);
	hba_state.has_clo = !!(cap & AHCI_HBA_CAP_SCLO);
	hba_state.nr_cmds = MIN(NR_CMDS,
		((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1);

	version_reg = hba_read(AHCI_HBA_VS);

	dprintf(V_INFO, ("AHCI%u: HBA v%d.%d%d, %ld ports, %ld commands, "
		"%s queuing, IRQ %d\n",
		ahci_instance,
		(int) (version_reg >> 16),
		(int) ((version_reg >> 8) & 0xFF),
		(int) (version_reg & 0xFF),
		((cap >> AHCI_HBA_CAP_NP_SHIFT) & AHCI_HBA_CAP_NP_MASK) + 1,
		(long) hba_state.nr_cmds,
		hba_state.has_ncq ? "supports" : "no", hba_state.irq));

	dprintf(V_INFO, ("AHCI%u: CAP %08x, CAP2 %08x, PI %08x\n",
		ahci_instance, cap, hba_read(AHCI_HBA_CAP2),
		hba_read(AHCI_HBA_PI)));

	mask = hba_read(AHCI_HBA_PI);

	for (port = 0; port < hba_state.nr_ports; port++) {
		port_state[port].device = NO_DEVICE;
		port_state[port].state = STATE_NO_PORT;

		if (mask & (1 << port))
			port_init(&port_state[port]);
	}
	return;

fail_irq_enable:
	sys_irqrmpolicy(&hba_state.hook_id);
	/* FALLTHROUGH */
fail_irq_policy:
	vm_unmap_phys(SELF, (void *)hba_state.base, hba_state.size);
	/* FALLTHROUGH */
fail_hba_mem_map:
	/* No resources to free for map failure or earlier. */
	/* Specific panics based on failure point. */
	if (hba_state.base == MAP_FAILED && hba_state.hook_id == 0) { /* If reached from fail_hba_mem_map */
		panic("unable to map HBA memory");
	} else if (hba_state.hook_id == 0) { /* If reached from fail_irq_policy */
		panic("unable to register IRQ: %d", r);
	} else { /* If reached from fail_irq_enable */
		panic("unable to enable IRQ: %d", r);
	}

fail_mem_size_check:
	panic("HBA memory size too small: %u", size);

fail_bar_type:
	panic("invalid BAR type");

fail_bar_retrieve:
	panic("unable to retrieve BAR: %d", r);
}

/*===========================================================================*
 *				ahci_stop				     *
 *===========================================================================*/
static void cleanup_all_ports(void)
{
	for (int port = 0; port < hba_state.nr_ports; port++) {
		struct port_state *ps = &port_state[port];

		if (ps->state != STATE_NO_PORT) {
			port_stop(ps);
			port_free(ps);
		}
	}
}

static void ahci_stop(void)
{
	cleanup_all_ports();

	ahci_reset();

	int r_unmap = vm_unmap_phys(SELF, (void *) hba_state.base, hba_state.size);
	if (r_unmap != OK) {
		panic("unable to unmap HBA memory: %d", r_unmap);
	}

	int r_irq = sys_irqrmpolicy(&hba_state.hook_id);
	if (r_irq != OK) {
		panic("unable to deregister IRQ: %d", r_irq);
	}
}

/*===========================================================================*
 *				ahci_alarm				     *
 *===========================================================================*/
static void ahci_alarm(clock_t stamp)
{
	expire_timers(stamp);
}

/*===========================================================================*
 *				ahci_intr				     *
 *===========================================================================*/
static void ahci_intr(void)
{
	struct port_state *ps;
	u32_t hba_interrupt_status;
	int r;
	unsigned int port;

	hba_interrupt_status = hba_read(AHCI_HBA_IS);

	for (port = 0; port < hba_state.nr_ports; port++) {
		if (hba_interrupt_status & (1U << port)) {
			ps = &port_state[port];

			port_intr(ps);

			if ((ps->flags & (FLAG_SUSPENDED | FLAG_BUSY)) == FLAG_SUSPENDED) {
				blockdriver_mt_wakeup(ps->cmd_info[0].tid);
			}
		}
	}

	hba_write(AHCI_HBA_IS, hba_interrupt_status);

	if ((r = sys_irqenable(&hba_state.hook_id)) != OK) {
		panic("ahci_intr: Failed to re-enable IRQ: %d", r);
	}
}

/*===========================================================================*
 *				ahci_get_params				     *
 *===========================================================================*/
static void ahci_get_params(void)
{
	long parsed_value;
	int ret;

	parsed_value = 0;
	ret = env_parse("instance", "d", 0, &parsed_value, 0, 255);
	if (ret == 0) {
		ahci_instance = (int) parsed_value;
	} else {
		ahci_instance = 0;
	}

	parsed_value = V_ERR;
	ret = env_parse("ahci_verbose", "d", 0, &parsed_value, V_NONE, V_REQ);
	if (ret == 0) {
		ahci_verbose = (int) parsed_value;
	} else {
		ahci_verbose = V_ERR;
	}

	static const size_t AHCI_TIME_VAR_COUNT = sizeof(ahci_timevar) / sizeof(ahci_timevar[0]);

	for (size_t i = 0; i < AHCI_TIME_VAR_COUNT; i++) {
		parsed_value = ahci_timevar[i].default_ms;

		ret = env_parse(ahci_timevar[i].name, "d", 0, &parsed_value, 1, LONG_MAX);
		if (ret == 0) {
			*ahci_timevar[i].ptr = millis_to_hz(parsed_value);
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
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

static void ahci_set_mapping(void)
{
    unsigned int i, j;

    for (i = 0, j = 0; i < NR_PORTS && j < MAX_DRIVES; i++) {
        if (port_state[i].state != STATE_NO_PORT) {
            ahci_map[j++] = i;
        }
    }

    for (; j < MAX_DRIVES; j++) {
        ahci_map[j] = NO_PORT;
    }

    char key[16];
    char val[64];

    int key_len = snprintf(key, sizeof(key), "ahci%d_map", ahci_instance);
    if (key_len < 0 || (size_t)key_len >= sizeof(key)) {
        // If key construction fails or truncates, env_get_param will likely
        // not find the key, and the default mapping will be used.
        // No further error handling is done to preserve original functionality.
    }

    if (env_get_param(key, val, sizeof(val)) == OK) {
        char *p = val;
        unsigned int port;

        for (i = 0; i < MAX_DRIVES; i++) {
            if (!*p) {
                ahci_map[i] = NO_PORT;
                continue;
            }

            port = (unsigned int)strtoul(p, &p, 0);

            ahci_map[i] = port % NR_PORTS;

            if (*p != '\0') {
                p++;
            }
        }
    }

    for (i = 0; i < MAX_DRIVES; i++) {
        if (ahci_map[i] != NO_PORT) {
            j = ahci_map[i];
            if (j < NR_PORTS) { // Defensive check, though '%' ensures this.
                port_state[j].device = i;
            }
        }
    }
}

/*===========================================================================*
 *				sef_cb_init_fresh			     *
 *===========================================================================*/
static int sef_cb_init_fresh(int type, sef_init_info_t *UNUSED(info))
{
	int devind;

	ahci_get_params();

	devind = ahci_probe(ahci_instance);

	if (devind < 0) {
		return ERROR;
	}

	ahci_init(devind);

	ahci_set_mapping();

	blockdriver_announce(type);

	return OK;
}

/*===========================================================================*
 *				sef_cb_signal_handler			     *
 *===========================================================================*/
static void sef_cb_signal_handler(int signo)
{
	if (signo != SIGTERM) {
		return;
	}

	ahci_exiting = TRUE;

	int any_open_ports = 0;
	for (int port_idx = 0; port_idx < hba_state.nr_ports; port_idx++) {
		if (port_state[port_idx].open_count > 0) {
			any_open_ports = 1;
			break;
		}
	}

	if (any_open_ports) {
		return;
	} else {
		ahci_stop();
		exit(0);
	}
}

/*===========================================================================*
 *				sef_local_startup			     *
 *===========================================================================*/
static int sef_local_startup(void)
{
	int ret;

	ret = sef_setcb_init_fresh(sef_cb_init_fresh);
	if (ret != 0) {
		return ret;
	}

	ret = sef_setcb_signal_handler(sef_cb_signal_handler);
	if (ret != 0) {
		return ret;
	}

	ret = blockdriver_mt_support_lu();
	if (ret != 0) {
		return ret;
	}

	ret = sef_startup();
	if (ret != 0) {
		return ret;
	}

	return 0;
}

/*===========================================================================*
 *				ahci_portname				     *
 *===========================================================================*/
#include <stdio.h>
#include <stdlib.h>

static char *ahci_portname(struct port_state *ps)
{
    const size_t buffer_size = 10;
    char *name = (char *)malloc(buffer_size);

    if (name == NULL) {
        return NULL;
    }

    if (ps->device == NO_DEVICE) {
        int port_idx = ps - port_state;
        snprintf(name, buffer_size, "AHCI%d-P%02d", ahci_instance, port_idx);
    } else {
        snprintf(name, buffer_size, "AHCI%d-D%d", ahci_instance, ps->device);
    }

    return name;
}

/*===========================================================================*
 *				ahci_map_minor				     *
 *===========================================================================*/
static struct port_state *ahci_map_minor(devminor_t minor, struct device **dvp)
{
	struct port_state *ps = NULL;
	int port;

	if (minor >= 0 && minor < NR_MINORS) {
		port = ahci_map[minor / DEV_PER_DRIVE];

		if (port == NO_PORT) {
			return NULL;
		}

		ps = &port_state[port];
		*dvp = &ps->part[minor % DEV_PER_DRIVE];
	}
	else {
		devminor_t adjusted_minor = minor - MINOR_d0p0s0;

		if ((unsigned)adjusted_minor < NR_SUBDEVS) {
			port = ahci_map[adjusted_minor / SUB_PER_DRIVE];

			if (port == NO_PORT) {
				return NULL;
			}

			ps = &port_state[port];
			*dvp = &ps->subpart[adjusted_minor % SUB_PER_DRIVE];
		}
	}

	return ps;
}

/*===========================================================================*
 *				ahci_part				     *
 *===========================================================================*/
static struct device *ahci_part(devminor_t minor)
{
	struct device *dv;
	return (ahci_map_minor(minor, &dv) == NULL) ? NULL : dv;
}

/*===========================================================================*
 *				ahci_open				     *
 *===========================================================================*/
static int ahci_initialize_port_on_first_open(struct port_state *ps)
{
    int r;

    ps->flags &= ~FLAG_BARRIER;

    if ((ps->flags & FLAG_ATAPI) && (r = atapi_check_medium(ps, 0)) != OK) {
        return r;
    }

    memset(ps->part, 0, sizeof(ps->part));
    memset(ps->subpart, 0, sizeof(ps->subpart));
    ps->part[0].dv_size = (long long)ps->lba_count * ps->sector_size;

    partition(&ahci_dtab, ps->device * DEV_PER_DRIVE, P_PRIMARY,
              !!(ps->flags & FLAG_ATAPI));

    blockdriver_mt_set_workers(ps->device, ps->queue_depth);

    return OK;
}

static int ahci_open(devminor_t minor, int access)
{
    struct port_state *ps = ahci_get_port(minor);
    int r;

    ps->cmd_info[0].tid = blockdriver_mt_get_tid();

    if (ps->flags & FLAG_BUSY) {
        port_wait(ps);
    }

    if (ps->state != STATE_GOOD_DEV) {
        return ENXIO;
    }

    if ((ps->flags & FLAG_READONLY) && (access & BDEV_W_BIT)) {
        return EACCES;
    }

    if (ps->open_count == 0) {
        r = ahci_initialize_port_on_first_open(ps);
        if (r != OK) {
            return r;
        }
    } else {
        if (ps->flags & FLAG_BARRIER) {
            return ENXIO;
        }
    }

    ps->open_count++;

    return OK;
}

/*===========================================================================*
 *				ahci_close				     *
 *===========================================================================*/
static int ahci_close(devminor_t minor)
{
    struct port_state *ps;

    ps = ahci_get_port(minor);

    if (ps->open_count <= 0) {
        dprintf(V_ERR, ("%s: closing already-closed or invalidly-counted port (count: %d)\n",
            ahci_portname(ps), ps->open_count));
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
        int all_ports_closed = 1;
        int port;

        for (port = 0; port < hba_state.nr_ports; ++port) {
            if (port_state[port].open_count > 0) {
                all_ports_closed = 0;
                break;
            }
        }

        if (all_ports_closed) {
            ahci_stop();
            blockdriver_mt_terminate();
        }
    }

    return OK;
}

/*===========================================================================*
 *				ahci_transfer				     *
 *===========================================================================*/
static ssize_t ahci_transfer(devminor_t minor, int do_write, u64_t position,
	endpoint_t endpt, iovec_t *iovec, unsigned int count, int flags)
{
	struct port_state *ps;
	struct device *dv;
	u64_t pos, eof;

	ps = ahci_get_port(minor);
	if (ps == NULL) {
		return ENODEV;
	}

	dv = ahci_part(minor);
	if (dv == NULL) {
		return ENODEV;
	}

	if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER)) {
		return EIO;
	}

	if (count > NR_IOREQS) {
		return EINVAL;
	}

	if (position >= dv->dv_size) {
		return OK;
	}

	pos = dv->dv_base + position;
	eof = dv->dv_base + dv->dv_size;

	return port_transfer(ps, pos, eof, endpt, (iovec_s_t *) iovec, count,
		do_write, flags);
}

/*===========================================================================*
 *				ahci_ioctl				     *
 *===========================================================================*/
static int ahci_ioctl(devminor_t minor, unsigned long request,
	endpoint_t endpt, cp_grant_id_t grant, endpoint_t UNUSED(user_endpt))
{
	struct port_state *ps;
	int r;

	ps = ahci_get_port(minor);

	switch (request) {
	case DIOCEJECT: {
		if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER)) {
			return EIO;
		}
		if (!(ps->flags & FLAG_ATAPI)) {
			return EINVAL;
		}
		return atapi_load_eject(ps, 0, FALSE);
	}

	case DIOCOPENCT: {
		return sys_safecopyto(endpt, grant, 0,
			(vir_bytes) &ps->open_count, sizeof(ps->open_count));
	}

	case DIOCFLUSH: {
		if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER)) {
			return EIO;
		}
		return gen_flush_wcache(ps);
	}

	case DIOCSETWC: {
		int val;
		if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER)) {
			return EIO;
		}
		r = sys_safecopyfrom(endpt, grant, 0, (vir_bytes) &val,
			sizeof(val));
		if (r != OK) {
			return r;
		}
		return gen_set_wcache(ps, val);
	}

	case DIOCGETWC: {
		int val;
		if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER)) {
			return EIO;
		}
		r = gen_get_wcache(ps, &val);
		if (r != OK) {
			return r;
		}
		return sys_safecopyto(endpt, grant, 0, (vir_bytes) &val,
			sizeof(val));
	}

	default:
		return ENOTTY;
	}
}

/*===========================================================================*
 *				ahci_device				     *
 *===========================================================================*/
static int ahci_device(devminor_t minor, device_id_t *id)
{
	struct port_state *ps;

	if (id == NULL)
		return EINVAL;

	if ((ps = ahci_map_minor(minor, NULL)) == NULL)
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
	struct device *dummy_dv;

	if ((ps = ahci_map_minor(minor, &dummy_dv)) == NULL)
		panic("device mapping for minor %d disappeared", minor);

	return ps;
}

/*===========================================================================*
 *				main					     *
 *===========================================================================*/
int main(int argc, char **argv)
{
	env_setargs(argc, argv);

	if (sef_local_startup() != 0) {
		return 1;
	}

	return blockdriver_mt_task(&ahci_dtab);
}
