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
    prd_t prd;
    int nr_prds = 0;

    if (ps == NULL || packet == NULL || size > AHCI_TMP_SIZE)
        return -1;

    memset(&fis, 0, sizeof(fis));
    fis.cf_cmd = ATA_CMD_PACKET;

    if (size > 0) {
        fis.cf_feat = ATA_FEAT_PACKET_DMA;
        if (!write && (ps->flags & FLAG_USE_DMADIR))
            fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;

        prd.vp_addr = ps->tmp_phys;
        prd.vp_size = size;
        nr_prds = 1;
    }

    port_set_cmd(ps, cmd, &fis, packet, (nr_prds ? &prd : NULL), nr_prds, write);

    return port_exec(ps, cmd, ahci_command_timeout);
}

/*===========================================================================*
 *				atapi_test_unit				     *
 *===========================================================================*/
static int atapi_test_unit(struct port_state *ps, int cmd)
{
	u8_t packet[ATAPI_PACKET_SIZE] = {0};
	packet[0] = ATAPI_CMD_TEST_UNIT;

	return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_request_sense			     *
 *===========================================================================*/
static int atapi_request_sense(struct port_state *ps, int cmd, int *sense)
{
    u8_t packet[ATAPI_PACKET_SIZE];

    if (!ps || !sense)
        return ERR_INVALID_ARG;

    memset(packet, 0, sizeof(packet));
    packet[0] = ATAPI_CMD_REQUEST_SENSE;
    packet[4] = ATAPI_REQUEST_SENSE_LEN;

    int r = atapi_exec(ps, cmd, packet, ATAPI_REQUEST_SENSE_LEN, FALSE);
    if (r != OK)
        return r;

    if (!ps->tmp_base) 
        return ERR_IO;

    *sense = ps->tmp_base[2] & 0xF;
    dprintf(V_REQ, "%s: ATAPI SENSE: sense %x ASC %x ASCQ %x\n",
        ahci_portname(ps), *sense, ps->tmp_base[12], ps->tmp_base[13]);

    return OK;
}

/*===========================================================================*
 *				atapi_load_eject			     *
 *===========================================================================*/
static int atapi_load_eject(struct port_state *ps, int cmd, int load)
{
	u8_t packet[ATAPI_PACKET_SIZE] = {0};
	packet[0] = ATAPI_CMD_START_STOP;
	packet[4] = load ? ATAPI_START_STOP_LOAD : ATAPI_START_STOP_EJECT;

	if (ps == NULL) {
		return -1;
	}

	return atapi_exec(ps, cmd, packet, 0, 0);
}

/*===========================================================================*
 *				atapi_read_capacity			     *
 *===========================================================================*/
static int atapi_read_capacity(struct port_state *ps, int cmd)
{
	u8_t packet[ATAPI_PACKET_SIZE];
	u8_t *buf;
	int r;

	memset(packet, 0, sizeof(packet));
	packet[0] = ATAPI_CMD_READ_CAPACITY;

	r = atapi_exec(ps, cmd, packet, ATAPI_READ_CAPACITY_LEN, FALSE);
	if (r != OK)
		return r;

	buf = ps->tmp_base;
	ps->lba_count = ((u64_t)buf[0] << 24 | (u64_t)buf[1] << 16 | (u64_t)buf[2] << 8 | (u64_t)buf[3]) + 1;
	ps->sector_size = ((unsigned int)buf[4] << 24) | ((unsigned int)buf[5] << 16) | ((unsigned int)buf[6] << 8) | (unsigned int)buf[7];

	if (ps->sector_size == 0 || (ps->sector_size & 1)) {
		dprintf(V_ERR, "%s: invalid medium sector size %u\n", ahci_portname(ps), ps->sector_size);
		return EINVAL;
	}

	dprintf(V_INFO, "%s: medium detected (%u byte sectors, %llu MB size)\n",
	        ahci_portname(ps), ps->sector_size,
	        (unsigned long long)(ps->lba_count * ps->sector_size / (1024*1024)));

	return OK;
}

/*===========================================================================*
 *				atapi_check_medium			     *
 *===========================================================================*/
static int atapi_check_medium(struct port_state *ps, int cmd)
{
	int sense;
	int test_result = atapi_test_unit(ps, cmd);

	if (test_result != OK) {
		ps->flags &= ~FLAG_HAS_MEDIUM;

		if (atapi_request_sense(ps, cmd, &sense) != OK)
			return ENXIO;
		if (sense != ATAPI_SENSE_UNIT_ATT)
			return ENXIO;
	}

	if (!(ps->flags & FLAG_HAS_MEDIUM)) {
		if (atapi_read_capacity(ps, cmd) != OK)
			return EIO;
		ps->flags |= FLAG_HAS_MEDIUM;
	}

	return OK;
}

/*===========================================================================*
 *				atapi_id_check				     *
 *===========================================================================*/
static int atapi_id_check(struct port_state *ps, u16_t *buf)
{
	u16_t gcap = buf[ATA_ID_GCAP];
	u16_t cap = buf[ATA_ID_CAP];
	u16_t dmadir = buf[ATA_ID_DMADIR];
	u16_t sup0 = buf[ATA_ID_SUP0];
	u16_t sup1 = buf[ATA_ID_SUP1];

	int is_atapi = (gcap & ATA_ID_GCAP_ATAPI_MASK) == ATA_ID_GCAP_ATAPI;
	int is_removable = (gcap & ATA_ID_GCAP_REMOVABLE) != 0;
	int gcap_incomplete = (gcap & ATA_ID_GCAP_INCOMPLETE) != 0;
	int supports_dma = (cap & ATA_ID_CAP_DMA) == ATA_ID_CAP_DMA;
	int supports_dmadir = (dmadir & (ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA)) == (ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA);

	if (!(is_atapi && is_removable) || gcap_incomplete || (!supports_dma && !supports_dmadir)) {
		dprintf(V_ERR, "%s: unsupported ATAPI device\n", ahci_portname(ps));
		dprintf(V_DEV, "%s: GCAP %04x CAP %04x DMADIR %04x\n", ahci_portname(ps), gcap, cap, dmadir);
		return FALSE;
	}

	if (dmadir & ATA_ID_DMADIR_DMADIR)
		ps->flags |= FLAG_USE_DMADIR;

	if (((gcap & ATA_ID_GCAP_TYPE_MASK) >> ATA_ID_GCAP_TYPE_SHIFT) == ATAPI_TYPE_CDROM)
		ps->flags |= FLAG_READONLY;

	if ((sup1 & ATA_ID_SUP1_VALID_MASK) == ATA_ID_SUP1_VALID && !(ps->flags & FLAG_READONLY)) {
		if (sup0 & ATA_ID_SUP0_WCACHE)
			ps->flags |= FLAG_HAS_WCACHE;
		if (sup1 & ATA_ID_SUP1_FLUSH)
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

    if (!ps || !prdt || nr_prds <= 0)
        return -1;

    memset(&fis, 0, sizeof(fis));
    fis.cf_cmd = ATA_CMD_PACKET;
    fis.cf_feat = ATA_FEAT_PACKET_DMA;
    if (!write && (ps->flags & FLAG_USE_DMADIR))
        fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;

    memset(packet, 0, sizeof(packet));
    packet[0] = write ? ATAPI_CMD_WRITE : ATAPI_CMD_READ;
    packet[2] = (u8_t)((start_lba >> 24) & 0xFF);
    packet[3] = (u8_t)((start_lba >> 16) & 0xFF);
    packet[4] = (u8_t)((start_lba >> 8) & 0xFF);
    packet[5] = (u8_t)(start_lba & 0xFF);
    packet[6] = (u8_t)((count >> 24) & 0xFF);
    packet[7] = (u8_t)((count >> 16) & 0xFF);
    packet[8] = (u8_t)((count >> 8) & 0xFF);
    packet[9] = (u8_t)(count & 0xFF);

    if (port_set_cmd(ps, cmd, &fis, packet, prdt, nr_prds, write) != 0)
        return -1;

    return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				ata_id_check				     *
 *===========================================================================*/
static int ata_id_check(struct port_state *ps, u16_t *buf)
{
	if (((buf[ATA_ID_GCAP] & (ATA_ID_GCAP_ATA_MASK | ATA_ID_GCAP_REMOVABLE | ATA_ID_GCAP_INCOMPLETE)) != ATA_ID_GCAP_ATA) ||
	    ((buf[ATA_ID_CAP] & (ATA_ID_CAP_LBA | ATA_ID_CAP_DMA)) != (ATA_ID_CAP_LBA | ATA_ID_CAP_DMA)) ||
	    ((buf[ATA_ID_SUP1] & (ATA_ID_SUP1_VALID_MASK | ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48)) != (ATA_ID_SUP1_VALID | ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48))) {

		dprintf(V_ERR, "%s: unsupported ATA device\n", ahci_portname(ps));
		dprintf(V_DEV, "%s: GCAP %04x CAP %04x SUP1 %04x\n", ahci_portname(ps), buf[ATA_ID_GCAP], buf[ATA_ID_CAP], buf[ATA_ID_SUP1]);
		return FALSE;
	}

	ps->lba_count = ((u64_t)buf[ATA_ID_LBA3] << 48) | ((u64_t)buf[ATA_ID_LBA2] << 32) | ((u64_t)buf[ATA_ID_LBA1] << 16) | (u64_t)buf[ATA_ID_LBA0];

	ps->queue_depth = 1;
	if (hba_state.has_ncq && (buf[ATA_ID_SATA_CAP] & ATA_ID_SATA_CAP_NCQ)) {
		ps->flags |= FLAG_HAS_NCQ;
		ps->queue_depth = ((buf[ATA_ID_QDEPTH] & ATA_ID_QDEPTH_MASK) + 1);
		if (ps->queue_depth > hba_state.nr_cmds)
			ps->queue_depth = hba_state.nr_cmds;
	}

	if ((buf[ATA_ID_PLSS] & (ATA_ID_PLSS_VALID_MASK | ATA_ID_PLSS_LLS)) == (ATA_ID_PLSS_VALID | ATA_ID_PLSS_LLS))
		ps->sector_size = ((buf[ATA_ID_LSS1] << 16) | buf[ATA_ID_LSS0]) << 1;
	else
		ps->sector_size = ATA_SECTOR_SIZE;

	if (ps->sector_size < ATA_SECTOR_SIZE) {
		dprintf(V_ERR, "%s: invalid sector size %u\n", ahci_portname(ps), ps->sector_size);
		return FALSE;
	}

	ps->flags |= (FLAG_HAS_MEDIUM | FLAG_HAS_FLUSH);

	if (buf[ATA_ID_SUP0] & ATA_ID_SUP0_WCACHE)
		ps->flags |= FLAG_HAS_WCACHE;

	if ((buf[ATA_ID_ENA2] & (ATA_ID_ENA2_VALID_MASK | ATA_ID_ENA2_FUA)) == (ATA_ID_ENA2_VALID | ATA_ID_ENA2_FUA))
		ps->flags |= FLAG_HAS_FUA;

	return TRUE;
}

/*===========================================================================*
 *				ata_transfer				     *
 *===========================================================================*/
static int ata_transfer(struct port_state *ps, int cmd, u64_t start_lba,
    unsigned int count, int write, int force, prd_t *prdt, int nr_prds)
{
    cmd_fis_t fis;

    if (count > ATA_MAX_SECTORS) {
        return -1;
    }

    if (count == ATA_MAX_SECTORS)
        count = 0;

    memset(&fis, 0, sizeof(fis));
    fis.cf_dev = ATA_DEV_LBA;

    if (ps->flags & FLAG_HAS_NCQ) {
        if (write) {
            if (force && (ps->flags & FLAG_HAS_FUA))
                fis.cf_dev |= ATA_DEV_FUA;
            fis.cf_cmd = ATA_CMD_WRITE_FPDMA_QUEUED;
        } else {
            fis.cf_cmd = ATA_CMD_READ_FPDMA_QUEUED;
        }
    } else {
        if (write) {
            fis.cf_cmd = (force && (ps->flags & FLAG_HAS_FUA)) ?
                         ATA_CMD_WRITE_DMA_FUA_EXT : ATA_CMD_WRITE_DMA_EXT;
        } else {
            fis.cf_cmd = ATA_CMD_READ_DMA_EXT;
        }
    }

    fis.cf_lba = start_lba & 0x00FFFFFFUL;
    fis.cf_lba_exp = (start_lba >> 24) & 0x00FFFFFFUL;
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
static int gen_identify(struct port_state *ps, int blocking)
{
	cmd_fis_t fis = {0};
	prd_t prd;

	fis.cf_cmd = (ps->flags & FLAG_ATAPI) ? ATA_CMD_IDENTIFY_PACKET : ATA_CMD_IDENTIFY;

	prd.vp_addr = ps->tmp_phys;
	prd.vp_size = ATA_ID_SIZE;

	port_set_cmd(ps, 0, &fis, NULL, &prd, 1, FALSE);

	if (blocking)
		return port_exec(ps, 0, ahci_command_timeout);

	port_issue(ps, 0, ahci_command_timeout);

	return OK;
}

/*===========================================================================*
 *				gen_flush_wcache			     *
 *===========================================================================*/
static int gen_flush_wcache(struct port_state *ps)
{
    cmd_fis_t fis;

    if (ps == NULL)
        return EINVAL;

    if (!(ps->flags & FLAG_HAS_FLUSH))
        return EINVAL;

    memset(&fis, 0, sizeof(fis));
    fis.cf_cmd = ATA_CMD_FLUSH_CACHE;

    if (port_set_cmd(ps, 0, &fis, NULL, NULL, 0, FALSE) != 0)
        return EIO;

    return port_exec(ps, 0, ahci_flush_timeout);
}

/*===========================================================================*
 *				gen_get_wcache				     *
 *===========================================================================*/
static int gen_get_wcache(struct port_state *ps, int *val)
{
	if (!ps || !val)
		return EINVAL;

	if (!(ps->flags & FLAG_HAS_WCACHE))
		return EINVAL;

	int r = gen_identify(ps, 1);
	if (r != OK)
		return r;

	u16_t *id_data = (u16_t *)ps->tmp_base;
	*val = (id_data[ATA_ID_ENA0] & ATA_ID_ENA0_WCACHE) ? 1 : 0;

	return OK;
}

/*===========================================================================*
 *				gen_set_wcache				     *
 *===========================================================================*/
static int gen_set_wcache(struct port_state *ps, int enable)
{
	cmd_fis_t fis;
	clock_t timeout;

	if ((ps == NULL) || !(ps->flags & FLAG_HAS_WCACHE))
		return EINVAL;

	timeout = enable ? ahci_command_timeout : ahci_flush_timeout;

	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_SET_FEATURES;
	fis.cf_feat = enable ? ATA_SF_EN_WCACHE : ATA_SF_DI_WCACHE;

	if (port_set_cmd(ps, 0, &fis, NULL, NULL, 0, FALSE) != 0)
		return EIO;

	return port_exec(ps, 0, timeout);
}

/*===========================================================================*
 *				ct_set_fis				     *
 *===========================================================================*/
static vir_bytes ct_set_fis(u8_t *ct, const cmd_fis_t *fis, unsigned int tag)
{
	if (!ct || !fis)
		return 0;

	memset(ct, 0, ATA_H2D_SIZE);

	ct[ATA_FIS_TYPE]       = ATA_FIS_TYPE_H2D;
	ct[ATA_H2D_FLAGS]      = ATA_H2D_FLAGS_C;
	ct[ATA_H2D_CMD]        = fis->cf_cmd;
	ct[ATA_H2D_LBA_LOW]    = (u8_t)(fis->cf_lba);
	ct[ATA_H2D_LBA_MID]    = (u8_t)(fis->cf_lba >> 8);
	ct[ATA_H2D_LBA_HIGH]   = (u8_t)(fis->cf_lba >> 16);
	ct[ATA_H2D_DEV]        = fis->cf_dev;
	ct[ATA_H2D_LBA_LOW_EXP]  = (u8_t)(fis->cf_lba_exp);
	ct[ATA_H2D_LBA_MID_EXP]  = (u8_t)(fis->cf_lba_exp >> 8);
	ct[ATA_H2D_LBA_HIGH_EXP] = (u8_t)(fis->cf_lba_exp >> 16);
	ct[ATA_H2D_CTL]        = fis->cf_ctl;

	if (ATA_IS_FPDMA_CMD(fis->cf_cmd)) {
		ct[ATA_H2D_FEAT]     = fis->cf_sec;
		ct[ATA_H2D_FEAT_EXP] = fis->cf_sec_exp;
		ct[ATA_H2D_SEC]      = (u8_t)(tag << ATA_SEC_TAG_SHIFT);
		ct[ATA_H2D_SEC_EXP]  = 0;
	} else {
		ct[ATA_H2D_FEAT]     = fis->cf_feat;
		ct[ATA_H2D_FEAT_EXP] = fis->cf_feat_exp;
		ct[ATA_H2D_SEC]      = fis->cf_sec;
		ct[ATA_H2D_SEC_EXP]  = fis->cf_sec_exp;
	}

	return ATA_H2D_SIZE;
}

/*===========================================================================*
 *				ct_set_packet				     *
 *===========================================================================*/
static void ct_set_packet(u8_t *ct, const u8_t *packet)
{
    if (!ct || !packet) {
        return;
    }
    memcpy(&ct[AHCI_CT_PACKET_OFF], packet, ATAPI_PACKET_SIZE);
}

/*===========================================================================*
 *				ct_set_prdt				     *
 *===========================================================================*/
static void ct_set_prdt(u8_t *ct, const prd_t *prdt, int nr_prds)
{
    if (!ct || !prdt || nr_prds <= 0) {
        return;
    }

    u32_t *p = (u32_t *)&ct[AHCI_CT_PRDT_OFF];

    for (int i = 0; i < nr_prds; i++) {
        *p++ = prdt[i].vp_addr;
        *p++ = 0;
        *p++ = 0;
        *p++ = (prdt[i].vp_size > 0) ? prdt[i].vp_size - 1 : 0;
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
    vir_bytes size;
    u32_t cl0;
    int is_fpdma_cmd;

    if (!ps || !fis || cmd < 0 || cmd >= AHCI_MAX_COMMANDS || nr_prds < 0 || nr_prds > NR_PRDS)
        return;

    is_fpdma_cmd = ATA_IS_FPDMA_CMD(fis->cf_cmd);

    if (is_fpdma_cmd) {
        ps->flags |= FLAG_NCQ_MODE;
    } else {
        if (ps->pend_mask)
            return;
        ps->flags &= ~FLAG_NCQ_MODE;
    }

    ct = ps->ct_base[cmd];
    if (!ct)
        return;

    size = ct_set_fis(ct, fis, cmd);

    if (packet)
        ct_set_packet(ct, packet);

    ct_set_prdt(ct, prdt, nr_prds);

    cl = &ps->cl_base[cmd * AHCI_CL_ENTRY_DWORDS];
    if (!cl)
        return;

    memset(cl, 0, AHCI_CL_ENTRY_SIZE);

    cl0 = 0;
    cl0 |= (u32_t)(nr_prds << AHCI_CL_PRDTL_SHIFT);
    if (!is_fpdma_cmd && (nr_prds > 0 || packet)) cl0 |= AHCI_CL_PREFETCHABLE;
    if (write) cl0 |= AHCI_CL_WRITE;
    if (packet) cl0 |= AHCI_CL_ATAPI;
    cl0 |= (u32_t)((size / sizeof(u32_t)) << AHCI_CL_CFL_SHIFT);
    cl[0] = cl0;

    cl[2] = ps->ct_phys[cmd];
}

/*===========================================================================*
 *				port_finish_cmd				     *
 *===========================================================================*/
static void port_finish_cmd(struct port_state *ps, int cmd, int result)
{
	if (ps == NULL || cmd < 0 || cmd >= ps->queue_depth) {
		return;
	}

	dprintf(V_REQ, "%s: command %d %s\n", ahci_portname(ps),
	        cmd, (result == RESULT_SUCCESS) ? "succeeded" : "failed");

	ps->cmd_info[cmd].result = result;

	if (!(ps->pend_mask & (1 << cmd))) {
		return;
	}
	ps->pend_mask &= ~(1 << cmd);

	if (ps->state != STATE_WAIT_ID) {
		blockdriver_mt_wakeup(ps->cmd_info[cmd].tid);
	}
}

/*===========================================================================*
 *				port_fail_cmds				     *
 *===========================================================================*/
static void port_fail_cmds(struct port_state *ps)
{
	int i;
	unsigned int mask = ps->pend_mask;

	if (!ps || ps->queue_depth <= 0)
		return;

	for (i = 0; mask != 0 && i < ps->queue_depth; i++) {
		if (mask & 1)
			port_finish_cmd(ps, i, RESULT_FAILURE);
		mask >>= 1;
	}
}

/*===========================================================================*
 *				port_check_cmds				     *
 *===========================================================================*/
static void port_check_cmds(struct port_state *ps)
{
	u32_t mask = 0, done = 0;
	int i;

	if (ps == NULL)
		return;

	if (ps->flags & FLAG_NCQ_MODE)
		mask = port_read(ps, AHCI_PORT_SACT);
	else
		mask = port_read(ps, AHCI_PORT_CI);

	done = ps->pend_mask & ~mask;

	for (i = 0; i < ps->queue_depth; i++) {
		if ((done & (1U << i)) != 0) {
			port_finish_cmd(ps, i, RESULT_SUCCESS);
		}
	}
}

/*===========================================================================*
 *				port_find_cmd				     *
 *===========================================================================*/
static int port_find_cmd(struct port_state *ps)
{
    int i;
    unsigned int mask;

    if (ps == NULL || ps->queue_depth <= 0)
        return -1;

    for (i = 0, mask = 1; i < ps->queue_depth; i++, mask <<= 1) {
        if (!(ps->pend_mask & mask)) {
            return i;
        }
    }

    return -1;
}

/*===========================================================================*
 *				port_get_padbuf				     *
 *===========================================================================*/
static int port_get_padbuf(struct port_state *ps, size_t size)
{
	if (ps == NULL)
		return ENOMEM;

	if (ps->pad_base && ps->pad_size >= size)
		return OK;

	if (ps->pad_base) {
		free_contig(ps->pad_base, ps->pad_size);
		ps->pad_base = NULL;
		ps->pad_size = 0;
		ps->pad_phys = 0;
	}

	ps->pad_size = size;
	ps->pad_base = alloc_contig(ps->pad_size, 0, &ps->pad_phys);

	if (!ps->pad_base) {
		dprintf(V_ERR, "%s: unable to allocate a padding buffer of size %lu\n",
			ahci_portname(ps), (unsigned long)size);
		ps->pad_size = 0;
		ps->pad_phys = 0;
		return ENOMEM;
	}

	dprintf(V_INFO, "%s: allocated padding buffer of size %lu\n",
		ahci_portname(ps), (unsigned long)size);

	return OK;
}

/*===========================================================================*
 *				sum_iovec				     *
 *===========================================================================*/
static int sum_iovec(struct port_state *ps, endpoint_t endpt,
    iovec_s_t *iovec, int nr_req, vir_bytes *total)
{
    vir_bytes bytes = 0;

    if (!ps || !iovec || !total || nr_req <= 0) {
        dprintf(V_ERR, ("sum_iovec: invalid arguments\n"));
        return EINVAL;
    }

    for (int i = 0; i < nr_req; i++) {
        vir_bytes size = iovec[i].iov_size;

        if (size == 0 || (size & 1) || size > LONG_MAX) {
            dprintf(V_ERR, ("%s: bad size %lu in iovec from %d\n",
                ahci_portname(ps), size, endpt));
            return EINVAL;
        }

        if (LONG_MAX - bytes < size) {
            dprintf(V_ERR, ("%s: iovec size overflow from %d\n",
                ahci_portname(ps), endpt));
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
        r = port_get_padbuf(ps, ps->sector_size);
        if (r != OK)
            return r;

        prdt[nr_prds].vp_addr = ps->pad_phys;
        prdt[nr_prds].vp_size = lead;
        nr_prds++;
    }

    trail = (ps->sector_size - (lead + size)) % ps->sector_size;

    for (i = 0; i < nr_req && size > 0; i++) {
        bytes = MIN(iovec[i].iov_size, size);

        if (endpt == SELF)
            vvec[i].vv_addr = (vir_bytes)iovec[i].iov_grant;
        else
            vvec[i].vv_grant = iovec[i].iov_grant;

        vvec[i].vv_size = bytes;
        size -= bytes;
    }

    pcount = i;

    r = sys_vumap(endpt, vvec, i, 0, write ? VUA_READ : VUA_WRITE,
        &prdt[nr_prds], &pcount);

    if (r != OK) {
        dprintf(V_ERR, "%s: unable to map memory from %d (%d)\n",
            ahci_portname(ps), endpt, r);
        return r;
    }

    if (pcount <= 0 || pcount > i) {
        dprintf(V_ERR, "%s: unexpected mapped buffer count from %d\n",
            ahci_portname(ps), endpt);
        return EINVAL;
    }

    for (i = 0; i < pcount; i++) {
        if (vvec[i].vv_size != prdt[nr_prds].vp_size) {
            dprintf(V_ERR, "%s: non-contiguous memory from %d\n",
                ahci_portname(ps), endpt);
            return EINVAL;
        }
        if ((prdt[nr_prds].vp_addr & 1) != 0) {
            dprintf(V_ERR, "%s: bad physical address from %d\n",
                ahci_portname(ps), endpt);
            return EINVAL;
        }
        nr_prds++;
    }

    if (trail > 0) {
        if (nr_prds >= NR_PRDS)
            return ENOMEM;
        prdt[nr_prds].vp_addr = ps->pad_phys + lead;
        prdt[nr_prds].vp_size = trail;
        nr_prds++;
    }

    return nr_prds;
}

/*===========================================================================*
 *				port_transfer				     *
 *===========================================================================*/
static ssize_t port_transfer(struct port_state *ps, u64_t pos, u64_t eof,
	endpoint_t endpt, iovec_s_t *iovec, int nr_req, int write, int flags)
{
	prd_t prdt[NR_PRDS];
	vir_bytes size, lead;
	unsigned int count, nr_prds;
	u64_t start_lba;
	int r, cmd;

	if (!ps || !iovec || nr_req <= 0)
		return EINVAL;

	if ((r = sum_iovec(ps, endpt, iovec, nr_req, &size)) != OK)
		return r;

	dprintf(V_REQ, ("%s: %s for %lu bytes at pos %llx\n",
		ahci_portname(ps), write ? "write" : "read", size, pos));

	if (ps->state != STATE_GOOD_DEV || !(ps->flags & FLAG_HAS_MEDIUM) || ps->sector_size == 0)
		return EIO;

	if (size > MAX_TRANSFER)
		size = MAX_TRANSFER;

	if (pos + size > eof)
		size = (vir_bytes)(eof - pos);

	start_lba = pos / ps->sector_size;
	lead = (vir_bytes)(pos % ps->sector_size);
	count = (lead + size + ps->sector_size - 1) / ps->sector_size;

	if ((lead & 1) || (write && lead != 0))
	{
		dprintf(V_ERR, ("%s: unaligned position from %d\n",
			ahci_portname(ps), endpt));
		return EINVAL;
	}

	if (write && (size % ps->sector_size) != 0) {
		dprintf(V_ERR, ("%s: unaligned size %lu from %d\n",
			ahci_portname(ps), size, endpt));
		return EINVAL;
	}

	nr_prds = setup_prdt(ps, endpt, iovec, nr_req, size, lead, write, prdt);
	if ((int)nr_prds < 0)
		return nr_prds;

	cmd = port_find_cmd(ps);
	if (cmd < 0)
		return EIO;

	if (ps->flags & FLAG_ATAPI)
		r = atapi_transfer(ps, cmd, start_lba, count, write, prdt, nr_prds);
	else
		r = ata_transfer(ps, cmd, start_lba, count, write,
			!!(flags & BDEV_FORCEWRITE), prdt, nr_prds);

	if (r != OK)
		return r;

	return (ssize_t)size;
}

/*===========================================================================*
 *				port_hardreset				     *
 *===========================================================================*/
static void port_hardreset(struct port_state *ps)
{
    if (!ps) {
        return;
    }

    if (port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_INIT) != 0) {
        return;
    }

    micro_delay(COMRESET_DELAY * 1000);

    port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_NONE);
}

/*===========================================================================*
 *				port_override				     *
 *===========================================================================*/
static void port_override(struct port_state *ps)
{
    u32_t cmd;
    int spin_count = 0;
    const int max_spin = 1000;

    cmd = port_read(ps, AHCI_PORT_CMD);
    if (port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_CLO) < 0) {
        dprintf(V_ERR, ("%s: port_write failed in port_override\n", ahci_portname(ps)));
        return;
    }

    while ((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CLO) && (spin_count < max_spin)) {
        PORTREG_DELAY;
        spin_count++;
    }

    if (spin_count >= max_spin) {
        dprintf(V_ERR, ("%s: timeout waiting for CLO clear in port_override\n", ahci_portname(ps)));
        return;
    }

    dprintf(V_INFO, ("%s: overridden\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_start				     *
 *===========================================================================*/
static void port_start(struct port_state *ps)
{
    u32_t cmd;

    if (!ps) {
        dprintf(V_ERR, ("port_start: invalid port_state\n"));
        return;
    }

    if (port_write(ps, AHCI_PORT_SERR, ~0) != 0) {
        dprintf(V_ERR, ("%s: failed to reset SERR\n", ahci_portname(ps)));
        return;
    }

    if (port_write(ps, AHCI_PORT_IS, ~0) != 0) {
        dprintf(V_ERR, ("%s: failed to reset IS\n", ahci_portname(ps)));
        return;
    }

    cmd = port_read(ps, AHCI_PORT_CMD);
    if (port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_ST) != 0) {
        dprintf(V_ERR, ("%s: failed to start port\n", ahci_portname(ps)));
        return;
    }

    dprintf(V_INFO, ("%s: started\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_stop				     *
 *===========================================================================*/
static void port_stop(struct port_state *ps)
{
    if (!ps) {
        return;
    }

    u32_t cmd = port_read(ps, AHCI_PORT_CMD);
    if ((cmd & (AHCI_PORT_CMD_CR | AHCI_PORT_CMD_ST)) == 0) {
        return;
    }

    port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_ST);

    int spin_count = 0;
    while (port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CR) {
        if (++spin_count > PORTREG_DELAY) {
            dprintf(V_WARN, ("%s: failed to stop\n", ahci_portname(ps)));
            return;
        }
    }

    dprintf(V_INFO, ("%s: stopped\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_restart				     *
 *===========================================================================*/
static void port_restart(struct port_state *ps)
{
	if (!ps) {
		dprintf(V_ERR, ("port_restart: invalid port_state\n"));
		return;
	}

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
static void print_string(u16_t *buf, int start, int end)
{
    if (!buf || start > end || start < 0) {
        return;
    }

    int i, print_last = 0;

    while (end >= start && buf[end] == 0x2020) {
        end--;
    }

    if (end >= start && (buf[end] & 0xFF) == 0x20) {
        end--;
        print_last = 1;
    }

    for (i = start; i <= end; i++) {
        putchar((int)(buf[i] >> 8) & 0xFF);
        putchar((int)buf[i] & 0xFF);
    }

    if (print_last && i <= end + 1) {
        putchar((int)(buf[i] >> 8) & 0xFF);
    }
}

/*===========================================================================*
 *				port_id_check				     *
 *===========================================================================*/
static void port_id_check(struct port_state *ps, int success)
{
	u16_t *buf = NULL;

	assert(ps->state == STATE_WAIT_ID);

	ps->flags &= ~FLAG_BUSY;
	cancel_timer(&ps->cmd_info[0].timer);

	if (!success) {
		if (!(ps->flags & FLAG_ATAPI) && port_read(ps, AHCI_PORT_SIG) != ATA_SIG_ATA) {
			dprintf(V_INFO, "%s: may not be ATA, trying ATAPI\n", ahci_portname(ps));
			ps->flags |= FLAG_ATAPI;
			(void)gen_identify(ps, FALSE);
			return;
		}
		dprintf(V_ERR, "%s: unable to identify\n", ahci_portname(ps));
	}

	if (success) {
		buf = (u16_t *)ps->tmp_base;
		if (ps->flags & FLAG_ATAPI) {
			if (!atapi_id_check(ps, buf))
				success = 0;
		} else {
			if (!ata_id_check(ps, buf))
				success = 0;
		}
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
		if (ps->flags & FLAG_HAS_MEDIUM)
			printf(", %u byte sectors, %llu MB size",
				ps->sector_size,
				(ps->lba_count * ps->sector_size) / (1024 * 1024));
		printf("\n");
	}
}

/*===========================================================================*
 *				port_connect				     *
 *===========================================================================*/
static void port_connect(struct port_state *ps)
{
	u32_t status;
	u32_t sig;

	dprintf(V_INFO, "%s: device connected\n", ahci_portname(ps));

	port_start(ps);

	status = port_read(ps, AHCI_PORT_SSTS) & AHCI_PORT_SSTS_DET_MASK;
	if (status != AHCI_PORT_SSTS_DET_PHY) {
		dprintf(V_ERR, "%s: device vanished!\n", ahci_portname(ps));
		port_stop(ps);
		ps->state = STATE_NO_DEV;
		ps->flags &= ~FLAG_BUSY;
		return;
	}

	ps->flags &= (FLAG_BUSY | FLAG_BARRIER | FLAG_SUSPENDED);

	sig = port_read(ps, AHCI_PORT_SIG);
	if (sig == ATA_SIG_ATAPI) {
		ps->flags |= FLAG_ATAPI;
	}

	ps->state = STATE_WAIT_ID;
	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_MASK);

	(void)gen_identify(ps, FALSE);
}

/*===========================================================================*
 *				port_disconnect				     *
 *===========================================================================*/
static void port_disconnect(struct port_state *ps)
{
    if (ps == NULL || ps->device == NULL) {
        return;
    }

    dprintf(V_INFO, "%s: device disconnected\n", ahci_portname(ps));

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

	dprintf(V_DEV, "%s: polled status %u\n", ahci_portname(ps), status);

	if (status == AHCI_PORT_SSTS_DET_PHY) {
		tfd = port_read(ps, AHCI_PORT_TFD);

		if ((tfd & (AHCI_PORT_TFD_STS_BSY | AHCI_PORT_TFD_STS_DRQ)) == 0) {
			port_connect(ps);
			return;
		}
		if (ps->left > 0) {
			ps->left--;
			set_timer(&ps->cmd_info[0].timer, ahci_device_delay,
				port_timeout, BUILD_ARG((int)(ps - port_state), 0));
			return;
		}
	}

	if (status == AHCI_PORT_SSTS_DET_DET && ps->left > 0) {
		ps->left--;
		set_timer(&ps->cmd_info[0].timer, ahci_device_delay,
			port_timeout, BUILD_ARG((int)(ps - port_state), 0));
		return;
	}

	dprintf(V_INFO, "%s: device not ready\n", ahci_portname(ps));

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
static void port_intr(struct port_state *ps)
{
    u32_t smask, emask;
    int success;

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

        switch (ps->state) {
            case STATE_SPIN_UP:
                cancel_timer(&ps->cmd_info[0].timer);
                /* fall through */
            case STATE_NO_DEV:
                ps->state = STATE_WAIT_DEV;
                ps->left = ahci_device_checks;
                port_dev_check(ps);
                break;
            case STATE_WAIT_DEV:
                break;
            default:
                assert(0);
        }
        return;
    }

    if (emask & AHCI_PORT_IS_PRCS) {
        port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_N);
        dprintf(V_DEV, "%s: device detached\n", ahci_portname(ps));
        switch (ps->state) {
            case STATE_WAIT_ID:
            case STATE_GOOD_DEV:
                port_stop(ps);
                /* fall through */
            case STATE_BAD_DEV:
                port_disconnect(ps);
                port_hardreset(ps);
                break;
            default:
                assert(0);
        }
        return;
    }

    if (smask & AHCI_PORT_IS_MASK) {
        u32_t tfd = port_read(ps, AHCI_PORT_TFD);
        success = !(tfd & (AHCI_PORT_TFD_STS_ERR | AHCI_PORT_TFD_STS_DF));
        if (tfd & (AHCI_PORT_TFD_STS_ERR | AHCI_PORT_TFD_STS_DF) || (smask & AHCI_PORT_IS_RESTART)) {
            port_restart(ps);
        }
        if (ps->state == STATE_WAIT_ID) {
            port_id_check(ps, success);
        }
    }
}

/*===========================================================================*
 *				port_timeout				     *
 *===========================================================================*/
static void port_timeout(int arg)
{
	struct port_state *ps;
	int port = GET_PORT(arg);
	int cmd = GET_TAG(arg);

	if (port < 0 || port >= hba_state.nr_ports)
		return;

	ps = &port_state[port];

	if (ps->flags & FLAG_SUSPENDED) {
		if (cmd == 0) {
			blockdriver_mt_wakeup(ps->cmd_info[0].tid);
		}
	}

	switch (ps->state) {
		case STATE_SPIN_UP:
			if (port_read(ps, AHCI_PORT_IS) & AHCI_PORT_IS_PCS) {
				dprintf(V_INFO, "%s: bad controller, no interrupt\n", ahci_portname(ps));
				ps->state = STATE_WAIT_DEV;
				ps->left = ahci_device_checks;
				port_dev_check(ps);
			} else {
				dprintf(V_INFO, "%s: spin-up timeout\n", ahci_portname(ps));
				ps->state = STATE_NO_DEV;
				ps->flags &= ~FLAG_BUSY;
			}
			return;

		case STATE_WAIT_DEV:
			port_dev_check(ps);
			return;

		default:
			dprintf(V_ERR, "%s: timeout\n", ahci_portname(ps));
			port_restart(ps);
			if (ps->state == STATE_WAIT_ID) {
				port_id_check(ps, FALSE);
			}
			break;
	}
}

/*===========================================================================*
 *				port_wait				     *
 *===========================================================================*/
static void port_wait(struct port_state *ps)
{
    if (ps == NULL) {
        return;
    }

    ps->flags |= FLAG_SUSPENDED;

    while ((ps->flags & FLAG_BUSY) != 0) {
        blockdriver_mt_sleep();
    }

    ps->flags &= ~FLAG_SUSPENDED;
}

/*===========================================================================*
 *				port_issue				     *
 *===========================================================================*/
static void port_issue(struct port_state *ps, int cmd, clock_t timeout)
{
    if (!ps || cmd < 0 || cmd >= MAX_PORT_COMMANDS)
        return;

    uint32_t cmd_bit = 1U << cmd;

    if (ps->flags & FLAG_HAS_NCQ) {
        port_write(ps, AHCI_PORT_SACT, cmd_bit);
    }

    __insn_barrier();

    port_write(ps, AHCI_PORT_CI, cmd_bit);

    ps->pend_mask |= cmd_bit;

    if (ps->cmd_info && &ps->cmd_info[cmd]) {
        set_timer(&ps->cmd_info[cmd].timer, timeout, port_timeout,
                  BUILD_ARG((int)(ps - port_state), cmd));
    }
}

/*===========================================================================*
 *				port_exec				     *
 *===========================================================================*/
static int port_exec(struct port_state *ps, int cmd, clock_t timeout)
{
	int result;

	if (!ps || cmd < 0 || cmd >= CMD_MAX) {
		return EINVAL;
	}

	port_issue(ps, cmd, timeout);

	ps->cmd_info[cmd].tid = blockdriver_mt_get_tid();

	blockdriver_mt_sleep();

	cancel_timer(&ps->cmd_info[cmd].timer);

	if ((ps->flags & FLAG_BUSY) != 0) {
		return EBUSY;
	}

	dprintf(V_REQ, "%s: end of command -- %s\n", ahci_portname(ps),
	        (ps->cmd_info[cmd].result == RESULT_FAILURE) ? "failure" : "success");

	result = ps->cmd_info[cmd].result;

	if (result == RESULT_FAILURE)
		return EIO;

	return OK;
}

/*===========================================================================*
 *				port_alloc				     *
 *===========================================================================*/
static void port_alloc(struct port_state *ps)
{
	size_t fis_off, tmp_off, ct_off;
	size_t ct_offs[NR_CMDS];
	int i;
	u32_t cmd;

	fis_off = AHCI_CL_SIZE + AHCI_FIS_SIZE - 1;
	fis_off -= fis_off % AHCI_FIS_SIZE;

	tmp_off = fis_off + AHCI_FIS_SIZE + AHCI_TMP_ALIGN - 1;
	tmp_off -= tmp_off % AHCI_TMP_ALIGN;

	ct_off = tmp_off + AHCI_TMP_SIZE;
	for (i = 0; i < NR_CMDS; i++) {
		ct_off = (ct_off + AHCI_CT_ALIGN - 1) - ((ct_off + AHCI_CT_ALIGN - 1) % AHCI_CT_ALIGN);
		ct_offs[i] = ct_off;
		ct_off += AHCI_CT_SIZE;
	}
	ps->mem_size = ct_off;

	ps->mem_base = alloc_contig(ps->mem_size, AC_ALIGN4K, &ps->mem_phys);
	if (!ps->mem_base)
	{
		panic("unable to allocate port memory");
	}
	memset(ps->mem_base, 0, ps->mem_size);

	ps->cl_base = (u32_t *)ps->mem_base;
	ps->cl_phys = ps->mem_phys;
	if (ps->cl_phys % AHCI_CL_SIZE != 0)
		panic("cl_phys alignment error");

	ps->fis_base = (u32_t *)(ps->mem_base + fis_off);
	ps->fis_phys = ps->mem_phys + fis_off;
	if (ps->fis_phys % AHCI_FIS_SIZE != 0)
		panic("fis_phys alignment error");

	ps->tmp_base = (u8_t *)(ps->mem_base + tmp_off);
	ps->tmp_phys = ps->mem_phys + tmp_off;
	if (ps->tmp_phys % AHCI_TMP_ALIGN != 0)
		panic("tmp_phys alignment error");

	for (i = 0; i < NR_CMDS; i++) {
		ps->ct_base[i] = ps->mem_base + ct_offs[i];
		ps->ct_phys[i] = ps->mem_phys + ct_offs[i];
		if (ps->ct_phys[i] % AHCI_CT_ALIGN != 0)
			panic("ct_phys alignment error");
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
static void port_free(struct port_state *ps)
{
	u32_t cmd;

	if (ps == NULL)
		return;

	cmd = port_read(ps, AHCI_PORT_CMD);

	if (cmd & (AHCI_PORT_CMD_FR | AHCI_PORT_CMD_FRE)) {
		port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_FRE);

		if (!SPIN_UNTIL(!(port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_FR), PORTREG_DELAY))
			return;
	}

	if (ps->pad_base)
		free_contig(ps->pad_base, ps->pad_size);

	if (ps->mem_base)
		free_contig(ps->mem_base, ps->mem_size);
}

/*===========================================================================*
 *				port_init				     *
 *===========================================================================*/
static void port_init(struct port_state *ps)
{
    u32_t cmd;
    int i;

    if (ps == NULL) {
        return;
    }

    ps->queue_depth = 1;
    ps->state = STATE_SPIN_UP;
    ps->flags = FLAG_BUSY;
    ps->sector_size = 0;
    ps->open_count = 0;
    ps->pend_mask = 0;

    for (i = 0; i < NR_CMDS; i++) {
        if (!init_timer(&ps->cmd_info[i].timer)) {
            // Proper error handling/logging can be added here
            return;
        }
    }

    ps->reg = NULL;
    if (hba_state.base != NULL) {
        ptrdiff_t index = ps - port_state;
        if (index >= 0 && index < (ptrdiff_t)NR_PORTS) {
            ps->reg = (u32_t *)((u8_t *)hba_state.base +
                AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * index);
        }
    }

    if (ps->reg == NULL) {
        return;
    }

    if (!port_alloc(ps)) {
        return;
    }

    port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);

    cmd = port_read(ps, AHCI_PORT_CMD);
    port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_SUD);

    port_hardreset(ps);

    set_timer(&ps->cmd_info[0].timer, ahci_spinup_timeout,
        port_timeout, BUILD_ARG(ps - port_state, 0));
}

/*===========================================================================*
 *				ahci_probe				     *
 *===========================================================================*/
static int ahci_probe(int skip)
{
	int r, devind;
	u16_t vid, did;

	if (pci_init() != 0)
		return -1;

	r = pci_first_dev(&devind, &vid, &did);
	if (r <= 0)
		return -1;

	while (skip > 0) {
		r = pci_next_dev(&devind, &vid, &did);
		if (r <= 0)
			return -1;
		skip--;
	}

	if (pci_reserve(devind) != 0)
		return -1;

	return devind;
}

/*===========================================================================*
 *				ahci_reset				     *
 *===========================================================================*/
static void ahci_reset(void)
{
    u32_t ghc = hba_read(AHCI_HBA_GHC);

    hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE);
    hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE | AHCI_HBA_GHC_HR);

    if (!SPIN_UNTIL(!(hba_read(AHCI_HBA_GHC) & AHCI_HBA_GHC_HR), RESET_DELAY) ||
        (hba_read(AHCI_HBA_GHC) & AHCI_HBA_GHC_HR)) {
        panic("unable to reset HBA");
    }
}

/*===========================================================================*
 *				ahci_init				     *
 *===========================================================================*/
static void ahci_init(int devind)
{
    u32_t base = 0, size = 0, cap = 0, ghc = 0, mask = 0;
    int r = 0, port = 0, ioflag = 0;

    r = pci_get_bar(devind, PCI_BAR_6, &base, &size, &ioflag);
    if (r != OK)
        panic("unable to retrieve BAR: %d", r);

    if (ioflag != 0)
        panic("invalid BAR type");

    if (size < AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE)
        panic("HBA memory size too small: %u", size);

    if (size > AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * NR_PORTS)
        size = AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * NR_PORTS;

    hba_state.nr_ports = (size - AHCI_MEM_BASE_SIZE) / AHCI_MEM_PORT_SIZE;

    hba_state.base = (u32_t *) vm_map_phys(SELF, (void *) base, size);
    hba_state.size = size;
    if (hba_state.base == MAP_FAILED)
        panic("unable to map HBA memory");

    hba_state.irq = pci_attr_r8(devind, PCI_ILR);
    hba_state.hook_id = 0;

    r = sys_irqsetpolicy(hba_state.irq, 0, &hba_state.hook_id);
    if (r != OK)
        panic("unable to register IRQ: %d", r);

    r = sys_irqenable(&hba_state.hook_id);
    if (r != OK)
        panic("unable to enable IRQ: %d", r);

    ahci_reset();

    ghc = hba_read(AHCI_HBA_GHC);
    hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE | AHCI_HBA_GHC_IE);

    cap = hba_read(AHCI_HBA_CAP);
    hba_state.has_ncq = (cap & AHCI_HBA_CAP_SNCQ) ? 1 : 0;
    hba_state.has_clo = (cap & AHCI_HBA_CAP_SCLO) ? 1 : 0;
    hba_state.nr_cmds = (((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1);
    if (hba_state.nr_cmds > NR_CMDS)
        hba_state.nr_cmds = NR_CMDS;

    {
        u32_t vs = hba_read(AHCI_HBA_VS);
        int major = (int)(vs >> 16);
        int minor = (int)((vs >> 8) & 0xFF);
        int patch = (int)(vs & 0xFF);
        int nports = ((cap >> AHCI_HBA_CAP_NP_SHIFT) & AHCI_HBA_CAP_NP_MASK) + 1;
        int ncmds = ((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1;
        dprintf(V_INFO, ("AHCI%u: HBA v%d.%d%d, %d ports, %d commands, %s queuing, IRQ %d\n",
            ahci_instance, major, minor, patch, nports, ncmds,
            hba_state.has_ncq ? "supports" : "no", hba_state.irq));
    }

    dprintf(V_INFO, ("AHCI%u: CAP %08x, CAP2 %08x, PI %08x\n",
        ahci_instance, cap, hba_read(AHCI_HBA_CAP2),
        hba_read(AHCI_HBA_PI)));

    mask = hba_read(AHCI_HBA_PI);
    for (port = 0; port < hba_state.nr_ports; port++) {
        port_state[port].device = NO_DEVICE;
        port_state[port].state = STATE_NO_PORT;
        if (mask & (1U << port))
            port_init(&port_state[port]);
    }
}

/*===========================================================================*
 *				ahci_stop				     *
 *===========================================================================*/
static void ahci_stop(void)
{
	struct port_state *ps;
	int r, port;

	for (port = 0; port < hba_state.nr_ports; port++) {
		ps = &port_state[port];

		if (ps->state == STATE_NO_PORT)
			continue;

		port_stop(ps);
		port_free(ps);
	}

	ahci_reset();

	r = vm_unmap_phys(SELF, (void *)hba_state.base, hba_state.size);
	if (r != OK) {
		panic("unable to unmap HBA memory: %d", r);
	}

	r = sys_irqrmpolicy(&hba_state.hook_id);
	if (r != OK) {
		panic("unable to deregister IRQ: %d", r);
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
static void ahci_intr(unsigned int UNUSED(irq_mask))
{
	struct port_state *ps;
	u32_t is_mask, proc_mask = 0;
	int r, port;

	is_mask = hba_read(AHCI_HBA_IS);

	for (port = 0; port < hba_state.nr_ports; port++) {
		if (is_mask & (1U << port)) {
			ps = &port_state[port];
			port_intr(ps);
			if ((ps->flags & (FLAG_SUSPENDED | FLAG_BUSY)) == FLAG_SUSPENDED) {
				blockdriver_mt_wakeup(ps->cmd_info[0].tid);
			}
			proc_mask |= (1U << port);
		}
	}

	if (proc_mask)
		hba_write(AHCI_HBA_IS, proc_mask);

	r = sys_irqenable(&hba_state.hook_id);
	if (r != OK)
		panic("unable to enable IRQ: %d", r);
}

/*===========================================================================*
 *				ahci_get_params				     *
 *===========================================================================*/
static void ahci_get_params(void)
{
    long v;
    unsigned int i;
    size_t num_timevars = sizeof(ahci_timevar) / sizeof(ahci_timevar[0]);

    v = 0;
    if (env_parse("instance", "d", 0, &v, 0, 255) != 0 || v < 0 || v > 255) {
        ahci_instance = 0;
    } else {
        ahci_instance = (int)v;
    }

    v = V_ERR;
    if (env_parse("ahci_verbose", "d", 0, &v, V_NONE, V_REQ) != 0 || v < V_NONE || v > V_REQ) {
        ahci_verbose = V_ERR;
    } else {
        ahci_verbose = (int)v;
    }

    for (i = 0; i < num_timevars; i++) {
        v = ahci_timevar[i].default_ms;
        if (env_parse(ahci_timevar[i].name, "d", 0, &v, 1, LONG_MAX) != 0 || v < 1 || v > LONG_MAX) {
            v = ahci_timevar[i].default_ms;
        }
        *ahci_timevar[i].ptr = millis_to_hz(v);
    }

    ahci_device_delay = millis_to_hz(DEVICE_DELAY);
    ahci_device_checks = (ahci_device_timeout + ahci_device_delay - 1) / ahci_device_delay;
}

/*===========================================================================*
 *				ahci_set_mapping			     *
 *===========================================================================*/
static void ahci_set_mapping(void)
{
	char key[16], val[32], *p;
	unsigned int port;
	int i, j;

	for (i = 0, j = 0; i < NR_PORTS && j < MAX_DRIVES; i++) {
		if (port_state[i].state != STATE_NO_PORT) {
			ahci_map[j++] = i;
		}
	}
	for (; j < MAX_DRIVES; j++) {
		ahci_map[j] = NO_PORT;
	}

	if (snprintf(key, sizeof(key), "ahci%d_map", ahci_instance) < 0 || sizeof(key) < 1) {
		return;
	}
	if (env_get_param(key, val, sizeof(val)) == OK) {
		p = val;
		for (i = 0; i < MAX_DRIVES; i++) {
			if (*p) {
				errno = 0;
				port = (unsigned int)strtoul(p, &p, 0);
				if (errno != 0 || port >= NR_PORTS) {
					ahci_map[i] = NO_PORT;
				} else {
					ahci_map[i] = port;
				}
				while (*p == ',') p++;
			} else {
				ahci_map[i] = NO_PORT;
			}
		}
	}

	for (i = 0; i < MAX_DRIVES; i++) {
		j = ahci_map[i];
		if (j != NO_PORT && j < NR_PORTS) {
			port_state[j].device = i;
		}
	}
}

/*===========================================================================*
 *				sef_cb_init_fresh			     *
 *===========================================================================*/
static int sef_cb_init_fresh(int type, sef_init_info_t *UNUSED(info))
{
	int devind;

	if (ahci_get_params() != 0)
		return -1;

	devind = ahci_probe(ahci_instance);
	if (devind < 0) {
		panic("no matching device found");
		return -1;
	}

	if (ahci_init(devind) != 0)
		return -1;

	if (ahci_set_mapping() != 0)
		return -1;

	blockdriver_announce(type);

	return OK;
}

/*===========================================================================*
 *				sef_cb_signal_handler			     *
 *===========================================================================*/
static void sef_cb_signal_handler(int signo)
{
    int port;

    if (signo != SIGTERM) {
        return;
    }

    ahci_exiting = TRUE;

    for (port = 0; port < hba_state.nr_ports; port++) {
        if (port_state[port].open_count > 0) {
            return;
        }
    }

    ahci_stop();

    exit(EXIT_SUCCESS);
}

/*===========================================================================*
 *				sef_local_startup			     *
 *===========================================================================*/
static void sef_local_startup(void)
{
	sef_setcb_init_fresh(sef_cb_init_fresh);
	sef_setcb_signal_handler(sef_cb_signal_handler);
	blockdriver_mt_support_lu();
	sef_startup();
}

/*===========================================================================*
 *				ahci_portname				     *
 *===========================================================================*/
static char *ahci_portname(const struct port_state *ps)
{
	static char name[] = "AHCI0-P00";
	int idx;

	name[4] = '0' + ahci_instance;

	if (ps == NULL) {
		name[6] = 'P';
		name[7] = name[8] = '0';
		name[9] = '\0';
		return name;
	}

	if (ps->device == NO_DEVICE) {
		idx = (int)(ps - port_state);
		if (idx < 0) idx = 0;
		name[6] = 'P';
		name[7] = '0' + (idx / 10);
		name[8] = '0' + (idx % 10);
		name[9] = '\0';
	} else {
		name[6] = 'D';
		name[7] = '0' + ps->device;
		name[8] = '\0';
	}

	return name;
}

/*===========================================================================*
 *				ahci_map_minor				     *
 *===========================================================================*/
static struct port_state *ahci_map_minor(devminor_t minor, struct device **dvp)
{
    if (dvp == NULL) {
        return NULL;
    }

    if (minor >= 0 && minor < NR_MINORS) {
        int port = ahci_map[minor / DEV_PER_DRIVE];
        if (port == NO_PORT) {
            return NULL;
        }
        struct port_state *ps = &port_state[port];
        *dvp = &ps->part[minor % DEV_PER_DRIVE];
        return ps;
    }

    if ((unsigned)(minor - MINOR_d0p0s0) < NR_SUBDEVS) {
        int sub_minor = minor - MINOR_d0p0s0;
        int port = ahci_map[sub_minor / SUB_PER_DRIVE];
        if (port == NO_PORT) {
            return NULL;
        }
        struct port_state *ps = &port_state[port];
        *dvp = &ps->subpart[sub_minor % SUB_PER_DRIVE];
        return ps;
    }

    return NULL;
}

/*===========================================================================*
 *				ahci_part				     *
 *===========================================================================*/
static struct device *ahci_part(devminor_t minor)
{
	struct device *dv = NULL;

	if (ahci_map_minor(minor, &dv) == NULL)
		return NULL;

	return dv;
}

/*===========================================================================*
 *				ahci_open				     *
 *===========================================================================*/
static int ahci_open(devminor_t minor, int access)
{
	struct port_state *ps = ahci_get_port(minor);

	if (ps == NULL)
		return ENXIO;

	ps->cmd_info[0].tid = blockdriver_mt_get_tid();

	if (ps->flags & FLAG_BUSY) {
		port_wait(ps);
	}

	if (ps->state != STATE_GOOD_DEV)
		return ENXIO;

	if ((ps->flags & FLAG_READONLY) && (access & BDEV_W_BIT))
		return EACCES;

	if (ps->open_count == 0) {
		ps->flags &= ~FLAG_BARRIER;

		if ((ps->flags & FLAG_ATAPI)) {
			int r = atapi_check_medium(ps, 0);
			if (r != OK)
				return r;
		}

		memset(ps->part, 0, sizeof(ps->part));
		memset(ps->subpart, 0, sizeof(ps->subpart));
		ps->part[0].dv_size = ps->lba_count * ps->sector_size;

		partition(&ahci_dtab, ps->device * DEV_PER_DRIVE, P_PRIMARY,
			(ps->flags & FLAG_ATAPI) ? 1 : 0);

		blockdriver_mt_set_workers(ps->device, ps->queue_depth);

	} else {
		if (ps->flags & FLAG_BARRIER)
			return ENXIO;
	}

	ps->open_count++;

	return OK;
}

/*===========================================================================*
 *				ahci_close				     *
 *===========================================================================*/
static int ahci_close(devminor_t minor)
{
	struct port_state *ps = ahci_get_port(minor);
	int port;

	if (!ps) {
		dprintf(V_ERR, ("ahci_close: invalid port state\n"));
		return EINVAL;
	}

	if (ps->open_count <= 0) {
		dprintf(V_ERR, ("%s: closing already-closed port\n", ahci_portname(ps)));
		return EINVAL;
	}

	ps->open_count--;

	if (ps->open_count > 0)
		return OK;

	blockdriver_mt_set_workers(ps->device, 1);

	if (ps->state == STATE_GOOD_DEV && !(ps->flags & FLAG_BARRIER)) {
		dprintf(V_INFO, ("%s: flushing write cache\n", ahci_portname(ps)));
		if (gen_flush_wcache(ps) != OK) {
			dprintf(V_ERR, ("%s: failed to flush write cache\n", ahci_portname(ps)));
		}
	}

	if (ahci_exiting) {
		int all_closed = 1;
		for (port = 0; port < hba_state.nr_ports; port++) {
			if (port_state[port].open_count > 0) {
				all_closed = 0;
				break;
			}
		}

		if (all_closed) {
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
	if (!ps)
		return EINVAL;

	dv = ahci_part(minor);
	if (!dv)
		return EINVAL;

	if (ps->state != STATE_GOOD_DEV)
		return EIO;

	if (ps->flags & FLAG_BARRIER)
		return EIO;

	if (count == 0 || count > NR_IOREQS)
		return EINVAL;

	if (position >= dv->dv_size)
		return 0;

	pos = dv->dv_base + position;
	eof = dv->dv_base + dv->dv_size;

	return port_transfer(ps, pos, eof, endpt, (iovec_s_t *)iovec, count, do_write, flags);
}

/*===========================================================================*
 *				ahci_ioctl				     *
 *===========================================================================*/
static int ahci_ioctl(devminor_t minor, unsigned long request,
                      endpoint_t endpt, cp_grant_id_t grant, endpoint_t UNUSED(user_endpt))
{
    struct port_state *ps = ahci_get_port(minor);
    int r = OK;
    int val = 0;

    if (!ps)
        return EIO;

    switch (request) {
        case DIOCEJECT:
            if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER))
                return EIO;
            if (!(ps->flags & FLAG_ATAPI))
                return EINVAL;
            return atapi_load_eject(ps, 0, FALSE);

        case DIOCOPENCT:
            r = sys_safecopyto(endpt, grant, 0,
                               (vir_bytes)&ps->open_count, sizeof(ps->open_count));
            return r == OK ? OK : r;

        case DIOCFLUSH:
            if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER))
                return EIO;
            return gen_flush_wcache(ps);

        case DIOCSETWC:
            if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER))
                return EIO;
            r = sys_safecopyfrom(endpt, grant, 0, (vir_bytes)&val, sizeof(val));
            if (r != OK)
                return r;
            return gen_set_wcache(ps, val);

        case DIOCGETWC:
            if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER))
                return EIO;
            r = gen_get_wcache(ps, &val);
            if (r != OK)
                return r;
            r = sys_safecopyto(endpt, grant, 0, (vir_bytes)&val, sizeof(val));
            return r == OK ? OK : r;

        default:
            return ENOTTY;
    }
}

/*===========================================================================*
 *				ahci_device				     *
 *===========================================================================*/
static int ahci_device(devminor_t minor, device_id_t *id)
{
	if (id == NULL) {
		return EINVAL;
	}

	struct device *dv;
	struct port_state *ps = ahci_map_minor(minor, &dv);
	if (ps == NULL) {
		return ENXIO;
	}

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
		fprintf(stderr, "Error: device mapping for minor %d disappeared\n", minor);
		abort();
	}

	return ps;
}

/*===========================================================================*
 *				main					     *
 *===========================================================================*/
int main(int argc, char **argv)
{
    if (env_setargs(argc, argv) != 0) {
        return 1;
    }

    sef_local_startup();

    if (blockdriver_mt_task(&ahci_dtab) != 0) {
        return 1;
    }

    return 0;
}
