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
	const u8_t packet[ATAPI_PACKET_SIZE], size_t size, int write)
{
	if (size > AHCI_TMP_SIZE) {
		return -1;
	}

	cmd_fis_t fis = { .cf_cmd = ATA_CMD_PACKET };
	prd_t prd[1];
	int nr_prds = 0;

	if (size > 0) {
		fis.cf_feat = ATA_FEAT_PACKET_DMA;
		if (!write && (ps->flags & FLAG_USE_DMADIR)) {
			fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;
		}

		prd[0] = (prd_t){
			.vp_addr = ps->tmp_phys,
			.vp_size = size,
		};
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
	u8_t packet[ATAPI_PACKET_SIZE] = { ATAPI_CMD_TEST_UNIT };

	return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_request_sense			     *
 *===========================================================================*/
static int atapi_request_sense(struct port_state *ps, int cmd, int *sense)
{
	enum {
		CDB_OPCODE_OFFSET = 0,
		CDB_ALLOCATION_LENGTH_OFFSET = 4
	};
	enum {
		SENSE_DATA_SENSE_KEY_OFFSET = 2,
		SENSE_DATA_ASC_OFFSET = 12,
		SENSE_DATA_ASCQ_OFFSET = 13
	};
	enum { SENSE_KEY_MASK = 0x0F };

	if (ps == NULL || sense == NULL) {
		return -1;
	}

	u8_t packet[ATAPI_PACKET_SIZE] = {0};
	packet[CDB_OPCODE_OFFSET] = ATAPI_CMD_REQUEST_SENSE;
	packet[CDB_ALLOCATION_LENGTH_OFFSET] = ATAPI_REQUEST_SENSE_LEN;

	const int status = atapi_exec(ps, cmd, packet,
		ATAPI_REQUEST_SENSE_LEN, FALSE);

	if (status != OK) {
		return status;
	}

	const u8_t sense_key =
		ps->tmp_base[SENSE_DATA_SENSE_KEY_OFFSET] & SENSE_KEY_MASK;
	const u8_t asc = ps->tmp_base[SENSE_DATA_ASC_OFFSET];
	const u8_t ascq = ps->tmp_base[SENSE_DATA_ASCQ_OFFSET];

	dprintf(V_REQ, ("%s: ATAPI SENSE: sense %x ASC %x ASCQ %x\n",
		ahci_portname(ps), sense_key, asc, ascq));

	*sense = sense_key;

	return OK;
}

/*===========================================================================*
 *				atapi_load_eject			     *
 *===========================================================================*/
static int atapi_load_eject(struct port_state *ps, int cmd, int load)
{
	const u8_t start_stop_flags = load ? ATAPI_START_STOP_LOAD :
					     ATAPI_START_STOP_EJECT;

	u8_t packet[ATAPI_PACKET_SIZE] = {
		[0] = ATAPI_CMD_START_STOP,
		[4] = start_stop_flags,
	};

	return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_read_capacity			     *
 *===========================================================================*/
static int atapi_read_capacity(struct port_state *ps, int cmd)
{
	u8_t packet[ATAPI_PACKET_SIZE] = { [0] = ATAPI_CMD_READ_CAPACITY };

	const int r = atapi_exec(ps, cmd, packet, ATAPI_READ_CAPACITY_LEN,
		FALSE);
	if (r != OK) {
		return r;
	}

	const u8_t *buf = ps->tmp_base;

	const u32_t last_lba = ((u32_t)buf[0] << 24) |
		((u32_t)buf[1] << 16) |
		((u32_t)buf[2] << 8) | (u32_t)buf[3];
	const u32_t sector_size = ((u32_t)buf[4] << 24) |
		((u32_t)buf[5] << 16) |
		((u32_t)buf[6] << 8) | (u32_t)buf[7];

	if (sector_size == 0 || (sector_size & 1) != 0) {
		dprintf(V_ERR, ("%s: invalid medium sector size %u\n",
			ahci_portname(ps), sector_size));
		return EINVAL;
	}

	ps->sector_size = sector_size;
	ps->lba_count = (u64_t)last_lba + 1;

	const u64_t size_in_mb =
		(ps->lba_count * ps->sector_size) / (1024ULL * 1024ULL);

	dprintf(V_INFO,
		("%s: medium detected (%u byte sectors, %llu MB size)\n",
		ahci_portname(ps), ps->sector_size, size_in_mb));

	return OK;
}

/*===========================================================================*
 *				atapi_check_medium			     *
 *===========================================================================*/
static int atapi_check_medium(struct port_state *ps, int cmd)
{
	if (atapi_test_unit(ps, cmd) != OK) {
		ps->flags &= ~FLAG_HAS_MEDIUM;

		int sense;
		if (atapi_request_sense(ps, cmd, &sense) != OK ||
		    sense != ATAPI_SENSE_UNIT_ATT) {
			return ENXIO;
		}
	} else if (ps->flags & FLAG_HAS_MEDIUM) {
		return OK;
	}

	if (atapi_read_capacity(ps, cmd) != OK) {
		return EIO;
	}

	ps->flags |= FLAG_HAS_MEDIUM;
	return OK;
}

/*===========================================================================*
 *				atapi_id_check				     *
 *===========================================================================*/
static int atapi_id_check(struct port_state *ps, u16_t *buf)
{
	/* Determine whether we support this ATAPI device based on the
	 * identification data it returned, and store some of its properties.
	 */
	const u16_t gcap = buf[ATA_ID_GCAP];
	const u16_t cap = buf[ATA_ID_CAP];
	const u16_t dmadir = buf[ATA_ID_DMADIR];

	const u16_t required_gcap = ATA_ID_GCAP_ATAPI | ATA_ID_GCAP_REMOVABLE;
	const u16_t gcap_mask = required_gcap | ATA_ID_GCAP_INCOMPLETE;
	const int is_valid_type = ((gcap & gcap_mask) == required_gcap);

	const int supports_dma_cap = ((cap & ATA_ID_CAP_DMA) == ATA_ID_CAP_DMA);
	const u16_t dmadir_flags = ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA;
	const int supports_dmadir = ((dmadir & dmadir_flags) == dmadir_flags);

	/* The device must be an ATAPI device; it must have removable media;
	 * it must support DMA without DMADIR, or DMADIR for DMA.
	 */
	if (!is_valid_type || (!supports_dma_cap && !supports_dmadir)) {
		dprintf(V_ERR, ("%s: unsupported ATAPI device\n", ahci_portname(ps)));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x DMADIR %04x\n",
			ahci_portname(ps), gcap, cap, dmadir));
		return FALSE;
	}

	/* Remember whether to use the DMADIR flag when appropriate. */
	if ((dmadir & ATA_ID_DMADIR_DMADIR) != 0) {
		ps->flags |= FLAG_USE_DMADIR;
	}

	/* ATAPI CD-ROM devices are considered read-only. */
	const u16_t device_type = (gcap & ATA_ID_GCAP_TYPE_MASK) >> ATA_ID_GCAP_TYPE_SHIFT;
	if (device_type == ATAPI_TYPE_CDROM) {
		ps->flags |= FLAG_READONLY;
	}

	const u16_t sup1 = buf[ATA_ID_SUP1];
	const int is_sup_valid = ((sup1 & ATA_ID_SUP1_VALID_MASK) == ATA_ID_SUP1_VALID);

	if (is_sup_valid && (ps->flags & FLAG_READONLY) == 0) {
		/* Save write cache related capabilities of the device. It is
		 * possible, although unlikely, that a device has support for
		 * either of these but not both.
		 */
		const u16_t sup0 = buf[ATA_ID_SUP0];
		if ((sup0 & ATA_ID_SUP0_WCACHE) != 0) {
			ps->flags |= FLAG_HAS_WCACHE;
		}
		if ((sup1 & ATA_ID_SUP1_FLUSH) != 0) {
			ps->flags |= FLAG_HAS_FLUSH;
		}
	}

	return TRUE;
}

/*===========================================================================*
 *				atapi_transfer				     *
 *===========================================================================*/
static inline void write_be32(u8_t *p, u32_t val)
{
	p[0] = (u8_t)(val >> 24);
	p[1] = (u8_t)(val >> 16);
	p[2] = (u8_t)(val >> 8);
	p[3] = (u8_t)val;
}

static int atapi_transfer(struct port_state *ps, int cmd, u64_t start_lba,
	unsigned int count, int write, prd_t *prdt, int nr_prds)
{
	if (!ps || (nr_prds > 0 && !prdt)) {
		return -1;
	}

	cmd_fis_t fis;
	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_PACKET;
	fis.cf_feat = ATA_FEAT_PACKET_DMA;
	if (!write && (ps->flags & FLAG_USE_DMADIR)) {
		fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;
	}

	u8_t packet[ATAPI_PACKET_SIZE];
	memset(packet, 0, sizeof(packet));
	packet[0] = write ? ATAPI_CMD_WRITE : ATAPI_CMD_READ;
	write_be32(&packet[2], (u32_t)start_lba);
	write_be32(&packet[6], count);

	port_set_cmd(ps, cmd, &fis, packet, prdt, nr_prds, write);

	return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				ata_id_check				     *
 *===========================================================================*/
static bool is_supported_device(const u16_t *buf)
{
	const u16_t gcap = buf[ATA_ID_GCAP];
	const u16_t required_gcap_mask = ATA_ID_GCAP_ATA_MASK |
		ATA_ID_GCAP_REMOVABLE | ATA_ID_GCAP_INCOMPLETE;
	if ((gcap & required_gcap_mask) != ATA_ID_GCAP_ATA) {
		return false;
	}

	const u16_t cap = buf[ATA_ID_CAP];
	const u16_t required_cap = ATA_ID_CAP_LBA | ATA_ID_CAP_DMA;
	if ((cap & required_cap) != required_cap) {
		return false;
	}

	const u16_t sup1 = buf[ATA_ID_SUP1];
	const u16_t required_sup1_mask = ATA_ID_SUP1_VALID_MASK |
		ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48;
	const u16_t required_sup1_val = ATA_ID_SUP1_VALID |
		ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48;
	if ((sup1 & required_sup1_mask) != required_sup1_val) {
		return false;
	}

	return true;
}

static u64_t get_lba_count(const u16_t *buf)
{
	return ((u64_t)buf[ATA_ID_LBA3] << 48) |
	       ((u64_t)buf[ATA_ID_LBA2] << 32) |
	       ((u64_t)buf[ATA_ID_LBA1] << 16) |
	       (u64_t)buf[ATA_ID_LBA0];
}

static void configure_ncq(struct port_state *ps, const u16_t *buf)
{
	if (hba_state.has_ncq && (buf[ATA_ID_SATA_CAP] & ATA_ID_SATA_CAP_NCQ)) {
		ps->flags |= FLAG_HAS_NCQ;
		size_t depth = (buf[ATA_ID_QDEPTH] & ATA_ID_QDEPTH_MASK) + 1;
		ps->queue_depth = (depth > hba_state.nr_cmds) ?
			hba_state.nr_cmds : depth;
	}
}

static unsigned int get_sector_size(const u16_t *buf)
{
	const u16_t plss = buf[ATA_ID_PLSS];
	const u16_t required_plss_mask = ATA_ID_PLSS_VALID_MASK |
		ATA_ID_PLSS_LLS;
	const u16_t required_plss_val = ATA_ID_PLSS_VALID | ATA_ID_PLSS_LLS;

	if ((plss & required_plss_mask) == required_plss_val) {
		u32_t long_sector_size_words =
			((u32_t)buf[ATA_ID_LSS1] << 16) | buf[ATA_ID_LSS0];
		return long_sector_size_words * 2;
	}

	return ATA_SECTOR_SIZE;
}

static void set_feature_flags(struct port_state *ps, const u16_t *buf)
{
	ps->flags |= FLAG_HAS_MEDIUM | FLAG_HAS_FLUSH;

	if (buf[ATA_ID_SUP0] & ATA_ID_SUP0_WCACHE) {
		ps->flags |= FLAG_HAS_WCACHE;
	}

	const u16_t ena2 = buf[ATA_ID_ENA2];
	const u16_t required_ena2_mask = ATA_ID_ENA2_VALID_MASK |
		ATA_ID_ENA2_FUA;
	const u16_t required_ena2_val = ATA_ID_ENA2_VALID | ATA_ID_ENA2_FUA;
	if ((ena2 & required_ena2_mask) == required_ena2_val) {
		ps->flags |= FLAG_HAS_FUA;
	}
}

static int ata_id_check(struct port_state *ps, u16_t *buf)
{
	if (!is_supported_device(buf)) {
		dprintf(V_ERR, ("%s: unsupported ATA device\n",
			ahci_portname(ps)));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x SUP1 %04x\n",
			ahci_portname(ps), buf[ATA_ID_GCAP], buf[ATA_ID_CAP],
			buf[ATA_ID_SUP1]));
		return FALSE;
	}

	ps->lba_count = get_lba_count(buf);

	configure_ncq(ps, buf);

	ps->sector_size = get_sector_size(buf);
	if (ps->sector_size < ATA_SECTOR_SIZE) {
		dprintf(V_ERR, ("%s: invalid sector size %u\n",
			ahci_portname(ps), ps->sector_size));
		return FALSE;
	}

	set_feature_flags(ps, buf);

	return TRUE;
}

/*===========================================================================*
 *				ata_transfer				     *
 *===========================================================================*/
static int ata_transfer(struct port_state *ps, int cmd, u64_t start_lba,
	unsigned int count, int write, int force, prd_t *prdt, int nr_prds)
{
	cmd_fis_t fis;
	unsigned int effective_count = count;

	assert(count <= ATA_MAX_SECTORS);

	if (effective_count == ATA_MAX_SECTORS) {
		effective_count = 0;
	}

	memset(&fis, 0, sizeof(fis));
	fis.cf_dev = ATA_DEV_LBA;

	const bool has_ncq = (ps->flags & FLAG_HAS_NCQ) != 0;
	const bool use_fua = write && force && ((ps->flags & FLAG_HAS_FUA) != 0);

	if (has_ncq) {
		fis.cf_cmd = write ? ATA_CMD_WRITE_FPDMA_QUEUED :
			ATA_CMD_READ_FPDMA_QUEUED;
		if (use_fua) {
			fis.cf_dev |= ATA_DEV_FUA;
		}
	} else {
		if (write) {
			fis.cf_cmd = use_fua ? ATA_CMD_WRITE_DMA_FUA_EXT :
				ATA_CMD_WRITE_DMA_EXT;
		} else {
			fis.cf_cmd = ATA_CMD_READ_DMA_EXT;
		}
	}

	const u64_t lba_mask = 0x00FFFFFFUL;
	const int lba_shift = 24;
	fis.cf_lba = start_lba & lba_mask;
	fis.cf_lba_exp = (start_lba >> lba_shift) & lba_mask;

	const unsigned int sector_mask = 0xFF;
	const int sector_shift = 8;
	fis.cf_sec = effective_count & sector_mask;
	fis.cf_sec_exp = (effective_count >> sector_shift) & sector_mask;

	port_set_cmd(ps, cmd, &fis, NULL, prdt, nr_prds, write);

	return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				gen_identify				     *
 *===========================================================================*/
static int gen_identify(struct port_state *ps, int blocking)
{
	cmd_fis_t fis;
	memset(&fis, 0, sizeof(fis));

	fis.cf_cmd = (ps->flags & FLAG_ATAPI) ?
		ATA_CMD_IDENTIFY_PACKET : ATA_CMD_IDENTIFY;

	prd_t prd = {
		.vp_addr = ps->tmp_phys,
		.vp_size = ATA_ID_SIZE,
	};

	port_set_cmd(ps, 0, &fis, NULL, &prd, 1, FALSE);

	if (blocking) {
		return port_exec(ps, 0, ahci_command_timeout);
	}

	port_issue(ps, 0, ahci_command_timeout);
	return OK;
}

/*===========================================================================*
 *				gen_flush_wcache			     *
 *===========================================================================*/
static int gen_flush_wcache(struct port_state *ps)
{
	if (!ps || !(ps->flags & FLAG_HAS_FLUSH)) {
		return EINVAL;
	}

	const cmd_fis_t fis = {
		.cf_cmd = ATA_CMD_FLUSH_CACHE
	};

	port_set_cmd(ps, 0, &fis, NULL, NULL, 0, FALSE);

	return port_exec(ps, 0, ahci_flush_timeout);
}

/*===========================================================================*
 *				gen_get_wcache				     *
 *===========================================================================*/
static int gen_get_wcache(struct port_state *ps, int *val)
{
	if (!ps || !val) {
		return EINVAL;
	}

	if (!(ps->flags & FLAG_HAS_WCACHE)) {
		return EINVAL;
	}

	const int r = gen_identify(ps, TRUE);
	if (r != OK) {
		return r;
	}

	const u16_t *identify_data = (const u16_t *)ps->tmp_base;
	const u16_t capabilities_word = identify_data[ATA_ID_ENA0];

	*val = (capabilities_word & ATA_ID_ENA0_WCACHE) != 0;

	return OK;
}

/*===========================================================================*
 *				gen_set_wcache				     *
 *===========================================================================*/
static int gen_set_wcache(struct port_state *ps, int enable)
{
	if (!ps) {
		return EINVAL;
	}

	if (!(ps->flags & FLAG_HAS_WCACHE)) {
		return EINVAL;
	}

	const clock_t timeout = enable ? ahci_command_timeout : ahci_flush_timeout;
	const int slot = 0;

	const cmd_fis_t fis = {
		.cf_cmd = ATA_CMD_SET_FEATURES,
		.cf_feat = enable ? ATA_SF_EN_WCACHE : ATA_SF_DI_WCACHE
	};

	port_set_cmd(ps, slot, &fis, NULL, NULL, 0, FALSE);

	return port_exec(ps, slot, timeout);
}

/*===========================================================================*
 *				ct_set_fis				     *
 *===========================================================================*/
typedef struct __attribute__((packed__)) fis_reg_h2d {
	u8_t fis_type;
	u8_t flags;
	u8_t command;
	u8_t features;
	u8_t lba_low;
	u8_t lba_mid;
	u8_t lba_high;
	u8_t device;
	u8_t lba_low_exp;
	u8_t lba_mid_exp;
	u8_t lba_high_exp;
	u8_t features_exp;
	u8_t sec_count;
	u8_t sec_count_exp;
	u8_t control;
	u8_t reserved[ATA_H2D_SIZE - 15];
} fis_reg_h2d_t;

static vir_bytes ct_set_fis(u8_t *ct, cmd_fis_t *fis, unsigned int tag)
{
	if (ct == NULL || fis == NULL) {
		return 0;
	}

	fis_reg_h2d_t *h2d_fis = (fis_reg_h2d_t *)ct;

	memset(h2d_fis, 0, sizeof(*h2d_fis));

	h2d_fis->fis_type = ATA_FIS_TYPE_H2D;
	h2d_fis->flags = ATA_H2D_FLAGS_C;
	h2d_fis->command = fis->cf_cmd;
	h2d_fis->device = fis->cf_dev;
	h2d_fis->control = fis->cf_ctl;

	h2d_fis->lba_low = (u8_t)(fis->cf_lba);
	h2d_fis->lba_mid = (u8_t)(fis->cf_lba >> 8);
	h2d_fis->lba_high = (u8_t)(fis->cf_lba >> 16);
	h2d_fis->lba_low_exp = (u8_t)(fis->cf_lba_exp);
	h2d_fis->lba_mid_exp = (u8_t)(fis->cf_lba_exp >> 8);
	h2d_fis->lba_high_exp = (u8_t)(fis->cf_lba_exp >> 16);

	if (ATA_IS_FPDMA_CMD(fis->cf_cmd)) {
		h2d_fis->features = fis->cf_sec;
		h2d_fis->features_exp = fis->cf_sec_exp;
		h2d_fis->sec_count = (u8_t)(tag << ATA_SEC_TAG_SHIFT);
		h2d_fis->sec_count_exp = 0;
	} else {
		h2d_fis->features = fis->cf_feat;
		h2d_fis->features_exp = fis->cf_feat_exp;
		h2d_fis->sec_count = fis->cf_sec;
		h2d_fis->sec_count_exp = fis->cf_sec_exp;
	}

	return sizeof(*h2d_fis);
}

/*===========================================================================*
 *				ct_set_packet				     *
 *===========================================================================*/
static void ct_set_packet(u8_t *ct, const u8_t *packet)
{
	if (ct == NULL || packet == NULL) {
		return;
	}

	memcpy(&ct[AHCI_CT_PACKET_OFF], packet, ATAPI_PACKET_SIZE);
}

/*===========================================================================*
 *				ct_set_prdt				     *
 *===========================================================================*/
typedef struct {
	u32_t dba;
	u32_t dbau;
	u32_t reserved;
	u32_t dbc;
} ahci_prde_t;

static void ct_set_prdt(u8_t *ct, prd_t *prdt, int nr_prds)
{
	if (ct == NULL || prdt == NULL || nr_prds <= 0) {
		return;
	}

	ahci_prde_t *prd_table = (ahci_prde_t *)&ct[AHCI_CT_PRDT_OFF];

	for (int i = 0; i < nr_prds; i++) {
		prd_table[i].dba = prdt[i].vp_addr;
		prd_table[i].dbau = 0;
		prd_table[i].reserved = 0;
		prd_table[i].dbc = (prdt[i].vp_size > 0) ? (prdt[i].vp_size - 1) : 0;
	}
}

/*===========================================================================*
 *				port_set_cmd				     *
 *===========================================================================*/
static void port_set_cmd(struct port_state *ps, int cmd, cmd_fis_t *fis,
	u8_t packet[ATAPI_PACKET_SIZE], prd_t *prdt, int nr_prds, int write)
{
	const int is_ncq_cmd = ATA_IS_FPDMA_CMD(fis->cf_cmd);
	if (is_ncq_cmd) {
		ps->flags |= FLAG_NCQ_MODE;
	} else {
		assert(!ps->pend_mask);
		ps->flags &= ~FLAG_NCQ_MODE;
	}

	u8_t *ct = ps->ct_base[cmd];
	assert(ct != NULL);
	assert(nr_prds <= NR_PRDS);

	const vir_bytes size = ct_set_fis(ct, fis, cmd);

	if (packet != NULL) {
		ct_set_packet(ct, packet);
	}
	ct_set_prdt(ct, prdt, nr_prds);

	u32_t *cl = &ps->cl_base[cmd * AHCI_CL_ENTRY_DWORDS];

	u32_t cl0_flags = 0;
	if (write) {
		cl0_flags |= AHCI_CL_WRITE;
	}
	if (packet != NULL) {
		cl0_flags |= AHCI_CL_ATAPI;
	}
	if (!is_ncq_cmd && (nr_prds > 0 || packet != NULL)) {
		cl0_flags |= AHCI_CL_PREFETCHABLE;
	}

	const u32_t cfl_dword_count = size / sizeof(u32_t);

	memset(cl, 0, AHCI_CL_ENTRY_SIZE);
	cl[0] = ((u32_t)nr_prds << AHCI_CL_PRDTL_SHIFT) |
		(cfl_dword_count << AHCI_CL_CFL_SHIFT) |
		cl0_flags;
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

	const char * const status_str =
		(result == RESULT_SUCCESS) ? "succeeded" : "failed";
	dprintf(V_REQ, ("%s: command %d %s\n", ahci_portname(ps),
		cmd, status_str));

	ps->cmd_info[cmd].result = result;

	assert(ps->pend_mask & (1U << cmd));
	ps->pend_mask &= ~(1U << cmd);

	if (ps->state != STATE_WAIT_ID) {
		blockdriver_mt_wakeup(ps->cmd_info[cmd].tid);
	}
}

/*===========================================================================*
 *				port_fail_cmds				     *
 *===========================================================================*/
static void port_fail_cmds(struct port_state *ps)
{
	if (!ps) {
		return;
	}

	for (int i = 0; ps->pend_mask != 0 && i < ps->queue_depth; i++) {
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
	if (!ps) {
		return;
	}

	const u32_t cmd_reg = (ps->flags & FLAG_NCQ_MODE) ? AHCI_PORT_SACT : AHCI_PORT_CI;
	const u32_t active_mask = port_read(ps, cmd_reg);
	const u32_t completed_mask = ps->pend_mask & ~active_mask;

	for (int i = 0; i < ps->queue_depth; i++) {
		if (completed_mask & (1U << i)) {
			port_finish_cmd(ps, i, RESULT_SUCCESS);
		}
	}
}

/*===========================================================================*
 *				port_find_cmd				     *
 *===========================================================================*/
static int port_find_cmd(struct port_state *ps)
{
	for (int i = 0; i < ps->queue_depth; i++) {
		if ((ps->pend_mask & (1U << i)) == 0) {
			return i;
		}
	}

	/* This path should be unreachable if the caller ensures a slot is free. */
	abort();
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
	}

	ps->pad_base = alloc_contig(size, 0, &ps->pad_phys);
	if (ps->pad_base == NULL) {
		ps->pad_size = 0;
		dprintf(V_ERR, ("%s: unable to allocate a padding buffer of "
			"size %lu\n", ahci_portname(ps),
			(unsigned long)size));
		return ENOMEM;
	}

	ps->pad_size = size;
	dprintf(V_INFO, ("%s: allocated padding buffer of size %lu\n",
		ahci_portname(ps), (unsigned long)size));

	return OK;
}

/*===========================================================================*
 *				sum_iovec				     *
 *===========================================================================*/
static int sum_iovec(struct port_state *ps, endpoint_t endpt,
	iovec_s_t *iovec, int nr_req, vir_bytes *total)
{
	vir_bytes bytes = 0;

	for (int i = 0; i < nr_req; i++) {
		const vir_bytes size = iovec[i].iov_size;

		if (size == 0 || (size & 1)) {
			dprintf(V_ERR, ("%s: bad size %lu in iovec from %d\n",
				ahci_portname(ps), size, endpt));
			return EINVAL;
		}

		if (bytes > LONG_MAX - size) {
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
	int nr_prds = 0;
	int r;

	const size_t trail = (ps->sector_size - (lead + size)) % ps->sector_size;

	if (lead > 0 || trail > 0) {
		if ((r = port_get_padbuf(ps, ps->sector_size)) != OK) {
			return r;
		}
	}

	if (lead > 0) {
		if (nr_prds >= NR_PRDS) {
			return E2BIG;
		}
		prdt[nr_prds].vp_addr = ps->pad_phys;
		prdt[nr_prds].vp_size = lead;
		nr_prds++;
	}

	if (size > 0) {
		struct vumap_vir vvec[NR_PRDS];
		int num_vvecs = 0;
		vir_bytes remaining_size = size;

		for (int i = 0; i < nr_req && remaining_size > 0 &&
		    num_vvecs < NR_PRDS; i++) {
			const size_t bytes = MIN(iovec[i].iov_size, remaining_size);

			if (endpt == SELF) {
				vvec[num_vvecs].vv_addr =
				    (vir_bytes)iovec[i].iov_grant;
			} else {
				vvec[num_vvecs].vv_grant = iovec[i].iov_grant;
			}
			vvec[num_vvecs].vv_size = bytes;
			num_vvecs++;
			remaining_size -= bytes;
		}

		if (num_vvecs > 0) {
			int pcount = NR_PRDS - nr_prds;
			if (pcount <= 0) {
				return E2BIG;
			}

			const int map_flags = (write ? VUA_READ : VUA_WRITE);
			r = sys_vumap(endpt, vvec, num_vvecs, 0, map_flags,
			    &prdt[nr_prds], &pcount);

			if (r != OK) {
				dprintf(V_ERR,
				    ("%s: unable to map memory from %d (%d)\n",
				    ahci_portname(ps), endpt, r));
				return r;
			}

			for (int j = 0; j < pcount; j++) {
				if (prdt[nr_prds + j].vp_addr & 1) {
					dprintf(V_ERR, ("%s: bad physical address "
					    "from %d\n", ahci_portname(ps), endpt));
					return EINVAL;
				}
			}

			nr_prds += pcount;
		}
	}

	if (trail > 0) {
		if (nr_prds >= NR_PRDS) {
			return E2BIG;
		}
		prdt[nr_prds].vp_addr = ps->pad_phys + lead;
		prdt[nr_prds].vp_size = trail;
		nr_prds++;
	}

	return nr_prds;
}

/*===========================================================================*
 *				port_transfer				     *
 *===========================================================================*/
static int check_transfer_alignment(const struct port_state *ps,
	endpoint_t endpt, vir_bytes lead, vir_bytes size, int is_write)
{
	if ((lead & 1) || (is_write && lead != 0)) {
		dprintf(V_ERR, ("%s: unaligned position from %d\n",
			ahci_portname(ps), endpt));
		return EINVAL;
	}

	if (is_write && (size % ps->sector_size) != 0) {
		dprintf(V_ERR, ("%s: unaligned size %lu from %d\n",
			ahci_portname(ps), size, endpt));
		return EINVAL;
	}

	return OK;
}

static ssize_t port_transfer(struct port_state *ps, u64_t pos, u64_t eof,
	endpoint_t endpt, iovec_s_t *iovec, int nr_req, int write, int flags)
{
	vir_bytes size;
	int status = sum_iovec(ps, endpt, iovec, nr_req, &size);
	if (status != OK) {
		return status;
	}

	dprintf(V_REQ, ("%s: %s for %lu bytes at pos %llx\n",
		ahci_portname(ps), write ? "write" : "read", size, pos));

	assert(ps->state == STATE_GOOD_DEV);
	assert(ps->flags & FLAG_HAS_MEDIUM);
	assert(ps->sector_size > 0);

	if (size > MAX_TRANSFER) {
		size = MAX_TRANSFER;
	}

	if (pos + size > eof) {
		size = (vir_bytes)(eof - pos);
	}

	const u64_t start_lba = pos / ps->sector_size;
	const vir_bytes lead = (vir_bytes)(pos % ps->sector_size);
	const unsigned int count =
		(lead + size + ps->sector_size - 1) / ps->sector_size;

	status = check_transfer_alignment(ps, endpt, lead, size, write);
	if (status != OK) {
		return status;
	}

	prd_t prdt[NR_PRDS];
	const int prds_or_err = setup_prdt(ps, endpt, iovec, nr_req, size,
		lead, write, prdt);
	if (prds_or_err < 0) {
		return prds_or_err;
	}
	const unsigned int nr_prds = (unsigned int)prds_or_err;

	const int cmd = port_find_cmd(ps);

	if (ps->flags & FLAG_ATAPI) {
		status = atapi_transfer(ps, cmd, start_lba, count, write, prdt,
			nr_prds);
	} else {
		status = ata_transfer(ps, cmd, start_lba, count, write,
			!!(flags & BDEV_FORCEWRITE), prdt, nr_prds);
	}

	if (status != OK) {
		return status;
	}

	return size;
}

/*===========================================================================*
 *				port_hardreset				     *
 *===========================================================================*/
static void port_hardreset(struct port_state *ps)
{
	const unsigned int MICROSECONDS_PER_MILLISECOND = 1000U;

	if (!ps) {
		return;
	}

	port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_INIT);
	micro_delay(COMRESET_DELAY * MICROSECONDS_PER_MILLISECOND);
	port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_NONE);
}

/*===========================================================================*
 *				port_override				     *
 *===========================================================================*/
static void port_override(struct port_state *ps)
{
	/* Override the port's BSY and/or DRQ flags. This may only be done
	 * prior to starting the port.
	 */
	int retries = PORTREG_DELAY;

	port_write(ps, AHCI_PORT_CMD,
		   port_read(ps, AHCI_PORT_CMD) | AHCI_PORT_CMD_CLO);

	while ((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CLO) && (retries > 0)) {
		udelay(1);
		retries--;
	}

	if (retries > 0) {
		dprintf(V_INFO, ("%s: overridden\n", ahci_portname(ps)));
	} else {
		dprintf(V_ERR, ("%s: CLO clear timeout\n", ahci_portname(ps)));
	}
}

/*===========================================================================*
 *				port_start				     *
 *===========================================================================*/
static void port_start(struct port_state *ps)
{
	if (!ps) {
		return;
	}

	port_write(ps, AHCI_PORT_SERR, ~0);
	port_write(ps, AHCI_PORT_IS, ~0);

	port_write(ps, AHCI_PORT_CMD, port_read(ps, AHCI_PORT_CMD) | AHCI_PORT_CMD_ST);

	dprintf(V_INFO, ("%s: started\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_stop				     *
 *===========================================================================*/
static void port_stop(struct port_state *ps)
{
	const u32_t cmd = port_read(ps, AHCI_PORT_CMD);

	if (!(cmd & (AHCI_PORT_CMD_CR | AHCI_PORT_CMD_ST))) {
		return;
	}

	port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_ST);

	if (SPIN_UNTIL(!(port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CR), PORTREG_DELAY)) {
		dprintf(V_ERR, ("%s: port stop command timed out\n", ahci_portname(ps)));
	} else {
		dprintf(V_INFO, ("%s: stopped\n", ahci_portname(ps)));
	}
}

/*===========================================================================*
 *				port_restart				     *
 *===========================================================================*/
static void port_restart(struct port_state *ps)
{
	if (!ps) {
		return;
	}

	port_fail_cmds(ps);
	port_stop(ps);

	const uint32_t port_status = port_read(ps, AHCI_PORT_TFD);
	const uint32_t busy_flags = AHCI_PORT_TFD_STS_BSY | AHCI_PORT_TFD_STS_DRQ;

	if (port_status & busy_flags) {
		dprintf(V_ERR, ("%s: port reset\n", ahci_portname(ps)));
		port_disconnect(ps);
		port_hardreset(ps);
	} else {
		port_start(ps);
	}
}

/*===========================================================================*
 *				print_string				     *
 *===========================================================================*/
static void print_string(u16_t *buf, int start, int end)
{
	if (buf == NULL || start > end) {
		return;
	}

	int last_idx = end;
	const u16_t SPACE_PAIR = (u16_t)((' ' << 8) | ' ');

	while (last_idx >= start && buf[last_idx] == SPACE_PAIR) {
		last_idx--;
	}

	if (last_idx < start) {
		return;
	}

	for (int i = start; i < last_idx; i++) {
		putchar((char)(buf[i] >> 8));
		putchar((char)(buf[i] & 0xFF));
	}

	const u16_t last_word = buf[last_idx];
	putchar((char)(last_word >> 8));

	const char low_byte = (char)(last_word & 0xFF);
	if (low_byte != ' ') {
		putchar(low_byte);
	}
}

/*===========================================================================*
 *				port_id_check				     *
 *===========================================================================*/
static void log_device_info(const struct port_state *ps)
{
	const u16_t *buf = (const u16_t *) ps->tmp_base;

	printf("%s: ATA%s, ", ahci_portname(ps),
		(ps->flags & FLAG_ATAPI) ? "PI" : "");
	print_string(buf, 27, 46);

	if (ahci_verbose >= V_DEV) {
		printf(" (");
		print_string(buf, 10, 19);
		printf(", ");
		print_string(buf, 23, 26);
		printf(")");
	}

	if (ps->flags & FLAG_HAS_MEDIUM) {
		printf(", %u byte sectors, %llu MB size",
			ps->sector_size,
			ps->lba_count * ps->sector_size / (1024 * 1024));
	}

	printf("\n");
}

static void port_id_check(struct port_state *ps, int success)
{
	assert(ps->state == STATE_WAIT_ID);

	ps->flags &= ~FLAG_BUSY;
	cancel_timer(&ps->cmd_info[0].timer);

	int is_device_usable = success;

	if (!is_device_usable) {
		if (!(ps->flags & FLAG_ATAPI) &&
				port_read(ps, AHCI_PORT_SIG) != ATA_SIG_ATA) {
			dprintf(V_INFO, ("%s: may not be ATA, trying ATAPI\n",
				ahci_portname(ps)));

			ps->flags |= FLAG_ATAPI;
			(void) gen_identify(ps, FALSE /*blocking*/);
			return;
		}
		dprintf(V_ERR, ("%s: unable to identify\n", ahci_portname(ps)));
	} else {
		u16_t *buf = (u16_t *) ps->tmp_base;
		if (ps->flags & FLAG_ATAPI) {
			is_device_usable = atapi_id_check(ps, buf);
		} else {
			is_device_usable = ata_id_check(ps, buf);
		}
	}

	if (is_device_usable) {
		ps->state = STATE_GOOD_DEV;
		if (ahci_verbose >= V_INFO) {
			log_device_info(ps);
		}
	} else {
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
	dprintf(V_INFO, ("%s: device connected\n", ahci_portname(ps)));

	port_start(ps);

	const u32_t status = port_read(ps, AHCI_PORT_SSTS) & AHCI_PORT_SSTS_DET_MASK;

	if (status != AHCI_PORT_SSTS_DET_PHY) {
		dprintf(V_ERR, ("%s: device vanished!\n", ahci_portname(ps)));
		port_stop(ps);
		ps->state = STATE_NO_DEV;
		ps->flags &= ~FLAG_BUSY;
		return;
	}

	const u32_t flags_to_preserve = FLAG_BUSY | FLAG_BARRIER | FLAG_SUSPENDED;
	ps->flags &= flags_to_preserve;

	const u32_t sig = port_read(ps, AHCI_PORT_SIG);
	if (sig == ATA_SIG_ATAPI) {
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

	dprintf(V_INFO, ("%s: device disconnected\n", ahci_portname(ps)));

	ps->state = STATE_NO_DEV;
	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);
	ps->flags &= ~FLAG_BUSY;

	port_fail_cmds(ps);

	ps->flags |= FLAG_BARRIER;

	if (ps->device) {
		blockdriver_mt_set_workers(ps->device, 1);
	}
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

	if ((status == AHCI_PORT_SSTS_DET_PHY ||
	    status == AHCI_PORT_SSTS_DET_DET) && ps->left > 0) {
		ps->left--;
		set_timer(&ps->cmd_info[0].timer, ahci_device_delay,
			port_timeout, BUILD_ARG(ps - port_state, 0));
		return;
	}

	dprintf(V_INFO, ("%s: device not ready\n", ahci_portname(ps)));

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
static void handle_phy_ready_change(struct port_state *ps)
{
	port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_X);
	dprintf(V_DEV, ("%s: device attached\n", ahci_portname(ps)));

	if (ps->state == STATE_SPIN_UP) {
		cancel_timer(&ps->cmd_info[0].timer);
	}

	if (ps->state == STATE_SPIN_UP || ps->state == STATE_NO_DEV) {
		ps->state = STATE_WAIT_DEV;
		ps->left = ahci_device_checks;
		port_dev_check(ps);
	} else if (ps->state != STATE_WAIT_DEV) {
		/* Impossible. */
		assert(0);
	}
}

static void handle_phy_reset_change(struct port_state *ps)
{
	port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_N);
	dprintf(V_DEV, ("%s: device detached\n", ahci_portname(ps)));

	if (ps->state == STATE_WAIT_ID || ps->state == STATE_GOOD_DEV) {
		port_stop(ps);
	} else if (ps->state != STATE_BAD_DEV) {
		/* Impossible. */
		assert(0);
		return;
	}

	port_disconnect(ps);
	port_hardreset(ps);
}

static void handle_command_status_change(struct port_state *ps, u32_t smask)
{
	u32_t tfd_status;
	int has_error;

	if (!(smask & AHCI_PORT_IS_MASK)) {
		return;
	}

	tfd_status = port_read(ps, AHCI_PORT_TFD);
	has_error = tfd_status & (AHCI_PORT_TFD_STS_ERR | AHCI_PORT_TFD_STS_DF);

	if (has_error || (smask & AHCI_PORT_IS_RESTART)) {
		port_restart(ps);
	}

	if (ps->state == STATE_WAIT_ID) {
		port_id_check(ps, !has_error);
	}
}

static void port_intr(struct port_state *ps)
{
	/* Process an interrupt on this port. */
	u32_t smask, emask;

	if (ps->state == STATE_NO_PORT) {
		dprintf(V_ERR, ("%s: interrupt for invalid port!\n",
			ahci_portname(ps)));
		return;
	}

	smask = port_read(ps, AHCI_PORT_IS);
	emask = smask & port_read(ps, AHCI_PORT_IE);

	/* Clear the interrupt flags that we saw were set. */
	port_write(ps, AHCI_PORT_IS, smask);

	dprintf(V_REQ, ("%s: interrupt (%08x)\n", ahci_portname(ps), smask));

	/* Check if any commands have completed. */
	port_check_cmds(ps);

	if (emask & AHCI_PORT_IS_PCS) {
		handle_phy_ready_change(ps);
	} else if (emask & AHCI_PORT_IS_PRCS) {
		handle_phy_reset_change(ps);
	} else {
		handle_command_status_change(ps, smask);
	}
}

/*===========================================================================*
 *				port_timeout				     *
 *===========================================================================*/
static void handle_spin_up_timeout(struct port_state *ps)
{
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
}

static void handle_generic_timeout(struct port_state *ps)
{
	dprintf(V_ERR, ("%s: timeout\n", ahci_portname(ps)));

	port_restart(ps);

	if (ps->state == STATE_WAIT_ID) {
		port_id_check(ps, FALSE);
	}
}

static void port_timeout(int arg)
{
	const int port = GET_PORT(arg);

	if (port < 0 || port >= hba_state.nr_ports) {
		return;
	}

	struct port_state *ps = &port_state[port];

	if (ps->flags & FLAG_SUSPENDED) {
		const int cmd = GET_TAG(arg);
		assert(cmd == 0);
		blockdriver_mt_wakeup(ps->cmd_info[0].tid);
	}

	switch (ps->state) {
	case STATE_SPIN_UP:
		handle_spin_up_timeout(ps);
		break;
	case STATE_WAIT_DEV:
		port_dev_check(ps);
		break;
	default:
		handle_generic_timeout(ps);
		break;
	}
}

/*===========================================================================*
 *				port_wait				     *
 *===========================================================================*/
static void port_wait(struct port_state *ps)
{
	__sync_fetch_and_or(&ps->flags, FLAG_SUSPENDED);

	while (*(volatile typeof(ps->flags) *)&ps->flags & FLAG_BUSY) {
		blockdriver_mt_sleep();
	}

	__sync_fetch_and_and(&ps->flags, ~FLAG_SUSPENDED);
}

/*===========================================================================*
 *				port_issue				     *
 *===========================================================================*/
static void port_issue(struct port_state *ps, int cmd, clock_t timeout)
{
	if (!ps || (unsigned int)cmd >= 32) {
		return;
	}

	const uint32_t cmd_mask = 1U << cmd;

	if (ps->flags & FLAG_HAS_NCQ) {
		port_write(ps, AHCI_PORT_SACT, cmd_mask);
	}

	__insn_barrier();

	port_write(ps, AHCI_PORT_CI, cmd_mask);

	ps->pend_mask |= cmd_mask;

	set_timer(&ps->cmd_info[cmd].timer, timeout, port_timeout,
		  &ps->cmd_info[cmd]);
}

/*===========================================================================*
 *				port_exec				     *
 *===========================================================================*/
static int port_exec(struct port_state *ps, int cmd, clock_t timeout)
{
	/* Assuming the type of ps->cmd_info elements is 'struct cmd_info' */
	struct cmd_info *cmd_info = &ps->cmd_info[cmd];
	const char *result_str;
	int ret_status;

	port_issue(ps, cmd, timeout);

	cmd_info->tid = blockdriver_mt_get_tid();

	blockdriver_mt_sleep();

	cancel_timer(&cmd_info->timer);

	assert(!(ps->flags & FLAG_BUSY));

	if (cmd_info->result == RESULT_FAILURE) {
		result_str = "failure";
		ret_status = EIO;
	} else {
		result_str = "success";
		ret_status = OK;
	}

	dprintf(V_REQ, ("%s: end of command -- %s\n", ahci_portname(ps),
		result_str));

	return ret_status;
}

/*===========================================================================*
 *				port_alloc				     *
 *===========================================================================*/
static void port_alloc(struct port_state *ps)
{
	size_t ct_offs[NR_CMDS];
	size_t current_offset = AHCI_CL_SIZE;

	current_offset = (current_offset + AHCI_FIS_SIZE - 1) & ~(AHCI_FIS_SIZE - 1);
	const size_t fis_off = current_offset;
	current_offset += AHCI_FIS_SIZE;

	current_offset = (current_offset + AHCI_TMP_ALIGN - 1) & ~(AHCI_TMP_ALIGN - 1);
	const size_t tmp_off = current_offset;
	current_offset += AHCI_TMP_SIZE;

	for (int i = 0; i < NR_CMDS; i++) {
		current_offset = (current_offset + AHCI_CT_ALIGN - 1) & ~(AHCI_CT_ALIGN - 1);
		ct_offs[i] = current_offset;
		current_offset += AHCI_CT_SIZE;
	}

	ps->mem_size = current_offset;

	ps->mem_base = alloc_contig(ps->mem_size, AC_ALIGN4K, &ps->mem_phys);
	if (ps->mem_base == NULL) {
		panic("unable to allocate port memory");
	}
	memset(ps->mem_base, 0, ps->mem_size);

	char * const base_ptr = ps->mem_base;

	ps->cl_base = (u32_t *) base_ptr;
	ps->cl_phys = ps->mem_phys;
	assert((ps->cl_phys % AHCI_CL_SIZE) == 0);

	ps->fis_base = (u32_t *) (base_ptr + fis_off);
	ps->fis_phys = ps->mem_phys + fis_off;
	assert((ps->fis_phys % AHCI_FIS_SIZE) == 0);

	ps->tmp_base = (u8_t *) (base_ptr + tmp_off);
	ps->tmp_phys = ps->mem_phys + tmp_off;
	assert((ps->tmp_phys % AHCI_TMP_ALIGN) == 0);

	for (int i = 0; i < NR_CMDS; i++) {
		ps->ct_base[i] = base_ptr + ct_offs[i];
		ps->ct_phys[i] = ps->mem_phys + ct_offs[i];
		assert((ps->ct_phys[i] % AHCI_CT_ALIGN) == 0);
	}

	port_write(ps, AHCI_PORT_FBU, 0);
	port_write(ps, AHCI_PORT_FB, ps->fis_phys);

	port_write(ps, AHCI_PORT_CLBU, 0);
	port_write(ps, AHCI_PORT_CLB, ps->cl_phys);

	const u32_t cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_FRE);

	ps->pad_base = NULL;
	ps->pad_size = 0;
}

/*===========================================================================*
 *				port_free				     *
 *===========================================================================*/
static void port_free(struct port_state *ps)
{
	const u32_t cmd = port_read(ps, AHCI_PORT_CMD);
	const bool fis_is_active = (cmd & (AHCI_PORT_CMD_FR | AHCI_PORT_CMD_FRE));

	if (fis_is_active) {
		port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_FRE);

		SPIN_UNTIL(!(port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_FR),
			   PORTREG_DELAY);
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
	const int port_index = ps - port_state;

	ps->queue_depth = 1;
	ps->state = STATE_SPIN_UP;
	ps->flags = FLAG_BUSY;
	ps->sector_size = 0;
	ps->open_count = 0;
	ps->pend_mask = 0;

	for (int i = 0; i < NR_CMDS; i++) {
		init_timer(&ps->cmd_info[i].timer);
	}

	ps->reg = (u32_t *)((uintptr_t)hba_state.base +
		AHCI_MEM_BASE_SIZE + ((size_t)port_index * AHCI_MEM_PORT_SIZE));

	port_alloc(ps);

	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);

	u32_t cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_SUD);

	port_hardreset(ps);

	set_timer(&ps->cmd_info[0].timer, ahci_spinup_timeout,
		port_timeout, BUILD_ARG(port_index, 0));
}

/*===========================================================================*
 *				ahci_probe				     *
 *===========================================================================*/
static int ahci_probe(int skip)
{
	int devind;
	u16_t vid, did;

	if (skip < 0) {
		return -1;
	}

	pci_init();

	if (pci_first_dev(&devind, &vid, &did) <= 0) {
		return -1;
	}

	for (int i = 0; i < skip; i++) {
		if (pci_next_dev(&devind, &vid, &did) <= 0) {
			return -1;
		}
	}

	pci_reserve(devind);

	return devind;
}

/*===========================================================================*
 *				ahci_reset				     *
 *===========================================================================*/
static inline bool hba_is_resetting(void)
{
	return (hba_read(AHCI_HBA_GHC) & AHCI_HBA_GHC_HR) != 0;
}

static void ahci_reset(void)
{
	const u32_t ghc = hba_read(AHCI_HBA_GHC);

	hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE);
	hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE | AHCI_HBA_GHC_HR);

	SPIN_UNTIL(!hba_is_resetting(), RESET_DELAY);

	if (hba_is_resetting()) {
		panic("unable to reset HBA");
	}
}

/*===========================================================================*
 *				ahci_init				     *
 *===========================================================================*/
static void ahci_map_bar(int devind)
{
	u32_t base, size;
	int r, ioflag;

	if ((r = pci_get_bar(devind, PCI_BAR_6, &base, &size, &ioflag)) != OK)
		panic("unable to retrieve BAR: %d", r);

	if (ioflag)
		panic("invalid BAR type");

	if (size < AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE)
		panic("HBA memory size too small: %u", size);

	const u32_t mapped_size = MIN(size, AHCI_MEM_BASE_SIZE +
		AHCI_MEM_PORT_SIZE * NR_PORTS);

	hba_state.nr_ports = (mapped_size - AHCI_MEM_BASE_SIZE) /
		AHCI_MEM_PORT_SIZE;

	hba_state.base = (u32_t *) vm_map_phys(SELF, (void *) base, mapped_size);
	hba_state.size = mapped_size;

	if (hba_state.base == MAP_FAILED)
		panic("unable to map HBA memory");
}

static void ahci_setup_interrupts(int devind)
{
	int r;

	hba_state.irq = pci_attr_r8(devind, PCI_ILR);
	hba_state.hook_id = 0;

	if ((r = sys_irqsetpolicy(hba_state.irq, 0, &hba_state.hook_id)) != OK)
		panic("unable to register IRQ: %d", r);

	if ((r = sys_irqenable(&hba_state.hook_id)) != OK)
		panic("unable to enable IRQ: %d", r);
}

static void ahci_configure_from_caps(void)
{
	const u32_t cap = hba_read(AHCI_HBA_CAP);
	const u32_t vs = hba_read(AHCI_HBA_VS);

	hba_state.has_ncq = !!(cap & AHCI_HBA_CAP_SNCQ);
	hba_state.has_clo = !!(cap & AHCI_HBA_CAP_SCLO);

	const unsigned int nr_cmds_from_cap =
		((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1;
	hba_state.nr_cmds = MIN(NR_CMDS, nr_cmds_from_cap);

	const unsigned int cap_np =
		((cap >> AHCI_HBA_CAP_NP_SHIFT) & AHCI_HBA_CAP_NP_MASK) + 1;

	dprintf(V_INFO, ("AHCI%u: HBA v%u.%u.%u, %u ports, %u commands, "
		"%s queuing, IRQ %d\n",
		ahci_instance,
		(unsigned int)(vs >> 16),
		(unsigned int)((vs >> 8) & 0xFF),
		(unsigned int)(vs & 0xFF),
		cap_np,
		nr_cmds_from_cap,
		hba_state.has_ncq ? "supports" : "no", hba_state.irq));

	dprintf(V_INFO, ("AHCI%u: CAP %08x, CAP2 %08x, PI %08x\n",
		ahci_instance, cap, hba_read(AHCI_HBA_CAP2),
		hba_read(AHCI_HBA_PI)));
}

static void ahci_initialize_ports(void)
{
	const u32_t implemented_ports = hba_read(AHCI_HBA_PI);

	for (int port = 0; port < hba_state.nr_ports; port++) {
		port_state[port].device = NO_DEVICE;
		port_state[port].state = STATE_NO_PORT;

		if (implemented_ports & (1U << port)) {
			port_init(&port_state[port]);
		}
	}
}

static void ahci_init(int devind)
{
	ahci_map_bar(devind);
	ahci_setup_interrupts(devind);

	ahci_reset();

	/* Enable AHCI and interrupts. */
	const u32_t ghc = hba_read(AHCI_HBA_GHC);
	hba_write(AHCI_HBA_GHC, ghc | AHCI_HBA_GHC_AE | AHCI_HBA_GHC_IE);

	ahci_configure_from_caps();
	ahci_initialize_ports();
}

/*===========================================================================*
 *				ahci_stop				     *
 *===========================================================================*/
static void ahci_stop(void)
{
	for (int port = 0; port < hba_state.nr_ports; port++) {
		struct port_state *ps = &port_state[port];

		if (ps->state != STATE_NO_PORT) {
			port_stop(ps);
			port_free(ps);
		}
	}

	ahci_reset();

	int r = vm_unmap_phys(SELF, (void *)hba_state.base, hba_state.size);
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
static void ahci_intr(unsigned int UNUSED(mask))
{
	const u32_t pending_is = hba_read(AHCI_HBA_IS);

	for (int port = 0; port < hba_state.nr_ports; port++) {
		if (pending_is & (1U << port)) {
			struct port_state *ps = &port_state[port];

			port_intr(ps);

			if ((ps->flags & (FLAG_SUSPENDED | FLAG_BUSY)) ==
			    FLAG_SUSPENDED) {
				blockdriver_mt_wakeup(ps->cmd_info[0].tid);
			}
		}
	}

	hba_write(AHCI_HBA_IS, pending_is);

	const int r = sys_irqenable(&hba_state.hook_id);
	if (r != OK) {
		panic("unable to enable IRQ: %d", r);
	}
}

/*===========================================================================*
 *				ahci_get_params				     *
 *===========================================================================*/
static void ahci_get_params(void)
{
	long instance = 0;
	(void)env_parse("instance", "d", 0, &instance, 0, 255);
	ahci_instance = (int)instance;

	long verbosity = V_ERR;
	(void)env_parse("ahci_verbose", "d", 0, &verbosity, V_NONE, V_REQ);
	ahci_verbose = (int)verbosity;

	const size_t timevar_count = sizeof(ahci_timevar) / sizeof(ahci_timevar[0]);
	for (size_t i = 0; i < timevar_count; i++) {
		long timeout_ms = ahci_timevar[i].default_ms;
		(void)env_parse(ahci_timevar[i].name, "d", 0, &timeout_ms, 1,
			LONG_MAX);
		*ahci_timevar[i].ptr = millis_to_hz(timeout_ms);
	}

	ahci_device_delay = millis_to_hz(DEVICE_DELAY);
	if (ahci_device_delay > 0) {
		ahci_device_checks = (ahci_device_timeout + ahci_device_delay - 1) /
			ahci_device_delay;
	} else {
		ahci_device_delay = 1;
		ahci_device_checks = ahci_device_timeout;
	}
}

/*===========================================================================*
 *				ahci_set_mapping			     *
 *===========================================================================*/
static void ahci_set_mapping(void)
{
	char key[16];
	char val[32];
	int drive_idx = 0;

	for (int port_idx = 0; port_idx < NR_PORTS && drive_idx < MAX_DRIVES; port_idx++) {
		if (port_state[port_idx].state != STATE_NO_PORT) {
			ahci_map[drive_idx++] = port_idx;
		}
	}
	for (; drive_idx < MAX_DRIVES; drive_idx++) {
		ahci_map[drive_idx] = NO_PORT;
	}

	snprintf(key, sizeof(key), "ahci%u_map", ahci_instance);
	if (env_get_param(key, val, sizeof(val)) == OK) {
		char *p = val;
		for (int i = 0; i < MAX_DRIVES && *p != '\0'; i++) {
			char *endp;
			unsigned long port_val = strtoul(p, &endp, 0);

			if (p != endp && port_val < NR_PORTS) {
				ahci_map[i] = (unsigned int)port_val;
			} else {
				ahci_map[i] = NO_PORT;
			}

			p = endp;
			if (*p == ',') {
				p++;
			}
		}
	}

	for (int i = 0; i < MAX_DRIVES; i++) {
		unsigned int port_num = ahci_map[i];
		if (port_num != NO_PORT) {
			port_state[port_num].device = i;
		}
	}
}

/*===========================================================================*
 *				sef_cb_init_fresh			     *
 *===========================================================================*/
static int sef_cb_init_fresh(int type, sef_init_info_t *UNUSED(info))
{
    int r;
    int devind;

    ahci_get_params();

    devind = ahci_probe(ahci_instance);
    if (devind < 0) {
        return ENXIO;
    }

    r = ahci_init(devind);
    if (r != OK) {
        return r;
    }

    r = ahci_set_mapping();
    if (r != OK) {
        return r;
    }

    blockdriver_announce(type);

    return OK;
}

/*===========================================================================*
 *				sef_cb_signal_handler			     *
 *===========================================================================*/
static int are_any_ports_open(void)
{
	for (int port = 0; port < hba_state.nr_ports; port++) {
		if (port_state[port].open_count > 0) {
			return 1;
		}
	}
	return 0;
}

static void sef_cb_signal_handler(int signo)
{
	if (signo != SIGTERM) {
		return;
	}

	ahci_exiting = TRUE;

	if (are_any_ports_open()) {
		return;
	}

	ahci_stop();
	exit(0);
}

/*===========================================================================*
 *				sef_local_startup			     *
 *===========================================================================*/
static void sef_local_startup(void)
{
	/* Set callbacks and initialize the System Event Framework (SEF). */
	sef_setcb_init_fresh(sef_cb_init_fresh);
	sef_setcb_signal_handler(sef_cb_signal_handler);
	blockdriver_mt_support_lu();
	sef_startup();
}

/*===========================================================================*
 *				ahci_portname				     *
 *===========================================================================*/
#include <stdio.h>
#include <stddef.h>

static char *ahci_portname(struct port_state *ps)
{
	static char name_buffer[16];
	const ptrdiff_t port_index = ps - port_state;

	if (ps->device == NO_DEVICE) {
		snprintf(name_buffer, sizeof(name_buffer), "AHCI%d-P%02td",
			 ahci_instance, port_index);
	} else {
		snprintf(name_buffer, sizeof(name_buffer), "AHCI%d-D%d",
			 ahci_instance, ps->device);
	}

	return name_buffer;
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

	devminor_t sub_minor = minor - MINOR_d0p0s0;
	if ((unsigned)sub_minor < NR_SUBDEVS) {
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
	struct device *dv;

	return (ahci_map_minor(minor, &dv) != NULL) ? dv : NULL;
}

/*===========================================================================*
 *				ahci_open				     *
 *===========================================================================*/
static int ahci_open(devminor_t minor, int access)
{
	struct port_state *ps = ahci_get_port(minor);

	if (ps == NULL) {
		return ENXIO;
	}

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

	if ((ps->open_count > 0) && (ps->flags & FLAG_BARRIER)) {
		return ENXIO;
	}

	if (ps->open_count == 0) {
		ps->flags &= ~FLAG_BARRIER;

		if (ps->flags & FLAG_ATAPI) {
			int r = atapi_check_medium(ps, 0);
			if (r != OK) {
				return r;
			}
		}

		memset(ps->part, 0, sizeof(ps->part));
		memset(ps->subpart, 0, sizeof(ps->subpart));

		ps->part[0].dv_size = ps->lba_count * ps->sector_size;

		partition(&ahci_dtab, ps->device * DEV_PER_DRIVE, P_PRIMARY,
			(ps->flags & FLAG_ATAPI) != 0);

		blockdriver_mt_set_workers(ps->device, ps->queue_depth);
	}

	ps->open_count++;

	return OK;
}

/*===========================================================================*
 *				ahci_close				     *
 *===========================================================================*/
static int are_all_ports_closed(void)
{
	for (int port = 0; port < hba_state.nr_ports; port++) {
		if (port_state[port].open_count > 0) {
			return 0;
		}
	}
	return 1;
}

static int ahci_close(devminor_t minor)
{
	struct port_state *ps = ahci_get_port(minor);

	if (ps == NULL) {
		dprintf(V_ERR, ("ahci_close: invalid minor device %d\n", minor));
		return EINVAL;
	}

	if (ps->open_count <= 0) {
		dprintf(V_ERR, ("%s: closing already-closed port\n",
			ahci_portname(ps)));
		return EINVAL;
	}

	ps->open_count--;

	if (ps->open_count > 0) {
		return OK;
	}

	blockdriver_mt_set_workers(ps->device, 1);

	if (ps->state == STATE_GOOD_DEV && !(ps->flags & FLAG_BARRIER)) {
		dprintf(V_INFO, ("%s: flushing write cache\n",
			ahci_portname(ps)));
		(void) gen_flush_wcache(ps);
	}

	if (ahci_exiting && are_all_ports_closed()) {
		ahci_stop();
		blockdriver_mt_terminate();
	}

	return OK;
}

/*===========================================================================*
 *				ahci_transfer				     *
 *===========================================================================*/
static ssize_t ahci_transfer(devminor_t minor, int do_write, u64_t position,
	endpoint_t endpt, iovec_t *iovec, unsigned int count, int flags)
{
	struct port_state *ps = ahci_get_port(minor);
	if (ps == NULL) {
		return ENXIO;
	}

	struct device *dv = ahci_part(minor);
	if (dv == NULL) {
		return ENXIO;
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

	if (dv->dv_base > UINT64_MAX - dv->dv_size) {
		return EIO;
	}

	const u64_t pos = dv->dv_base + position;
	const u64_t eof = dv->dv_base + dv->dv_size;

	return port_transfer(ps, pos, eof, endpt, (iovec_s_t *)iovec, count,
		do_write, flags);
}

/*===========================================================================*
 *				ahci_ioctl				     *
 *===========================================================================*/
static int ahci_ioctl(devminor_t minor, unsigned long request,
	endpoint_t endpt, cp_grant_id_t grant, endpoint_t UNUSED(user_endpt))
{
	struct port_state *ps = ahci_get_port(minor);

	if (ps == NULL) {
		return ENXIO;
	}

	if (request == DIOCOPENCT) {
		return sys_safecopyto(endpt, grant, 0,
			(vir_bytes) &ps->open_count, sizeof(ps->open_count));
	}

	if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER)) {
		return EIO;
	}

	switch (request) {
	case DIOCEJECT:
		if (!(ps->flags & FLAG_ATAPI)) {
			return EINVAL;
		}
		return atapi_load_eject(ps, 0, FALSE /*load*/);

	case DIOCFLUSH:
		return gen_flush_wcache(ps);

	case DIOCSETWC: {
		int val;
		int r = sys_safecopyfrom(endpt, grant, 0, (vir_bytes) &val,
			sizeof(val));
		if (r != OK) {
			return r;
		}
		return gen_set_wcache(ps, val);
	}

	case DIOCGETWC: {
		int val;
		int r = gen_get_wcache(ps, &val);
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
	/* Map a minor device number to a device ID.
	 */
	struct port_state *ps;

	if (id == NULL) {
		return EINVAL;
	}

	ps = ahci_map_minor(minor, NULL);
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
	struct device *dv;
	struct port_state * const port = ahci_map_minor(minor, &dv);

	if (port == NULL) {
		panic("AHCI: device mapping for minor %d disappeared", minor);
	}

	return port;
}

/*===========================================================================*
 *				main					     *
 *===========================================================================*/
#include <stdio.h>
#include <stdlib.h>

/* Forward declarations for functions and data from other modules */
extern void env_setargs(int argc, char *argv[]);
extern int sef_local_startup(void);
extern int blockdriver_mt_task(void *driver_data);
extern struct blockdriver ahci_dtab;

int main(int argc, char *argv[])
{
    int status;

    env_setargs(argc, argv);

    status = sef_local_startup();
    if (status != 0) {
        fprintf(stderr, "sef_local_startup failed with status: %d\n", status);
        return EXIT_FAILURE;
    }

    status = blockdriver_mt_task(&ahci_dtab);
    if (status != 0) {
        fprintf(stderr, "blockdriver_mt_task failed with status: %d\n", status);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
