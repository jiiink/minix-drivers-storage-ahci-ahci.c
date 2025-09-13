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

	if (ps == NULL || packet == NULL) {
		return EINVAL;
	}

	if (size > AHCI_TMP_SIZE) {
		return EINVAL;
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
	u8_t packet[ATAPI_PACKET_SIZE] = {0};
	
	if (ps == NULL) {
		return -1;
	}
	
	packet[0] = ATAPI_CMD_TEST_UNIT;
	
	return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_request_sense			     *
 *===========================================================================*/
static int atapi_request_sense(struct port_state *ps, int cmd, int *sense)
{
	u8_t packet[ATAPI_PACKET_SIZE];
	int r;

	if (ps == NULL || sense == NULL) {
		return EINVAL;
	}

	memset(packet, 0, sizeof(packet));
	packet[0] = ATAPI_CMD_REQUEST_SENSE;
	packet[4] = ATAPI_REQUEST_SENSE_LEN;

	r = atapi_exec(ps, cmd, packet, ATAPI_REQUEST_SENSE_LEN, FALSE);
	if (r != OK) {
		return r;
	}

	dprintf(V_REQ, ("%s: ATAPI SENSE: sense %x ASC %x ASCQ %x\n",
		ahci_portname(ps), ps->tmp_base[2] & 0xF, ps->tmp_base[12],
		ps->tmp_base[13]));

	*sense = ps->tmp_base[2] & 0xF;

	return OK;
}

/*===========================================================================*
 *				atapi_load_eject			     *
 *===========================================================================*/
static int atapi_load_eject(struct port_state *ps, int cmd, int load)
{
	u8_t packet[ATAPI_PACKET_SIZE] = {0};

	if (ps == NULL) {
		return -1;
	}

	packet[0] = ATAPI_CMD_START_STOP;
	packet[4] = (load != 0) ? ATAPI_START_STOP_LOAD : ATAPI_START_STOP_EJECT;

	return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_read_capacity			     *
 *===========================================================================*/
static int atapi_read_capacity(struct port_state *ps, int cmd)
{
	u8_t packet[ATAPI_PACKET_SIZE];
	u8_t *buf;
	u32_t lba_blocks;
	u32_t sector_size;
	int r;

	memset(packet, 0, sizeof(packet));
	packet[0] = ATAPI_CMD_READ_CAPACITY;

	r = atapi_exec(ps, cmd, packet, ATAPI_READ_CAPACITY_LEN, FALSE);
	if (r != OK) {
		return r;
	}

	buf = ps->tmp_base;
	
	lba_blocks = ((u32_t)buf[0] << 24) | 
	             ((u32_t)buf[1] << 16) | 
	             ((u32_t)buf[2] << 8) | 
	             (u32_t)buf[3];
	
	sector_size = ((u32_t)buf[4] << 24) | 
	              ((u32_t)buf[5] << 16) | 
	              ((u32_t)buf[6] << 8) | 
	              (u32_t)buf[7];

	if (sector_size == 0 || (sector_size & 1)) {
		dprintf(V_ERR, ("%s: invalid medium sector size %u\n",
			ahci_portname(ps), sector_size));
		return EINVAL;
	}

	ps->lba_count = (u64_t)lba_blocks + 1;
	ps->sector_size = sector_size;

	dprintf(V_INFO,
		("%s: medium detected (%u byte sectors, %llu MB size)\n",
		ahci_portname(ps), ps->sector_size,
		ps->lba_count * ps->sector_size / (1024*1024)));

	return OK;
}

/*===========================================================================*
 *				atapi_check_medium			     *
 *===========================================================================*/
static int atapi_check_medium(struct port_state *ps, int cmd)
{
	int sense;
	int test_result;
	int sense_result;
	int capacity_result;

	if (ps == NULL) {
		return EINVAL;
	}

	test_result = atapi_test_unit(ps, cmd);
	
	if (test_result != OK) {
		ps->flags &= ~FLAG_HAS_MEDIUM;

		sense_result = atapi_request_sense(ps, cmd, &sense);
		if (sense_result != OK) {
			return ENXIO;
		}
		
		if (sense != ATAPI_SENSE_UNIT_ATT) {
			return ENXIO;
		}
	}

	if ((ps->flags & FLAG_HAS_MEDIUM) == 0) {
		capacity_result = atapi_read_capacity(ps, cmd);
		if (capacity_result != OK) {
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
	u16_t gcap;
	u16_t cap;
	u16_t dmadir;
	u16_t device_type;
	int is_atapi_removable;
	int has_dma_support;

	if (!ps || !buf) {
		return FALSE;
	}

	gcap = buf[ATA_ID_GCAP];
	cap = buf[ATA_ID_CAP];
	dmadir = buf[ATA_ID_DMADIR];

	is_atapi_removable = ((gcap & (ATA_ID_GCAP_ATAPI_MASK | ATA_ID_GCAP_REMOVABLE | ATA_ID_GCAP_INCOMPLETE)) ==
		(ATA_ID_GCAP_ATAPI | ATA_ID_GCAP_REMOVABLE));

	has_dma_support = ((cap & ATA_ID_CAP_DMA) == ATA_ID_CAP_DMA) ||
		((dmadir & (ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA)) == 
		(ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA));

	if (!is_atapi_removable || !has_dma_support) {
		dprintf(V_ERR, ("%s: unsupported ATAPI device\n", ahci_portname(ps)));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x DMADIR %04x\n",
			ahci_portname(ps), gcap, cap, dmadir));
		return FALSE;
	}

	if (dmadir & ATA_ID_DMADIR_DMADIR) {
		ps->flags |= FLAG_USE_DMADIR;
	}

	device_type = (gcap & ATA_ID_GCAP_TYPE_MASK) >> ATA_ID_GCAP_TYPE_SHIFT;
	if (device_type == ATAPI_TYPE_CDROM) {
		ps->flags |= FLAG_READONLY;
	}

	if ((buf[ATA_ID_SUP1] & ATA_ID_SUP1_VALID_MASK) == ATA_ID_SUP1_VALID) {
		if ((ps->flags & FLAG_READONLY) == 0) {
			if (buf[ATA_ID_SUP0] & ATA_ID_SUP0_WCACHE) {
				ps->flags |= FLAG_HAS_WCACHE;
			}
			if (buf[ATA_ID_SUP1] & ATA_ID_SUP1_FLUSH) {
				ps->flags |= FLAG_HAS_FLUSH;
			}
		}
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

	if (ps == NULL || prdt == NULL) {
		return -1;
	}

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
	packet[4] = (u8_t)((start_lba >> 8) & 0xFF);
	packet[5] = (u8_t)(start_lba & 0xFF);
	packet[6] = (u8_t)((count >> 24) & 0xFF);
	packet[7] = (u8_t)((count >> 16) & 0xFF);
	packet[8] = (u8_t)((count >> 8) & 0xFF);
	packet[9] = (u8_t)(count & 0xFF);

	port_set_cmd(ps, cmd, &fis, packet, prdt, nr_prds, write);

	return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				ata_id_check				     *
 *===========================================================================*/
static int ata_id_check(struct port_state *ps, u16_t *buf)
{
	u16_t gcap_mask = ATA_ID_GCAP_ATA_MASK | ATA_ID_GCAP_REMOVABLE | ATA_ID_GCAP_INCOMPLETE;
	u16_t cap_mask = ATA_ID_CAP_LBA | ATA_ID_CAP_DMA;
	u16_t sup1_mask = ATA_ID_SUP1_VALID_MASK | ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48;
	u16_t sup1_required = ATA_ID_SUP1_VALID | ATA_ID_SUP1_FLUSH | ATA_ID_SUP1_LBA48;
	
	if ((buf[ATA_ID_GCAP] & gcap_mask) != ATA_ID_GCAP_ATA ||
	    (buf[ATA_ID_CAP] & cap_mask) != cap_mask ||
	    (buf[ATA_ID_SUP1] & sup1_mask) != sup1_required) {
		
		dprintf(V_ERR, ("%s: unsupported ATA device\n", ahci_portname(ps)));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x SUP1 %04x\n",
			ahci_portname(ps), buf[ATA_ID_GCAP], buf[ATA_ID_CAP],
			buf[ATA_ID_SUP1]));
		return FALSE;
	}

	ps->lba_count = ((u64_t)buf[ATA_ID_LBA3] << 48) |
	                ((u64_t)buf[ATA_ID_LBA2] << 32) |
	                ((u64_t)buf[ATA_ID_LBA1] << 16) |
	                ((u64_t)buf[ATA_ID_LBA0]);

	if (hba_state.has_ncq && (buf[ATA_ID_SATA_CAP] & ATA_ID_SATA_CAP_NCQ)) {
		ps->flags |= FLAG_HAS_NCQ;
		ps->queue_depth = (buf[ATA_ID_QDEPTH] & ATA_ID_QDEPTH_MASK) + 1;
		if (ps->queue_depth > hba_state.nr_cmds) {
			ps->queue_depth = hba_state.nr_cmds;
		}
	}

	if ((buf[ATA_ID_PLSS] & (ATA_ID_PLSS_VALID_MASK | ATA_ID_PLSS_LLS)) ==
	    (ATA_ID_PLSS_VALID | ATA_ID_PLSS_LLS)) {
		ps->sector_size = ((buf[ATA_ID_LSS1] << 16) | buf[ATA_ID_LSS0]) << 1;
	} else {
		ps->sector_size = ATA_SECTOR_SIZE;
	}

	if (ps->sector_size < ATA_SECTOR_SIZE) {
		dprintf(V_ERR, ("%s: invalid sector size %u\n",
			ahci_portname(ps), ps->sector_size));
		return FALSE;
	}

	ps->flags |= FLAG_HAS_MEDIUM | FLAG_HAS_FLUSH;

	if (buf[ATA_ID_SUP0] & ATA_ID_SUP0_WCACHE) {
		ps->flags |= FLAG_HAS_WCACHE;
	}

	if ((buf[ATA_ID_ENA2] & (ATA_ID_ENA2_VALID_MASK | ATA_ID_ENA2_FUA)) ==
	    (ATA_ID_ENA2_VALID | ATA_ID_ENA2_FUA)) {
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

	if (ps == NULL || prdt == NULL) {
		return -1;
	}

	if (count > ATA_MAX_SECTORS) {
		return -1;
	}

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

	if (ps == NULL) {
		return EINVAL;
	}

	memset(&fis, 0, sizeof(fis));

	fis.cf_cmd = (ps->flags & FLAG_ATAPI) ? 
		ATA_CMD_IDENTIFY_PACKET : ATA_CMD_IDENTIFY;

	prd.vp_addr = ps->tmp_phys;
	prd.vp_size = ATA_ID_SIZE;

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
	cmd_fis_t fis;

	if (!(ps->flags & FLAG_HAS_FLUSH))
		return EINVAL;

	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_FLUSH_CACHE;

	port_set_cmd(ps, 0, &fis, NULL, NULL, 0, FALSE);

	return port_exec(ps, 0, ahci_flush_timeout);
}

/*===========================================================================*
 *				gen_get_wcache				     *
 *===========================================================================*/
static int gen_get_wcache(struct port_state *ps, int *val)
{
	int r;
	u16_t *id_data;

	if (ps == NULL || val == NULL)
		return EINVAL;

	if (!(ps->flags & FLAG_HAS_WCACHE))
		return EINVAL;

	r = gen_identify(ps, TRUE);
	if (r != OK)
		return r;

	id_data = (u16_t *)ps->tmp_base;
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

	if (ps == NULL) {
		return EINVAL;
	}

	if (!(ps->flags & FLAG_HAS_WCACHE)) {
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
static vir_bytes ct_set_fis(u8_t *ct, cmd_fis_t *fis, unsigned int tag)
{
	if (ct == NULL || fis == NULL) {
		return 0;
	}

	memset(ct, 0, ATA_H2D_SIZE);
	
	ct[ATA_FIS_TYPE] = ATA_FIS_TYPE_H2D;
	ct[ATA_H2D_FLAGS] = ATA_H2D_FLAGS_C;
	ct[ATA_H2D_CMD] = fis->cf_cmd;
	ct[ATA_H2D_DEV] = fis->cf_dev;
	ct[ATA_H2D_CTL] = fis->cf_ctl;
	
	ct[ATA_H2D_LBA_LOW] = (u8_t)(fis->cf_lba & 0xFF);
	ct[ATA_H2D_LBA_MID] = (u8_t)((fis->cf_lba >> 8) & 0xFF);
	ct[ATA_H2D_LBA_HIGH] = (u8_t)((fis->cf_lba >> 16) & 0xFF);
	
	ct[ATA_H2D_LBA_LOW_EXP] = (u8_t)(fis->cf_lba_exp & 0xFF);
	ct[ATA_H2D_LBA_MID_EXP] = (u8_t)((fis->cf_lba_exp >> 8) & 0xFF);
	ct[ATA_H2D_LBA_HIGH_EXP] = (u8_t)((fis->cf_lba_exp >> 16) & 0xFF);

	if (ATA_IS_FPDMA_CMD(fis->cf_cmd)) {
		ct[ATA_H2D_FEAT] = fis->cf_sec;
		ct[ATA_H2D_FEAT_EXP] = fis->cf_sec_exp;
		ct[ATA_H2D_SEC] = (u8_t)(tag << ATA_SEC_TAG_SHIFT);
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
static void ct_set_packet(u8_t *ct, const u8_t packet[ATAPI_PACKET_SIZE])
{
	if (ct == NULL || packet == NULL) {
		return;
	}
	
	memcpy(&ct[AHCI_CT_PACKET_OFF], packet, ATAPI_PACKET_SIZE);
}

/*===========================================================================*
 *				ct_set_prdt				     *
 *===========================================================================*/
static void ct_set_prdt(u8_t *ct, prd_t *prdt, int nr_prds)
{
	u32_t *p;
	int i;

	if (ct == NULL || prdt == NULL || nr_prds <= 0) {
		return;
	}

	p = (u32_t *)&ct[AHCI_CT_PRDT_OFF];

	for (i = 0; i < nr_prds; i++) {
		p[i * 4] = prdt[i].vp_addr;
		p[i * 4 + 1] = 0;
		p[i * 4 + 2] = 0;
		p[i * 4 + 3] = prdt[i].vp_size - 1;
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

	if (ps == NULL || fis == NULL) {
		return;
	}

	if (cmd < 0 || nr_prds < 0 || nr_prds > NR_PRDS) {
		return;
	}

	if (ATA_IS_FPDMA_CMD(fis->cf_cmd)) {
		ps->flags |= FLAG_NCQ_MODE;
	} else {
		assert(!ps->pend_mask);
		ps->flags &= ~FLAG_NCQ_MODE;
	}

	ct = ps->ct_base[cmd];
	if (ct == NULL) {
		return;
	}

	size = ct_set_fis(ct, fis, cmd);

	if (packet != NULL) {
		ct_set_packet(ct, packet);
	}

	ct_set_prdt(ct, prdt, nr_prds);

	cl = &ps->cl_base[cmd * AHCI_CL_ENTRY_DWORDS];
	memset(cl, 0, AHCI_CL_ENTRY_SIZE);

	u32_t cl_flags = 0;
	
	cl_flags |= (nr_prds << AHCI_CL_PRDTL_SHIFT);
	cl_flags |= ((size / sizeof(u32_t)) << AHCI_CL_CFL_SHIFT);
	
	if (write) {
		cl_flags |= AHCI_CL_WRITE;
	}
	
	if (packet != NULL) {
		cl_flags |= AHCI_CL_ATAPI;
	}
	
	if (!ATA_IS_FPDMA_CMD(fis->cf_cmd) && (nr_prds > 0 || packet != NULL)) {
		cl_flags |= AHCI_CL_PREFETCHABLE;
	}
	
	cl[0] = cl_flags;
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

    if ((ps->pend_mask & (1U << cmd)) == 0) {
        return;
    }

    const char *status_msg = (result == RESULT_SUCCESS) ? "succeeded" : "failed";
    dprintf(V_REQ, ("%s: command %d %s\n", ahci_portname(ps), cmd, status_msg));

    ps->cmd_info[cmd].result = result;
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
	if (ps == NULL) {
		return;
	}

	for (int i = 0; i < ps->queue_depth && ps->pend_mask != 0; i++) {
		unsigned int mask = 1U << i;
		if ((ps->pend_mask & mask) != 0) {
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
	u32_t done;
	int i;

	if (ps == NULL) {
		return;
	}

	if ((ps->flags & FLAG_NCQ_MODE) != 0) {
		mask = port_read(ps, AHCI_PORT_SACT);
	} else {
		mask = port_read(ps, AHCI_PORT_CI);
	}

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
	if (ps == NULL) {
		return -1;
	}

	for (int i = 0; i < ps->queue_depth; i++) {
		if ((ps->pend_mask & (1U << i)) == 0) {
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
	if (ps->pad_base != NULL) {
		if (ps->pad_size >= size)
			return OK;
		
		free_contig(ps->pad_base, ps->pad_size);
		ps->pad_base = NULL;
	}

	ps->pad_size = size;
	ps->pad_base = alloc_contig(ps->pad_size, 0, &ps->pad_phys);

	if (ps->pad_base == NULL) {
		ps->pad_size = 0;
		dprintf(V_ERR, ("%s: unable to allocate a padding buffer of "
			"size %lu\n", ahci_portname(ps),
			(unsigned long) size));
		return ENOMEM;
	}

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
	vir_bytes size, bytes;
	int i;

	if (ps == NULL || iovec == NULL || total == NULL || nr_req <= 0) {
		return EINVAL;
	}

	bytes = 0;

	for (i = 0; i < nr_req; i++) {
		size = iovec[i].iov_size;

		if (size == 0 || (size & 1) || size > LONG_MAX) {
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
	struct vumap_vir vvec[NR_PRDS];
	size_t bytes, trail;
	int i, r, pcount, nr_prds = 0;

	if (ps == NULL || prdt == NULL || (nr_req > 0 && iovec == NULL)) {
		return EINVAL;
	}

	if (lead > 0) {
		r = port_get_padbuf(ps, ps->sector_size);
		if (r != OK) {
			return r;
		}

		prdt[nr_prds].vp_addr = ps->pad_phys;
		prdt[nr_prds].vp_size = lead;
		nr_prds++;
	}

	trail = (ps->sector_size - (lead + size)) % ps->sector_size;

	for (i = 0; i < nr_req && size > 0 && i < NR_PRDS; i++) {
		bytes = MIN(iovec[i].iov_size, size);

		if (endpt == SELF) {
			vvec[i].vv_addr = (vir_bytes) iovec[i].iov_grant;
		} else {
			vvec[i].vv_grant = iovec[i].iov_grant;
		}

		vvec[i].vv_size = bytes;
		size -= bytes;
	}

	pcount = i;

	if (pcount > 0) {
		r = sys_vumap(endpt, vvec, pcount, 0, 
			write ? VUA_READ : VUA_WRITE, &prdt[nr_prds], &pcount);
		if (r != OK) {
			dprintf(V_ERR, ("%s: unable to map memory from %d (%d)\n",
				ahci_portname(ps), endpt, r));
			return r;
		}

		if (pcount <= 0 || pcount > i) {
			return EINVAL;
		}

		for (i = 0; i < pcount; i++) {
			if (vvec[i].vv_size != prdt[nr_prds].vp_size) {
				dprintf(V_ERR, ("%s: non-contiguous memory from %d\n",
					ahci_portname(ps), endpt));
				return EINVAL;
			}

			if (prdt[nr_prds].vp_addr & 1) {
				dprintf(V_ERR, ("%s: bad physical address from %d\n",
					ahci_portname(ps), endpt));
				return EINVAL;
			}

			nr_prds++;
			if (nr_prds >= NR_PRDS) {
				return EINVAL;
			}
		}
	}

	if (trail > 0) {
		if (nr_prds >= NR_PRDS) {
			return EINVAL;
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
static ssize_t port_transfer(struct port_state *ps, u64_t pos, u64_t eof,
	endpoint_t endpt, iovec_s_t *iovec, int nr_req, int write, int flags)
{
	prd_t prdt[NR_PRDS];
	vir_bytes size, lead;
	unsigned int count, nr_prds;
	u64_t start_lba;
	int r, cmd;

	if (!ps || !iovec) {
		return EINVAL;
	}

	r = sum_iovec(ps, endpt, iovec, nr_req, &size);
	if (r != OK) {
		return r;
	}

	dprintf(V_REQ, ("%s: %s for %lu bytes at pos %llx\n",
		ahci_portname(ps), write ? "write" : "read", size, pos));

	assert(ps->state == STATE_GOOD_DEV);
	assert(ps->flags & FLAG_HAS_MEDIUM);
	assert(ps->sector_size > 0);

	if (size > MAX_TRANSFER) {
		size = MAX_TRANSFER;
	}

	if (pos > eof) {
		return EINVAL;
	}

	if (pos + size > eof) {
		size = (vir_bytes)(eof - pos);
	}

	start_lba = pos / ps->sector_size;
	lead = (vir_bytes)(pos % ps->sector_size);
	count = (lead + size + ps->sector_size - 1) / ps->sector_size;

	if (lead & 1) {
		dprintf(V_ERR, ("%s: unaligned position from %d\n",
			ahci_portname(ps), endpt));
		return EINVAL;
	}

	if (write) {
		if (lead != 0) {
			dprintf(V_ERR, ("%s: unaligned position from %d\n",
				ahci_portname(ps), endpt));
			return EINVAL;
		}
		if ((size % ps->sector_size) != 0) {
			dprintf(V_ERR, ("%s: unaligned size %lu from %d\n",
				ahci_portname(ps), size, endpt));
			return EINVAL;
		}
	}

	r = setup_prdt(ps, endpt, iovec, nr_req, size, lead, write, prdt);
	if (r < 0) {
		return r;
	}
	nr_prds = r;

	cmd = port_find_cmd(ps);

	if (ps->flags & FLAG_ATAPI) {
		r = atapi_transfer(ps, cmd, start_lba, count, write, prdt,
			nr_prds);
	} else {
		int force_write = (flags & BDEV_FORCEWRITE) ? 1 : 0;
		r = ata_transfer(ps, cmd, start_lba, count, write,
			force_write, prdt, nr_prds);
	}

	if (r != OK) {
		return r;
	}

	return size;
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
static void port_override(struct port_state *ps)
{
	u32_t cmd;
	u32_t timeout_counter;

	if (ps == NULL) {
		return;
	}

	cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_CLO);

	timeout_counter = 0;
	while ((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CLO) != 0) {
		if (timeout_counter >= PORTREG_DELAY) {
			break;
		}
		timeout_counter++;
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

	port_write(ps, AHCI_PORT_SERR, ~0);
	port_write(ps, AHCI_PORT_IS, ~0);

	cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_ST);

	dprintf(V_INFO, ("%s: started\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_stop				     *
 *===========================================================================*/
static void port_stop(struct port_state *ps)
{
	u32_t cmd;
	u32_t active_mask;
	int retry_count;

	if (ps == NULL) {
		return;
	}

	cmd = port_read(ps, AHCI_PORT_CMD);
	active_mask = AHCI_PORT_CMD_CR | AHCI_PORT_CMD_ST;

	if ((cmd & active_mask) == 0) {
		return;
	}

	cmd &= ~AHCI_PORT_CMD_ST;
	port_write(ps, AHCI_PORT_CMD, cmd);

	retry_count = PORTREG_DELAY;
	while (retry_count > 0) {
		if ((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CR) == 0) {
			break;
		}
		retry_count--;
	}

	dprintf(V_INFO, ("%s: stopped\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_restart				     *
 *===========================================================================*/
static void port_restart(struct port_state *ps)
{
	port_fail_cmds(ps);
	port_stop(ps);

	if (port_read(ps, AHCI_PORT_TFD) & (AHCI_PORT_TFD_STS_BSY | AHCI_PORT_TFD_STS_DRQ)) {
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
	const u16_t SPACE_WORD = 0x2020;
	const u8_t SPACE_CHAR = 0x20;
	
	if (buf == NULL || start < 0 || end < start) {
		return;
	}
	
	while (end >= start && buf[end] == SPACE_WORD) {
		end--;
	}
	
	int print_last_byte = 0;
	if (end >= start && (buf[end] & 0xFF) == SPACE_CHAR) {
		print_last_byte = 1;
		end--;
	}
	
	for (int i = start; i <= end; i++) {
		u8_t high_byte = (u8_t)(buf[i] >> 8);
		u8_t low_byte = (u8_t)(buf[i] & 0xFF);
		printf("%c%c", high_byte, low_byte);
	}
	
	if (print_last_byte && end >= start) {
		u8_t high_byte = (u8_t)(buf[end + 1] >> 8);
		printf("%c", high_byte);
	}
}

/*===========================================================================*
 *				port_id_check				     *
 *===========================================================================*/
static void port_id_check(struct port_state *ps, int success)
{
	u16_t *buf;

	assert(ps->state == STATE_WAIT_ID);

	ps->flags &= ~FLAG_BUSY;
	cancel_timer(&ps->cmd_info[0].timer);

	if (!success) {
		if (!(ps->flags & FLAG_ATAPI) &&
				port_read(ps, AHCI_PORT_SIG) != ATA_SIG_ATA) {
			dprintf(V_INFO, ("%s: may not be ATA, trying ATAPI\n",
				ahci_portname(ps)));

			ps->flags |= FLAG_ATAPI;

			(void) gen_identify(ps, FALSE);
			return;
		}

		dprintf(V_ERR,
			("%s: unable to identify\n", ahci_portname(ps)));
	}

	if (success) {
		buf = (u16_t *) ps->tmp_base;

		if (ps->flags & FLAG_ATAPI)
			success = atapi_id_check(ps, buf);
		else
			success = ata_id_check(ps, buf);
	}

	if (!success) {
		port_stop(ps);

		ps->state = STATE_BAD_DEV;
		port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PRCE);

		return;
	}

	ps->state = STATE_GOOD_DEV;

	if (ahci_verbose >= V_INFO) {
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

		if (ps->flags & FLAG_HAS_MEDIUM)
			printf(", %u byte sectors, %llu MB size",
				ps->sector_size,
				ps->lba_count * ps->sector_size / (1024*1024));

		printf("\n");
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
	if (ps == NULL) {
		return;
	}

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

	if (ps->state != STATE_WAIT_DEV) {
		return;
	}

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
static void port_intr(struct port_state *ps)
{
	u32_t smask, emask;
	int success;

	if (ps->state == STATE_NO_PORT) {
		dprintf(V_ERR, ("%s: interrupt for invalid port!\n",
			ahci_portname(ps)));
		return;
	}

	smask = port_read(ps, AHCI_PORT_IS);
	emask = smask & port_read(ps, AHCI_PORT_IE);

	port_write(ps, AHCI_PORT_IS, smask);

	dprintf(V_REQ, ("%s: interrupt (%08x)\n", ahci_portname(ps), smask));

	port_check_cmds(ps);

	if (emask & AHCI_PORT_IS_PCS) {
		handle_device_attached(ps);
	} else if (emask & AHCI_PORT_IS_PRCS) {
		handle_device_detached(ps);
	} else if (smask & AHCI_PORT_IS_MASK) {
		handle_command_completion(ps, smask);
	}
}

static void handle_device_attached(struct port_state *ps)
{
	port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_X);

	dprintf(V_DEV, ("%s: device attached\n", ahci_portname(ps)));

	if (ps->state == STATE_SPIN_UP || ps->state == STATE_NO_DEV) {
		if (ps->state == STATE_SPIN_UP) {
			cancel_timer(&ps->cmd_info[0].timer);
		}

		ps->state = STATE_WAIT_DEV;
		ps->left = ahci_device_checks;
		port_dev_check(ps);
	} else if (ps->state != STATE_WAIT_DEV) {
		assert(0);
	}
}

static void handle_device_detached(struct port_state *ps)
{
	port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_N);

	dprintf(V_DEV, ("%s: device detached\n", ahci_portname(ps)));

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
}

static void handle_command_completion(struct port_state *ps, u32_t smask)
{
	u32_t tfd_status;
	int success;

	tfd_status = port_read(ps, AHCI_PORT_TFD);
	success = !(tfd_status & (AHCI_PORT_TFD_STS_ERR | AHCI_PORT_TFD_STS_DF));

	if (!success || (smask & AHCI_PORT_IS_RESTART)) {
		port_restart(ps);
	}

	if (ps->state == STATE_WAIT_ID) {
		port_id_check(ps, success);
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

	if (port < 0 || port >= hba_state.nr_ports) {
		return;
	}

	ps = &port_state[port];

	if (ps->flags & FLAG_SUSPENDED) {
		if (cmd == 0) {
			blockdriver_mt_wakeup(ps->cmd_info[0].tid);
		}
	}

	switch (ps->state) {
	case STATE_SPIN_UP:
		handle_spin_up_timeout(ps);
		break;
	case STATE_WAIT_DEV:
		port_dev_check(ps);
		break;
	case STATE_WAIT_ID:
		handle_wait_id_timeout(ps);
		break;
	default:
		handle_generic_timeout(ps);
		break;
	}
}

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

static void handle_wait_id_timeout(struct port_state *ps)
{
	dprintf(V_ERR, ("%s: timeout\n", ahci_portname(ps)));
	port_restart(ps);
	port_id_check(ps, FALSE);
}

static void handle_generic_timeout(struct port_state *ps)
{
	dprintf(V_ERR, ("%s: timeout\n", ahci_portname(ps)));
	port_restart(ps);
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
	uint32_t cmd_mask;
	struct cmd_info *cmd_info;
	int port_index;

	if (ps == NULL || cmd < 0 || cmd >= 32) {
		return;
	}

	cmd_mask = 1U << cmd;
	cmd_info = &ps->cmd_info[cmd];
	port_index = ps - port_state;

	if ((ps->flags & FLAG_HAS_NCQ) != 0) {
		port_write(ps, AHCI_PORT_SACT, cmd_mask);
	}

	__insn_barrier();

	port_write(ps, AHCI_PORT_CI, cmd_mask);

	ps->pend_mask |= cmd_mask;

	set_timer(&cmd_info->timer, timeout, port_timeout,
		BUILD_ARG(port_index, cmd));
}

/*===========================================================================*
 *				port_exec				     *
 *===========================================================================*/
static int port_exec(struct port_state *ps, int cmd, clock_t timeout)
{
	struct cmd_info *cmd_info;
	int result;

	if (ps == NULL || cmd < 0) {
		return EIO;
	}

	cmd_info = &ps->cmd_info[cmd];

	port_issue(ps, cmd, timeout);

	cmd_info->tid = blockdriver_mt_get_tid();

	blockdriver_mt_sleep();

	cancel_timer(&cmd_info->timer);

	assert(!(ps->flags & FLAG_BUSY));

	result = cmd_info->result;

	dprintf(V_REQ, ("%s: end of command -- %s\n", ahci_portname(ps),
		(result == RESULT_FAILURE) ? "failure" : "success"));

	if (result == RESULT_FAILURE) {
		return EIO;
	}

	return OK;
}

/*===========================================================================*
 *				port_alloc				     *
 *===========================================================================*/
static void port_alloc(struct port_state *ps)
{
	size_t fis_off, tmp_off, ct_off;
	size_t ct_offs[NR_CMDS];
	u32_t cmd;
	int i;

	fis_off = AHCI_CL_SIZE + AHCI_FIS_SIZE - 1;
	fis_off -= fis_off % AHCI_FIS_SIZE;

	tmp_off = fis_off + AHCI_FIS_SIZE + AHCI_TMP_ALIGN - 1;
	tmp_off -= tmp_off % AHCI_TMP_ALIGN;

	ct_off = tmp_off + AHCI_TMP_SIZE;
	for (i = 0; i < NR_CMDS; i++) {
		ct_off += AHCI_CT_ALIGN - 1;
		ct_off -= ct_off % AHCI_CT_ALIGN;
		ct_offs[i] = ct_off;
		ct_off += AHCI_CT_SIZE;
	}
	ps->mem_size = ct_off;

	ps->mem_base = alloc_contig(ps->mem_size, AC_ALIGN4K, &ps->mem_phys);
	if (ps->mem_base == NULL) {
		panic("unable to allocate port memory");
	}
	memset(ps->mem_base, 0, ps->mem_size);

	ps->cl_base = (u32_t *) ps->mem_base;
	ps->cl_phys = ps->mem_phys;
	assert(ps->cl_phys % AHCI_CL_SIZE == 0);

	ps->fis_base = (u32_t *) (ps->mem_base + fis_off);
	ps->fis_phys = ps->mem_phys + fis_off;
	assert(ps->fis_phys % AHCI_FIS_SIZE == 0);

	ps->tmp_base = (u8_t *) (ps->mem_base + tmp_off);
	ps->tmp_phys = ps->mem_phys + tmp_off;
	assert(ps->tmp_phys % AHCI_TMP_ALIGN == 0);

	for (i = 0; i < NR_CMDS; i++) {
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
static void port_free(struct port_state *ps)
{
	u32_t cmd;

	if (ps == NULL) {
		return;
	}

	cmd = port_read(ps, AHCI_PORT_CMD);

	if ((cmd & AHCI_PORT_CMD_FR) != 0 || (cmd & AHCI_PORT_CMD_FRE) != 0) {
		port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_FRE);

		SPIN_UNTIL((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_FR) == 0,
			PORTREG_DELAY);
	}

	if (ps->pad_base != NULL) {
		free_contig(ps->pad_base, ps->pad_size);
		ps->pad_base = NULL;
	}

	if (ps->mem_base != NULL) {
		free_contig(ps->mem_base, ps->mem_size);
		ps->mem_base = NULL;
	}
}

/*===========================================================================*
 *				port_init				     *
 *===========================================================================*/
static void port_init(struct port_state *ps)
{
	u32_t cmd;
	int i;
	size_t port_index;

	ps->queue_depth = 1;
	ps->state = STATE_SPIN_UP;
	ps->flags = FLAG_BUSY;
	ps->sector_size = 0;
	ps->open_count = 0;
	ps->pend_mask = 0;
	
	for (i = 0; i < NR_CMDS; i++)
		init_timer(&ps->cmd_info[i].timer);

	port_index = ps - port_state;
	ps->reg = (u32_t *) ((u8_t *) hba_state.base +
		AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * port_index);

	port_alloc(ps);

	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);

	cmd = port_read(ps, AHCI_PORT_CMD);
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
	int r, devind;
	u16_t vid, did;

	pci_init();

	r = pci_first_dev(&devind, &vid, &did);
	if (r <= 0) {
		return -1;
	}

	for (int i = 0; i < skip; i++) {
		r = pci_next_dev(&devind, &vid, &did);
		if (r <= 0) {
			return -1;
		}
	}

	pci_reserve(devind);

	return devind;
}

/*===========================================================================*
 *				ahci_reset				     *
 *===========================================================================*/
static void ahci_reset(void)
{
	u32_t ghc;
	u32_t ghc_with_ae;
	u32_t ghc_reset;
	u32_t timeout_counter;

	ghc = hba_read(AHCI_HBA_GHC);
	ghc_with_ae = ghc | AHCI_HBA_GHC_AE;
	ghc_reset = ghc_with_ae | AHCI_HBA_GHC_HR;

	hba_write(AHCI_HBA_GHC, ghc_with_ae);
	hba_write(AHCI_HBA_GHC, ghc_reset);

	timeout_counter = 0;
	while ((hba_read(AHCI_HBA_GHC) & AHCI_HBA_GHC_HR) != 0) {
		if (timeout_counter >= RESET_DELAY) {
			panic("unable to reset HBA");
		}
		timeout_counter++;
	}
}

/*===========================================================================*
 *				ahci_init				     *
 *===========================================================================*/
static void ahci_init(int devind)
{
	u32_t base, size, cap, ghc, mask;
	int r, port, ioflag;

	r = pci_get_bar(devind, PCI_BAR_6, &base, &size, &ioflag);
	if (r != OK)
		panic("unable to retrieve BAR: %d", r);

	if (ioflag)
		panic("invalid BAR type");

	if (size < AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE)
		panic("HBA memory size too small: %u", size);

	size = MIN(size, AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * NR_PORTS);

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
	hba_state.has_ncq = !!(cap & AHCI_HBA_CAP_SNCQ);
	hba_state.has_clo = !!(cap & AHCI_HBA_CAP_SCLO);
	hba_state.nr_cmds = MIN(NR_CMDS,
		((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1);

	dprintf(V_INFO, ("AHCI%u: HBA v%d.%d%d, %ld ports, %ld commands, "
		"%s queuing, IRQ %d\n",
		ahci_instance,
		(int) (hba_read(AHCI_HBA_VS) >> 16),
		(int) ((hba_read(AHCI_HBA_VS) >> 8) & 0xFF),
		(int) (hba_read(AHCI_HBA_VS) & 0xFF),
		((cap >> AHCI_HBA_CAP_NP_SHIFT) & AHCI_HBA_CAP_NP_MASK) + 1,
		((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1,
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

		if (ps->state != STATE_NO_PORT) {
			port_stop(ps);
			port_free(ps);
		}
	}

	ahci_reset();

	r = vm_unmap_phys(SELF, (void *) hba_state.base, hba_state.size);
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
	struct port_state *ps;
	u32_t interrupt_mask;
	int result;
	int port;

	interrupt_mask = hba_read(AHCI_HBA_IS);

	for (port = 0; port < hba_state.nr_ports; port++) {
		if ((interrupt_mask & (1U << port)) == 0) {
			continue;
		}

		ps = &port_state[port];
		port_intr(ps);

		if ((ps->flags & FLAG_SUSPENDED) != 0 && 
		    (ps->flags & FLAG_BUSY) == 0) {
			blockdriver_mt_wakeup(ps->cmd_info[0].tid);
		}
	}

	hba_write(AHCI_HBA_IS, interrupt_mask);

	result = sys_irqenable(&hba_state.hook_id);
	if (result != OK) {
		panic("unable to enable IRQ: %d", result);
	}
}

/*===========================================================================*
 *				ahci_get_params				     *
 *===========================================================================*/
static void ahci_get_params(void)
{
	long v;
	unsigned int i;

	v = 0;
	env_parse("instance", "d", 0, &v, 0, 255);
	ahci_instance = (int)v;

	v = V_ERR;
	env_parse("ahci_verbose", "d", 0, &v, V_NONE, V_REQ);
	ahci_verbose = (int)v;

	for (i = 0; i < sizeof(ahci_timevar) / sizeof(ahci_timevar[0]); i++) {
		v = ahci_timevar[i].default_ms;
		env_parse(ahci_timevar[i].name, "d", 0, &v, 1, LONG_MAX);
		*ahci_timevar[i].ptr = millis_to_hz(v);
	}

	ahci_device_delay = millis_to_hz(DEVICE_DELAY);
	ahci_device_checks = (ahci_device_timeout + ahci_device_delay - 1) /
		ahci_device_delay;
}

/*===========================================================================*
 *				ahci_set_mapping			     *
 *===========================================================================*/
static void ahci_set_mapping(void)
{
	char key[16], val[32], *p;
	unsigned int port;
	int i, j;

	for (i = j = 0; i < NR_PORTS && j < MAX_DRIVES; i++) {
		if (port_state[i].state != STATE_NO_PORT) {
			ahci_map[j++] = i;
		}
	}

	for ( ; j < MAX_DRIVES; j++) {
		ahci_map[j] = NO_PORT;
	}

	if (snprintf(key, sizeof(key), "ahci%d_map", ahci_instance) >= sizeof(key)) {
		return;
	}

	if (env_get_param(key, val, sizeof(val)) != OK) {
		goto create_reverse_mapping;
	}

	p = val;
	for (i = 0; i < MAX_DRIVES && *p; i++) {
		char *endptr;
		port = (unsigned int) strtoul(p, &endptr, 0);
		
		if (endptr == p) {
			ahci_map[i] = NO_PORT;
			break;
		}
		
		ahci_map[i] = port % NR_PORTS;
		p = endptr;
		
		if (*p == ',') {
			p++;
		} else if (*p != '\0') {
			break;
		}
	}
	
	for ( ; i < MAX_DRIVES; i++) {
		ahci_map[i] = NO_PORT;
	}

create_reverse_mapping:
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

	ahci_get_params();

	devind = ahci_probe(ahci_instance);
	if (devind < 0) {
		panic("no matching device found");
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
	exit(0);
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
static char *ahci_portname(struct port_state *ps)
{
	static char name[10];
	int port_index;

	if (ps == NULL) {
		return "";
	}

	port_index = ps - port_state;
	if (port_index < 0 || port_index > 99) {
		return "";
	}

	name[0] = 'A';
	name[1] = 'H';
	name[2] = 'C';
	name[3] = 'I';
	name[4] = '0' + ahci_instance;
	name[5] = '-';

	if (ps->device == NO_DEVICE) {
		name[6] = 'P';
		name[7] = '0' + (port_index / 10);
		name[8] = '0' + (port_index % 10);
		name[9] = '\0';
	}
	else {
		name[6] = 'D';
		name[7] = '0' + ps->device;
		name[8] = '\0';
		name[9] = '\0';
	}

	return name;
}

/*===========================================================================*
 *				ahci_map_minor				     *
 *===========================================================================*/
static struct port_state *ahci_map_minor(devminor_t minor, struct device **dvp)
{
	struct port_state *ps;
	int port;
	int index;
	int offset;

	if (dvp == NULL) {
		return NULL;
	}

	if (minor >= 0 && minor < NR_MINORS) {
		index = minor / DEV_PER_DRIVE;
		offset = minor % DEV_PER_DRIVE;
		
		port = ahci_map[index];
		if (port == NO_PORT) {
			return NULL;
		}
		
		ps = &port_state[port];
		*dvp = &ps->part[offset];
		return ps;
	}

	minor -= MINOR_d0p0s0;
	if ((unsigned)minor < NR_SUBDEVS) {
		index = minor / SUB_PER_DRIVE;
		offset = minor % SUB_PER_DRIVE;
		
		port = ahci_map[index];
		if (port == NO_PORT) {
			return NULL;
		}
		
		ps = &port_state[port];
		*dvp = &ps->subpart[offset];
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
    ahci_map_minor(minor, &dv);
    return dv;
}

/*===========================================================================*
 *				ahci_open				     *
 *===========================================================================*/
static int ahci_open(devminor_t minor, int access)
{
	struct port_state *ps;
	int r;

	ps = ahci_get_port(minor);
	if (ps == NULL)
		return ENXIO;

	ps->cmd_info[0].tid = blockdriver_mt_get_tid();

	if (ps->flags & FLAG_BUSY)
		port_wait(ps);

	if (ps->state != STATE_GOOD_DEV)
		return ENXIO;

	if ((ps->flags & FLAG_READONLY) && (access & BDEV_W_BIT))
		return EACCES;

	if (ps->open_count == 0) {
		r = handle_first_open(ps);
		if (r != OK)
			return r;
	} else if (ps->flags & FLAG_BARRIER) {
		return ENXIO;
	}

	ps->open_count++;
	return OK;
}

static int handle_first_open(struct port_state *ps)
{
	int r;

	ps->flags &= ~FLAG_BARRIER;

	if (ps->flags & FLAG_ATAPI) {
		r = atapi_check_medium(ps, 0);
		if (r != OK)
			return r;
	}

	reset_partition_tables(ps);
	ps->part[0].dv_size = ps->lba_count * ps->sector_size;

	partition(&ahci_dtab, ps->device * DEV_PER_DRIVE, P_PRIMARY,
		!!(ps->flags & FLAG_ATAPI));

	blockdriver_mt_set_workers(ps->device, ps->queue_depth);
	return OK;
}

static void reset_partition_tables(struct port_state *ps)
{
	memset(ps->part, 0, sizeof(ps->part));
	memset(ps->subpart, 0, sizeof(ps->subpart));
}

/*===========================================================================*
 *				ahci_close				     *
 *===========================================================================*/
static int ahci_close(devminor_t minor)
{
	struct port_state *ps;
	int port;

	ps = ahci_get_port(minor);
	if (ps == NULL) {
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
		gen_flush_wcache(ps);
	}

	if (!ahci_exiting) {
		return OK;
	}

	for (port = 0; port < hba_state.nr_ports; port++) {
		if (port_state[port].open_count > 0) {
			return OK;
		}
	}

	ahci_stop();
	blockdriver_mt_terminate();

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
		return EIO;
	}

	dv = ahci_part(minor);
	if (dv == NULL) {
		return EIO;
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
	int r, val;

	ps = ahci_get_port(minor);
	if (ps == NULL)
		return EINVAL;

	if (request != DIOCOPENCT) {
		if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER))
			return EIO;
	}

	switch (request) {
	case DIOCEJECT:
		if ((ps->flags & FLAG_ATAPI) == 0)
			return EINVAL;
		return atapi_load_eject(ps, 0, FALSE);

	case DIOCOPENCT:
		return sys_safecopyto(endpt, grant, 0,
			(vir_bytes) &ps->open_count, sizeof(ps->open_count));

	case DIOCFLUSH:
		return gen_flush_wcache(ps);

	case DIOCSETWC:
		r = sys_safecopyfrom(endpt, grant, 0, (vir_bytes) &val,
			sizeof(val));
		if (r != OK)
			return r;
		return gen_set_wcache(ps, val);

	case DIOCGETWC:
		r = gen_get_wcache(ps, &val);
		if (r != OK)
			return r;
		return sys_safecopyto(endpt, grant, 0, (vir_bytes) &val,
			sizeof(val));

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
	struct device *dv;

	if (id == NULL)
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
		panic("device mapping for minor %d disappeared", minor);
	}

	return ps;
}

/*===========================================================================*
 *				main					     *
 *===========================================================================*/
int main(int argc, char **argv)
{
	env_setargs(argc, argv);
	sef_local_startup();
	blockdriver_mt_task(&ahci_dtab);
	return 0;
}
