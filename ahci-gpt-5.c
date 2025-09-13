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
#include <errno.h>

static int atapi_exec(struct port_state *ps, int cmd,
	u8_t packet[ATAPI_PACKET_SIZE], size_t size, int write)
{
	cmd_fis_t fis;
	prd_t prd[1];
	int nr_prds;

	if (ps == NULL || packet == NULL) return EINVAL;
	if (size > AHCI_TMP_SIZE) return EINVAL;

	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_PACKET;

	nr_prds = (size > 0) ? 1 : 0;

	if (nr_prds) {
		fis.cf_feat = ATA_FEAT_PACKET_DMA;
		if (!write && (ps->flags & FLAG_USE_DMADIR)) {
			fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;
		}
		prd[0].vp_addr = ps->tmp_phys;
		prd[0].vp_size = size;
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
	enum { SENSE_KEY_IDX = 2, ASC_IDX = 12, ASCQ_IDX = 13 };
	const u8_t sense_key_mask = 0x0F;
	u8_t packet[ATAPI_PACKET_SIZE];
	int r;

	if (!ps || !sense || ATAPI_REQUEST_SENSE_LEN <= ASCQ_IDX)
		return EINVAL;

	memset(packet, 0, sizeof(packet));
	packet[0] = ATAPI_CMD_REQUEST_SENSE;
	packet[4] = ATAPI_REQUEST_SENSE_LEN;

	r = atapi_exec(ps, cmd, packet, ATAPI_REQUEST_SENSE_LEN, FALSE);
	if (r != OK)
		return r;

	dprintf(V_REQ, ("%s: ATAPI SENSE: sense %x ASC %x ASCQ %x\n",
	    ahci_portname(ps),
	    ps->tmp_base[SENSE_KEY_IDX] & sense_key_mask,
	    ps->tmp_base[ASC_IDX],
	    ps->tmp_base[ASCQ_IDX]));

	*sense = ps->tmp_base[SENSE_KEY_IDX] & sense_key_mask;

	return OK;
}

/*===========================================================================*
 *				atapi_load_eject			     *
 *===========================================================================*/
static int atapi_load_eject(struct port_state *ps, int cmd, int load)
{
	enum { PACKET_OPCODE_IDX = 0, PACKET_START_STOP_IDX = 4 };

	u8_t packet[ATAPI_PACKET_SIZE] = {0};

	if (ATAPI_PACKET_SIZE > PACKET_OPCODE_IDX) {
		packet[PACKET_OPCODE_IDX] = ATAPI_CMD_START_STOP;
	}
	if (ATAPI_PACKET_SIZE > PACKET_START_STOP_IDX) {
		packet[PACKET_START_STOP_IDX] =
			(load != 0) ? ATAPI_START_STOP_LOAD : ATAPI_START_STOP_EJECT;
	}

	return atapi_exec(ps, cmd, packet, 0, FALSE);
}

/*===========================================================================*
 *				atapi_read_capacity			     *
 *===========================================================================*/
static int atapi_read_capacity(struct port_state *ps, int cmd)
{
	u8_t packet[ATAPI_PACKET_SIZE];
	u8_t *buf;
	u32_t last_lba, sector_size;
	u64_t mb_size;
	int r;

	memset(packet, 0, sizeof(packet));
	packet[0] = ATAPI_CMD_READ_CAPACITY;

	r = atapi_exec(ps, cmd, packet, ATAPI_READ_CAPACITY_LEN, FALSE);
	if (r != OK)
		return r;

	buf = ps->tmp_base;
	if (buf == NULL)
		return EINVAL;

	last_lba = ((u32_t)buf[0] << 24) | ((u32_t)buf[1] << 16) |
	    ((u32_t)buf[2] << 8) | (u32_t)buf[3];
	sector_size = ((u32_t)buf[4] << 24) | ((u32_t)buf[5] << 16) |
	    ((u32_t)buf[6] << 8) | (u32_t)buf[7];

	ps->lba_count = (u64_t)last_lba + 1;
	ps->sector_size = sector_size;

	if (ps->sector_size == 0 || (ps->sector_size & 1)) {
		dprintf(V_ERR, ("%s: invalid medium sector size %u\n",
		    ahci_portname(ps), ps->sector_size));
		return EINVAL;
	}

	{
		const u32_t MB = 1024U * 1024U;
		u32_t b = sector_size;
		u32_t m = MB;
		u32_t a1 = b, b1 = m;

		while (b1 != 0U) {
			u32_t t = a1 % b1;
			a1 = b1;
			b1 = t;
		}

		{
			u32_t g = a1;
			u32_t bs = b / g;
			u32_t ms = m / g;
			u64_t q = ps->lba_count / ms;
			u64_t rem = ps->lba_count % ms;

			mb_size = q * bs + (rem * bs) / ms;
		}
	}

	{
		const char *name = ahci_portname(ps);
		dprintf(V_INFO,
		    ("%s: medium detected (%u byte sectors, %llu MB size)\n",
		    name, ps->sector_size, mb_size));
	}

	return OK;
}

/*===========================================================================*
 *				atapi_check_medium			     *
 *===========================================================================*/
static int atapi_check_medium(struct port_state *ps, int cmd)
{
	int sense = 0;
	int need_capacity = 0;

	if (ps == NULL)
		return EINVAL;

	if (atapi_test_unit(ps, cmd) != OK) {
		ps->flags &= ~FLAG_HAS_MEDIUM;

		if (atapi_request_sense(ps, cmd, &sense) != OK)
			return ENXIO;

		if (sense != ATAPI_SENSE_UNIT_ATT)
			return ENXIO;

		need_capacity = 1;
	} else if (!(ps->flags & FLAG_HAS_MEDIUM)) {
		need_capacity = 1;
	}

	if (need_capacity) {
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
	if (ps == NULL || buf == NULL) {
		if (ps != NULL)
			dprintf(V_ERR, ("%s: invalid parameters\n", ahci_portname(ps)));
		return FALSE;
	}

	const char *portname = ahci_portname(ps);
	u16_t gcap = buf[ATA_ID_GCAP];
	u16_t cap = buf[ATA_ID_CAP];
	u16_t dmadir = buf[ATA_ID_DMADIR];
	u16_t sup0 = buf[ATA_ID_SUP0];
	u16_t sup1 = buf[ATA_ID_SUP1];

	int is_atapi = ((gcap & (ATA_ID_GCAP_ATAPI_MASK | ATA_ID_GCAP_REMOVABLE | ATA_ID_GCAP_INCOMPLETE)) ==
		(ATA_ID_GCAP_ATAPI | ATA_ID_GCAP_REMOVABLE));
	int cap_dma = ((cap & ATA_ID_CAP_DMA) == ATA_ID_CAP_DMA);
	int dma_with_dir = ((dmadir & (ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA)) ==
		(ATA_ID_DMADIR_DMADIR | ATA_ID_DMADIR_DMA));

	if (!is_atapi || (!cap_dma && !dma_with_dir)) {
		dprintf(V_ERR, ("%s: unsupported ATAPI device\n", portname));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x DMADIR %04x\n", portname, gcap, cap, dmadir));
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
static inline void pack_be32(u8_t *dst, u32_t value)
{
	dst[0] = (u8_t)((value >> 24) & 0xFF);
	dst[1] = (u8_t)((value >> 16) & 0xFF);
	dst[2] = (u8_t)((value >> 8) & 0xFF);
	dst[3] = (u8_t)(value & 0xFF);
}

static int atapi_transfer(struct port_state *ps, int cmd, u64_t start_lba,
	unsigned int count, int write, prd_t *prdt, int nr_prds)
{
	cmd_fis_t fis;
	u8_t packet[ATAPI_PACKET_SIZE];
	u32_t lba32 = (u32_t)start_lba;
	u32_t count32 = (u32_t)count;

	memset(&fis, 0, sizeof(fis));
	fis.cf_cmd = ATA_CMD_PACKET;
	fis.cf_feat = ATA_FEAT_PACKET_DMA;
	if (!write && (ps->flags & FLAG_USE_DMADIR))
		fis.cf_feat |= ATA_FEAT_PACKET_DMADIR;

	memset(packet, 0, sizeof(packet));
	packet[0] = write ? ATAPI_CMD_WRITE : ATAPI_CMD_READ;
	pack_be32(&packet[2], lba32);
	pack_be32(&packet[6], count32);

	port_set_cmd(ps, cmd, &fis, packet, prdt, nr_prds, write);

	return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				ata_id_check				     *
 *===========================================================================*/
static int ata_id_check(struct port_state *ps, u16_t *buf)
{
	if (ps == NULL || buf == NULL)
		return FALSE;

	const char *name = ahci_portname(ps);

	u16_t gcap = buf[ATA_ID_GCAP];
	u16_t cap = buf[ATA_ID_CAP];
	u16_t sup1 = buf[ATA_ID_SUP1];

	int is_ata = ((gcap & (ATA_ID_GCAP_ATA_MASK | ATA_ID_GCAP_REMOVABLE |
	    ATA_ID_GCAP_INCOMPLETE)) == ATA_ID_GCAP_ATA);
	int has_lba_dma = ((cap & (ATA_ID_CAP_LBA | ATA_ID_CAP_DMA)) ==
	    (ATA_ID_CAP_LBA | ATA_ID_CAP_DMA));
	int sup1_ok = ((sup1 & (ATA_ID_SUP1_VALID_MASK | ATA_ID_SUP1_FLUSH |
	    ATA_ID_SUP1_LBA48)) == (ATA_ID_SUP1_VALID | ATA_ID_SUP1_FLUSH |
	    ATA_ID_SUP1_LBA48));

	if (!(is_ata && has_lba_dma && sup1_ok)) {
		dprintf(V_ERR, ("%s: unsupported ATA device\n", name));
		dprintf(V_DEV, ("%s: GCAP %04x CAP %04x SUP1 %04x\n",
		    name, gcap, cap, sup1));
		return FALSE;
	}

	ps->lba_count =
	    ((u64_t)buf[ATA_ID_LBA3] << 48) |
	    ((u64_t)buf[ATA_ID_LBA2] << 32) |
	    ((u64_t)buf[ATA_ID_LBA1] << 16) |
	    (u64_t)buf[ATA_ID_LBA0];

	if (hba_state.has_ncq && (buf[ATA_ID_SATA_CAP] & ATA_ID_SATA_CAP_NCQ)) {
		u16_t qd_raw = (u16_t)(buf[ATA_ID_QDEPTH] & ATA_ID_QDEPTH_MASK);
		unsigned int depth = (unsigned int)qd_raw + 1U;

		ps->flags |= FLAG_HAS_NCQ;

		if (depth > hba_state.nr_cmds)
			depth = hba_state.nr_cmds;

		ps->queue_depth = depth;
	}

	{
		u16_t plss = buf[ATA_ID_PLSS];
		if ((plss & (ATA_ID_PLSS_VALID_MASK | ATA_ID_PLSS_LLS)) ==
		    (ATA_ID_PLSS_VALID | ATA_ID_PLSS_LLS)) {
			u32_t lss = ((u32_t)buf[ATA_ID_LSS1] << 16) |
			            (u32_t)buf[ATA_ID_LSS0];
			u32_t ssize = lss << 1;
			ps->sector_size = ssize;
		} else {
			ps->sector_size = ATA_SECTOR_SIZE;
		}
	}

	if (ps->sector_size < ATA_SECTOR_SIZE) {
		dprintf(V_ERR, ("%s: invalid sector size %u\n",
		    name, ps->sector_size));
		return FALSE;
	}

	ps->flags |= FLAG_HAS_MEDIUM | FLAG_HAS_FLUSH;

	if (buf[ATA_ID_SUP0] & ATA_ID_SUP0_WCACHE)
		ps->flags |= FLAG_HAS_WCACHE;

	{
		u16_t ena2 = buf[ATA_ID_ENA2];
		if ((ena2 & (ATA_ID_ENA2_VALID_MASK | ATA_ID_ENA2_FUA)) ==
		    (ATA_ID_ENA2_VALID | ATA_ID_ENA2_FUA))
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
	unsigned int sec_cnt;
	int is_write;
	int has_ncq;
	int has_fua;

	if (ps == NULL)
		return -1;
	if (nr_prds < 0)
		return -1;
	if (nr_prds > 0 && prdt == NULL)
		return -1;

	assert(count <= ATA_MAX_SECTORS);
	if (count > ATA_MAX_SECTORS)
		return -1;

	sec_cnt = (count == ATA_MAX_SECTORS) ? 0 : count;

	memset(&fis, 0, sizeof(fis));

	is_write = (write != 0);
	has_ncq = (ps->flags & FLAG_HAS_NCQ) != 0;
	has_fua = (ps->flags & FLAG_HAS_FUA) != 0;

	fis.cf_dev = ATA_DEV_LBA;

	if (has_ncq) {
		if (is_write) {
			if (force && has_fua)
				fis.cf_dev |= ATA_DEV_FUA;
			fis.cf_cmd = ATA_CMD_WRITE_FPDMA_QUEUED;
		} else {
			fis.cf_cmd = ATA_CMD_READ_FPDMA_QUEUED;
		}
	} else {
		if (is_write) {
			fis.cf_cmd = (force && has_fua) ? ATA_CMD_WRITE_DMA_FUA_EXT
			                                : ATA_CMD_WRITE_DMA_EXT;
		} else {
			fis.cf_cmd = ATA_CMD_READ_DMA_EXT;
		}
	}

	fis.cf_lba = start_lba & 0x00FFFFFFUL;
	fis.cf_lba_exp = (start_lba >> 24) & 0x00FFFFFFUL;
	fis.cf_sec = sec_cnt & 0xFF;
	fis.cf_sec_exp = (sec_cnt >> 8) & 0xFF;

	port_set_cmd(ps, cmd, &fis, NULL, prdt, nr_prds, is_write);

	return port_exec(ps, cmd, ahci_transfer_timeout);
}

/*===========================================================================*
 *				gen_identify				     *
 *===========================================================================*/
static int gen_identify(struct port_state *ps, int blocking)
{
	cmd_fis_t fis;
	prd_t prd;
	const int slot = 0;
	const int prd_count = 1;
	const int write = FALSE;
	const int timeout = ahci_command_timeout;

	memset(&fis, 0, sizeof(fis));
	memset(&prd, 0, sizeof(prd));

	fis.cf_cmd = (ps->flags & FLAG_ATAPI) ? ATA_CMD_IDENTIFY_PACKET : ATA_CMD_IDENTIFY;

	prd.vp_addr = ps->tmp_phys;
	prd.vp_size = ATA_ID_SIZE;

	port_set_cmd(ps, slot, &fis, NULL, &prd, prd_count, write);

	if (blocking)
		return port_exec(ps, slot, timeout);

	port_issue(ps, slot, timeout);

	return OK;
}

/*===========================================================================*
 *				gen_flush_wcache			     *
 *===========================================================================*/
static int gen_flush_wcache(struct port_state *ps)
{
	if (ps == NULL)
		return EINVAL;

	if (!(ps->flags & FLAG_HAS_FLUSH))
		return EINVAL;

	cmd_fis_t fis = { .cf_cmd = ATA_CMD_FLUSH_CACHE };

	port_set_cmd(ps, 0, &fis, NULL, NULL, 0, 0);

	return port_exec(ps, 0, ahci_flush_timeout);
}

/*===========================================================================*
 *				gen_get_wcache				     *
 *===========================================================================*/
static int gen_get_wcache(struct port_state *ps, int *val)
{
	int r;
	const u16_t *id;

	if (ps == NULL || val == NULL)
		return EINVAL;

	if ((ps->flags & FLAG_HAS_WCACHE) == 0)
		return EINVAL;

	r = gen_identify(ps, TRUE);
	if (r != OK)
		return r;

	id = (const u16_t *)ps->tmp_base;
	if (id == NULL)
		return EINVAL;

	*val = (id[ATA_ID_ENA0] & ATA_ID_ENA0_WCACHE) ? 1 : 0;

	return OK;
}

/*===========================================================================*
 *				gen_set_wcache				     *
 *===========================================================================*/
static int gen_set_wcache(struct port_state *ps, int enable)
{
	cmd_fis_t fis;
	clock_t timeout;

	if (ps == NULL)
		return EINVAL;

	if ((ps->flags & FLAG_HAS_WCACHE) == 0)
		return EINVAL;

	timeout = enable ? ahci_command_timeout : ahci_flush_timeout;

	fis = (cmd_fis_t){0};
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
	if (ct == NULL || fis == NULL) return 0;

	const cmd_fis_t *f = fis;

	memset(ct, 0, ATA_H2D_SIZE);
	ct[ATA_FIS_TYPE] = (u8_t)ATA_FIS_TYPE_H2D;
	ct[ATA_H2D_FLAGS] = (u8_t)ATA_H2D_FLAGS_C;
	ct[ATA_H2D_CMD] = (u8_t)f->cf_cmd;
	ct[ATA_H2D_LBA_LOW] = (u8_t)(f->cf_lba >> 0);
	ct[ATA_H2D_LBA_MID] = (u8_t)(f->cf_lba >> 8);
	ct[ATA_H2D_LBA_HIGH] = (u8_t)(f->cf_lba >> 16);
	ct[ATA_H2D_DEV] = (u8_t)f->cf_dev;
	ct[ATA_H2D_LBA_LOW_EXP] = (u8_t)(f->cf_lba_exp >> 0);
	ct[ATA_H2D_LBA_MID_EXP] = (u8_t)(f->cf_lba_exp >> 8);
	ct[ATA_H2D_LBA_HIGH_EXP] = (u8_t)(f->cf_lba_exp >> 16);
	ct[ATA_H2D_CTL] = (u8_t)f->cf_ctl;

	if (ATA_IS_FPDMA_CMD(f->cf_cmd)) {
		ct[ATA_H2D_FEAT] = (u8_t)f->cf_sec;
		ct[ATA_H2D_FEAT_EXP] = (u8_t)f->cf_sec_exp;
		ct[ATA_H2D_SEC] = (u8_t)((tag << ATA_SEC_TAG_SHIFT) & 0xFF);
		ct[ATA_H2D_SEC_EXP] = (u8_t)0;
	} else {
		ct[ATA_H2D_FEAT] = (u8_t)f->cf_feat;
		ct[ATA_H2D_FEAT_EXP] = (u8_t)f->cf_feat_exp;
		ct[ATA_H2D_SEC] = (u8_t)f->cf_sec;
		ct[ATA_H2D_SEC_EXP] = (u8_t)f->cf_sec_exp;
	}

	return ATA_H2D_SIZE;
}

/*===========================================================================*
 *				ct_set_packet				     *
 *===========================================================================*/
static void ct_set_packet(u8_t *ct, u8_t packet[ATAPI_PACKET_SIZE])
{
    if (ct == NULL || packet == NULL) {
        return;
    }

    u8_t *dest = &ct[AHCI_CT_PACKET_OFF];
    memmove(dest, packet, ATAPI_PACKET_SIZE);
}

/*===========================================================================*
 *				ct_set_prdt				     *
 *===========================================================================*/
static void ct_set_prdt(u8_t *ct, prd_t *prdt, int nr_prds)
{
	if (ct == NULL || prdt == NULL || nr_prds <= 0) {
		return;
	}

	u32_t *entry = (u32_t *)&ct[AHCI_CT_PRDT_OFF];
	int i;

	for (i = 0; i < nr_prds; i++) {
		entry[0] = prdt[i].vp_addr;
		entry[1] = 0;
		entry[2] = 0;
		entry[3] = prdt[i].vp_size - 1;
		entry += 4;
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
	int is_ncq;
	int has_packet;
	int prefetchable;
	u32_t dword_len;
	u32_t entry0;
	u32_t prdtl;

	assert(ps != NULL);
	assert(fis != NULL);
	assert(cmd >= 0);
	assert(nr_prds >= 0);
	assert(nr_prds <= NR_PRDS);

	is_ncq = ATA_IS_FPDMA_CMD(fis->cf_cmd);
	if (is_ncq) {
		ps->flags |= FLAG_NCQ_MODE;
	} else {
		assert(!ps->pend_mask);
		ps->flags &= ~FLAG_NCQ_MODE;
	}

	ct = ps->ct_base[cmd];
	assert(ct != NULL);

	size = ct_set_fis(ct, fis, cmd);
	assert(size > 0);
	assert((size % sizeof(u32_t)) == 0);
	dword_len = (u32_t)(size / sizeof(u32_t));

	has_packet = (packet != NULL);
	if (has_packet)
		ct_set_packet(ct, packet);

	if (nr_prds > 0)
		assert(prdt != NULL);
	ct_set_prdt(ct, prdt, nr_prds);

	assert(ps->cl_base != NULL);
	cl = &ps->cl_base[cmd * AHCI_CL_ENTRY_DWORDS];

	memset(cl, 0, AHCI_CL_ENTRY_SIZE);

	prdtl = (u32_t)nr_prds;
	prefetchable = (!is_ncq && (nr_prds > 0 || has_packet)) ? AHCI_CL_PREFETCHABLE : 0;

	entry0 = (prdtl << AHCI_CL_PRDTL_SHIFT)
	       | (u32_t)prefetchable
	       | (write ? AHCI_CL_WRITE : 0)
	       | (has_packet ? AHCI_CL_ATAPI : 0)
	       | (dword_len << AHCI_CL_CFL_SHIFT);

	cl[0] = entry0;
	cl[2] = ps->ct_phys[cmd];
}

/*===========================================================================*
 *				port_finish_cmd				     *
 *===========================================================================*/
static void port_finish_cmd(struct port_state *ps, int cmd, int result)
{
	if (ps == NULL)
		return;

	if (cmd < 0 || cmd >= ps->queue_depth)
		return;

	dprintf(V_REQ, ("%s: command %d %s\n", ahci_portname(ps),
		cmd, (result == RESULT_SUCCESS) ? "succeeded" : "failed"));

	ps->cmd_info[cmd].result = result;

	{
		unsigned int bit_width = (unsigned int)(sizeof(ps->pend_mask) * 8U);
		if ((unsigned int)cmd < bit_width) {
			if (bit_width <= 32U) {
				unsigned long mask = 1UL << (unsigned int)cmd;
				if (ps->pend_mask & mask) {
					ps->pend_mask &= ~mask;
				}
			} else {
				unsigned long long mask = 1ULL << (unsigned int)cmd;
				unsigned long long pm = (unsigned long long)ps->pend_mask;
				if (pm & mask) {
					pm &= ~mask;
					ps->pend_mask = (unsigned long long)pm;
				}
			}
		}
	}

	if (ps->state != STATE_WAIT_ID)
		blockdriver_mt_wakeup(ps->cmd_info[cmd].tid);
}

/*===========================================================================*
 *				port_fail_cmds				     *
 *===========================================================================*/
static void port_fail_cmds(struct port_state *ps)
{
	int i;
	int limit = ps->queue_depth;
	int max_bits = (int)(sizeof(ps->pend_mask) * 8U);

	if (limit > max_bits)
		limit = max_bits;

	for (i = 0; ps->pend_mask != 0 && i < limit; i++) {
		unsigned long long snapshot = (unsigned long long)ps->pend_mask;

		if (((snapshot >> i) & 1ULL) != 0ULL)
			port_finish_cmd(ps, i, RESULT_FAILURE);
	}
}

/*===========================================================================*
 *				port_check_cmds				     *
 *===========================================================================*/
static void port_check_cmds(struct port_state *ps)
{
    u32_t mask;
    const u32_t done;
    unsigned int i;
    unsigned int depth;
    unsigned int max;

    if (ps == NULL) {
        return;
    }

    mask = port_read(ps, (ps->flags & FLAG_NCQ_MODE) ? AHCI_PORT_SACT : AHCI_PORT_CI);
    {
        u32_t active_inv = ~mask;
        done = (u32_t)(ps->pend_mask & active_inv);
    }

    if (done == 0U) {
        return;
    }

    depth = (ps->queue_depth > 0) ? (unsigned int)ps->queue_depth : 0U;
    max = (depth < 32U) ? depth : 32U;

    for (i = 0; i < max; i++) {
        u32_t bit = (u32_t)(1U << i);
        if ((done & bit) != 0U) {
            port_finish_cmd(ps, (int)i, RESULT_SUCCESS);
        }
    }
}

/*===========================================================================*
 *				port_find_cmd				     *
 *===========================================================================*/
static int port_find_cmd(struct port_state *ps)
{
    int i;
    int max_bits = (int)(sizeof(ps->pend_mask) * 8);
    const int shift_bits = (int)(sizeof(unsigned long long) * 8);

    assert(ps != NULL);
    assert(ps->queue_depth >= 0);
    assert(ps->queue_depth <= max_bits);
    assert(ps->queue_depth <= shift_bits);

    for (i = 0; i < ps->queue_depth; i++) {
        unsigned long long mask = 1ULL << i;
        if (((unsigned long long)ps->pend_mask & mask) == 0ULL) {
            break;
        }
    }

    assert(i < ps->queue_depth);

    return i;
}

/*===========================================================================*
 *				port_get_padbuf				     *
 *===========================================================================*/
static int port_get_padbuf(struct port_state *ps, size_t size)
{
	if (ps->pad_base != NULL && ps->pad_size >= size)
		return OK;

	if (ps->pad_base != NULL) {
		free_contig(ps->pad_base, ps->pad_size);
		ps->pad_base = NULL;
		ps->pad_size = 0;
		ps->pad_phys = 0;
	}

	ps->pad_size = size;
	ps->pad_base = alloc_contig(ps->pad_size, 0, &ps->pad_phys);

	if (ps->pad_base == NULL) {
		const char *name = ahci_portname(ps);
		dprintf(V_ERR, ("%s: unable to allocate a padding buffer of size %lu\n",
			name, (unsigned long) size));
		return ENOMEM;
	}

	{
		const char *name = ahci_portname(ps);
		dprintf(V_INFO, ("%s: allocated padding buffer of size %lu\n",
			name, (unsigned long) size));
	}

	return OK;
}

/*===========================================================================*
 *				sum_iovec				     *
 *===========================================================================*/
static int sum_iovec(struct port_state *ps, endpoint_t endpt,
	iovec_s_t *iovec, int nr_req, vir_bytes *total)
{
	vir_bytes bytes = 0, size;
	const vir_bytes limit = (vir_bytes)LONG_MAX;
	const char *pname = ps ? ahci_portname(ps) : "unknown";
	int i;

	if (total == NULL || nr_req < 0 || (iovec == NULL && nr_req != 0))
		return EINVAL;

	for (i = 0; i < nr_req; i++) {
		size = iovec[i].iov_size;

		if (size == 0 || (size & 1) != 0 || size > limit) {
			dprintf(V_ERR, ("%s: bad size %lu in iovec from %d\n",
				pname, (unsigned long)size, endpt));
			return EINVAL;
		}

		if (bytes > limit - size) {
			dprintf(V_ERR, ("%s: iovec size overflow from %d\n",
				pname, endpt));
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
	vir_bytes bytes, trail = 0;
	int i = 0, r, pcount = 0, nr_prds = 0;
	int want_trail_slot, max_prd_capacity, max_vvec;

	if (ps == NULL || prdt == NULL) return EINVAL;
	if (size > 0 && (iovec == NULL || nr_req <= 0)) return EINVAL;
	if (ps->sector_size == 0) return EINVAL;

	if (lead > 0) {
		if (nr_prds >= NR_PRDS) return E2BIG;
		r = port_get_padbuf(ps, ps->sector_size);
		if (r != OK) return r;

		prdt[nr_prds].vp_addr = ps->pad_phys;
		prdt[nr_prds].vp_size = lead;
		nr_prds++;
	}

	trail = (ps->sector_size - ((lead + size) % ps->sector_size)) % ps->sector_size;
	want_trail_slot = (trail > 0) ? 1 : 0;

	max_prd_capacity = NR_PRDS - nr_prds - want_trail_slot;
	if (max_prd_capacity < 0) max_prd_capacity = 0;

	for (i = 0; i < nr_req && size > 0 && i < max_prd_capacity && i < NR_PRDS; i++) {
		bytes = MIN(iovec[i].iov_size, size);

		if (endpt == SELF)
			vvec[i].vv_addr = (vir_bytes) iovec[i].iov_grant;
		else
			vvec[i].vv_grant = iovec[i].iov_grant;

		vvec[i].vv_size = bytes;

		size -= bytes;
	}

	max_vvec = i;

	if (max_vvec > 0) {
		pcount = max_vvec;

		r = sys_vumap(endpt, vvec, max_vvec, 0, write ? VUA_READ : VUA_WRITE,
			&prdt[nr_prds], &pcount);
		if (r != OK) {
			dprintf(V_ERR, ("%s: unable to map memory from %d (%d)\n",
				ahci_portname(ps), endpt, r));
			return r;
		}

		if (pcount <= 0 || pcount > max_vvec) return EINVAL;

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
		}
	}

	if (trail > 0) {
		if (nr_prds >= NR_PRDS) return E2BIG;
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
	u64_t available;
	int r, cmd;

	r = sum_iovec(ps, endpt, iovec, nr_req, &size);
	if (r != OK) return r;

	dprintf(V_REQ, ("%s: %s for %lu bytes at pos %llx\n",
		ahci_portname(ps), write ? "write" : "read", size, pos));

	assert(ps->state == STATE_GOOD_DEV);
	assert(ps->flags & FLAG_HAS_MEDIUM);
	assert(ps->sector_size > 0);

	if (size > MAX_TRANSFER) size = MAX_TRANSFER;

	available = (pos < eof) ? (eof - pos) : 0;
	if ((u64_t)size > available) size = (vir_bytes)available;

	start_lba = pos / ps->sector_size;
	lead = (vir_bytes)(pos % ps->sector_size);

	if ((lead & 1) || (write && lead != 0)) {
		dprintf(V_ERR, ("%s: unaligned position from %d\n",
			ahci_portname(ps), endpt));
		return EINVAL;
	}

	if (write && (size % ps->sector_size) != 0) {
		dprintf(V_ERR, ("%s: unaligned size %lu from %d\n",
			ahci_portname(ps), size, endpt));
		return EINVAL;
	}

	{
		u64_t total = (u64_t)lead + (u64_t)size + (u64_t)ps->sector_size - 1;
		count = (unsigned int)(total / ps->sector_size);
	}

	{
		int prd_count = setup_prdt(ps, endpt, iovec, nr_req, size, lead,
			write, prdt);
		if (prd_count < 0) return prd_count;
		nr_prds = (unsigned int)prd_count;
	}

	cmd = port_find_cmd(ps);

	if (ps->flags & FLAG_ATAPI)
		r = atapi_transfer(ps, cmd, start_lba, count, write, prdt, nr_prds);
	else
		r = ata_transfer(ps, cmd, start_lba, count, write,
			(flags & BDEV_FORCEWRITE) != 0, prdt, nr_prds);

	if (r != OK) return r;

	return (ssize_t)size;
}

/*===========================================================================*
 *				port_hardreset				     *
 *===========================================================================*/
static void port_hardreset(struct port_state *ps)
{
	if (ps == NULL)
		return;

	{
		unsigned int usec = (unsigned int)((unsigned long)COMRESET_DELAY * 1000UL);
		port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_INIT);
		micro_delay(usec);
		port_write(ps, AHCI_PORT_SCTL, AHCI_PORT_SCTL_DET_NONE);
	}
}

/*===========================================================================*
 *				port_override				     *
 *===========================================================================*/
static void port_override(struct port_state *ps)
{
	if (ps == NULL)
		return;

	u32_t cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_CLO);

	SPIN_UNTIL((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CLO) == 0, PORTREG_DELAY);

	dprintf(V_INFO, ("%s: overridden\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_start				     *
 *===========================================================================*/
static void port_start(struct port_state *ps)
{
    if (ps == NULL) {
        return;
    }

    port_write(ps, AHCI_PORT_SERR, (u32_t)~0U);
    port_write(ps, AHCI_PORT_IS, (u32_t)~0U);

    u32_t cmd = port_read(ps, AHCI_PORT_CMD);
    if ((cmd & AHCI_PORT_CMD_ST) == 0) {
        port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_ST);
    }

    dprintf(V_INFO, ("%s: started\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_stop				     *
 *===========================================================================*/
static void port_stop(struct port_state *ps)
{
	u32_t cmd = port_read(ps, AHCI_PORT_CMD);

	if ((cmd & (AHCI_PORT_CMD_CR | AHCI_PORT_CMD_ST)) == 0) {
		return;
	}

	port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_ST);

	SPIN_UNTIL(!(port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_CR), PORTREG_DELAY);

	dprintf(V_INFO, ("%s: stopped\n", ahci_portname(ps)));
}

/*===========================================================================*
 *				port_restart				     *
 *===========================================================================*/
static void port_restart(struct port_state *ps)
{
	if (ps == NULL) {
		return;
	}

	port_fail_cmds(ps);
	port_stop(ps);

	const unsigned int mask = AHCI_PORT_TFD_STS_BSY | AHCI_PORT_TFD_STS_DRQ;
	const unsigned int tfd = port_read(ps, AHCI_PORT_TFD);

	if ((tfd & mask) != 0U) {
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
	if (buf == NULL) {
		return;
	}

	int left = start;
	int right = end;

	if (right < left) {
		return;
	}

	const u16_t DOUBLE_SPACE = 0x2020;
	const u16_t SPACE_MASK = 0x00FF;
	const u16_t SPACE = 0x0020;

	while (right >= left && buf[right] == DOUBLE_SPACE) {
		right--;
	}

	int high_only_index = -1;
	if (right >= left && (buf[right] & SPACE_MASK) == SPACE) {
		high_only_index = right;
		right--;
	}

	for (int i = left; i <= right; i++) {
		int hi = (int)((buf[i] >> 8) & 0x00FF);
		int lo = (int)(buf[i] & 0x00FF);
		printf("%c%c", hi, lo);
	}

	if (high_only_index >= 0) {
		int hi = (int)((buf[high_only_index] >> 8) & 0x00FF);
		printf("%c", hi);
	}
}

/*===========================================================================*
 *				port_id_check				     *
 *===========================================================================*/
static void port_id_check(struct port_state *ps, int success)
{
	u16_t *buf;
	const char *portname;

	if (ps == NULL) {
		return;
	}

	assert(ps->state == STATE_WAIT_ID);

	ps->flags &= ~FLAG_BUSY;
	cancel_timer(&ps->cmd_info[0].timer);

	portname = ahci_portname(ps);

	if (!success) {
		if (!(ps->flags & FLAG_ATAPI) &&
		    port_read(ps, AHCI_PORT_SIG) != ATA_SIG_ATA) {
			dprintf(V_INFO, ("%s: may not be ATA, trying ATAPI\n", portname));
			ps->flags |= FLAG_ATAPI;
			(void) gen_identify(ps, FALSE);
			return;
		}

		dprintf(V_ERR, ("%s: unable to identify\n", portname));
	}

	buf = (u16_t *) ps->tmp_base;

	if (success) {
		if (ps->flags & FLAG_ATAPI) {
			success = atapi_id_check(ps, buf);
		} else {
			success = ata_id_check(ps, buf);
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
		printf("%s: ATA%s, ", portname, (ps->flags & FLAG_ATAPI) ? "PI" : "");
		print_string(buf, 27, 46);

		if (ahci_verbose >= V_DEV) {
			printf(" (");
			print_string(buf, 10, 19);
			printf(", ");
			print_string(buf, 23, 26);
			printf(")");
		}

		if (ps->flags & FLAG_HAS_MEDIUM) {
			unsigned long long mb = (unsigned long long) ps->lba_count;
			mb *= (unsigned long long) ps->sector_size;
			mb /= (1024ULL * 1024ULL);
			printf(", %u byte sectors, %llu MB size", ps->sector_size, mb);
		}

		printf("\n");
	}
}

/*===========================================================================*
 *				port_connect				     *
 *===========================================================================*/
static void port_connect(struct port_state *ps)
{
	u32_t status, sig;
	const char *portname = ahci_portname(ps);

	dprintf(V_INFO, ("%s: device connected\n", portname));

	port_start(ps);

	status = port_read(ps, AHCI_PORT_SSTS) & AHCI_PORT_SSTS_DET_MASK;

	if (status != AHCI_PORT_SSTS_DET_PHY) {
		dprintf(V_ERR, ("%s: device vanished!\n", portname));

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
	if (ps == NULL)
		return;

	dprintf(V_INFO, ("%s: device disconnected\n", ahci_portname(ps)));

	ps->state = STATE_NO_DEV;
	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);
	ps->flags &= ~FLAG_BUSY;

	port_fail_cmds(ps);

	ps->flags |= FLAG_BARRIER;

	if (ps->device != NULL)
		blockdriver_mt_set_workers(ps->device, 1);
}

/*===========================================================================*
 *				port_dev_check				     *
 *===========================================================================*/
static void port_dev_check(struct port_state *ps)
{
	u32_t status, tfd;
	const char *pname;

	assert(ps->state == STATE_WAIT_DEV);

	status = port_read(ps, AHCI_PORT_SSTS) & AHCI_PORT_SSTS_DET_MASK;

	pname = ahci_portname(ps);
	dprintf(V_DEV, ("%s: polled status %u\n", pname, status));

	if (status == AHCI_PORT_SSTS_DET_PHY) {
		tfd = port_read(ps, AHCI_PORT_TFD);

		if ((tfd & (AHCI_PORT_TFD_STS_BSY | AHCI_PORT_TFD_STS_DRQ)) == 0) {
			port_connect(ps);
			return;
		}
	}

	if (status == AHCI_PORT_SSTS_DET_PHY || status == AHCI_PORT_SSTS_DET_DET) {
		if (ps->left > 0) {
			ps->left--;
			set_timer(&ps->cmd_info[0].timer, ahci_device_delay,
				port_timeout, BUILD_ARG(ps - port_state, 0));
			return;
		}
	}

	dprintf(V_INFO, ("%s: device not ready\n", pname));

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
	if (ps == NULL)
		return;

	u32_t smask, emask;

	if (ps->state == STATE_NO_PORT) {
		dprintf(V_ERR, ("%s: interrupt for invalid port!\n", ahci_portname(ps)));
		return;
	}

	smask = port_read(ps, AHCI_PORT_IS);
	emask = smask & port_read(ps, AHCI_PORT_IE);

	port_write(ps, AHCI_PORT_IS, smask);

	dprintf(V_REQ, ("%s: interrupt (%08x)\n", ahci_portname(ps), smask));

	port_check_cmds(ps);

	if (emask & AHCI_PORT_IS_PCS) {
		port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_X);
		dprintf(V_DEV, ("%s: device attached\n", ahci_portname(ps)));

		if (ps->state == STATE_SPIN_UP)
			cancel_timer(&ps->cmd_info[0].timer);

		if (ps->state == STATE_SPIN_UP || ps->state == STATE_NO_DEV) {
			ps->state = STATE_WAIT_DEV;
			ps->left = ahci_device_checks;
			port_dev_check(ps);
		} else if (ps->state != STATE_WAIT_DEV) {
			assert(0);
		}
		return;
	}

	if (emask & AHCI_PORT_IS_PRCS) {
		port_write(ps, AHCI_PORT_SERR, AHCI_PORT_SERR_DIAG_N);
		dprintf(V_DEV, ("%s: device detached\n", ahci_portname(ps)));

		if (ps->state == STATE_WAIT_ID || ps->state == STATE_GOOD_DEV)
			port_stop(ps);

		if (ps->state == STATE_WAIT_ID || ps->state == STATE_GOOD_DEV || ps->state == STATE_BAD_DEV) {
			port_disconnect(ps);
			port_hardreset(ps);
			return;
		}

		assert(0);
		return;
	}

	if (smask & AHCI_PORT_IS_MASK) {
		u32_t tfd = port_read(ps, AHCI_PORT_TFD);
		int has_error = (tfd & (AHCI_PORT_TFD_STS_ERR | AHCI_PORT_TFD_STS_DF)) != 0;
		int success = !has_error;

		if (has_error || (smask & AHCI_PORT_IS_RESTART))
			port_restart(ps);

		if (ps->state == STATE_WAIT_ID)
			port_id_check(ps, success);
	}
}

/*===========================================================================*
 *				port_timeout				     *
 *===========================================================================*/
static void port_timeout(int arg)
{
	const int port = GET_PORT(arg);
	const int cmd = GET_TAG(arg);
	struct port_state *ps;

	if (port < 0 || port >= hba_state.nr_ports) {
		dprintf(V_ERR, ("ahci: invalid port in timeout: %d\n", port));
		return;
	}

	ps = &port_state[port];

	if (ps->flags & FLAG_SUSPENDED) {
		assert(cmd == 0);
		blockdriver_mt_wakeup(ps->cmd_info[0].tid);
	}

	switch (ps->state) {
	case STATE_SPIN_UP: {
		const char *name = ahci_portname(ps);
		if (port_read(ps, AHCI_PORT_IS) & AHCI_PORT_IS_PCS) {
			dprintf(V_INFO, ("%s: bad controller, no interrupt\n", name));
			ps->state = STATE_WAIT_DEV;
			ps->left = ahci_device_checks;
			port_dev_check(ps);
		} else {
			dprintf(V_INFO, ("%s: spin-up timeout\n", name));
			ps->state = STATE_NO_DEV;
			ps->flags &= ~FLAG_BUSY;
		}
		return;
	}
	case STATE_WAIT_DEV:
		port_dev_check(ps);
		return;
	default: {
		const char *name = ahci_portname(ps);
		const int prev_state = ps->state;
		dprintf(V_ERR, ("%s: timeout\n", name));
		port_restart(ps);
		if (prev_state == STATE_WAIT_ID)
			port_id_check(ps, FALSE);
		return;
	}
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

    if (!(ps->flags & FLAG_BUSY)) {
        return;
    }

    ps->flags |= FLAG_SUSPENDED;

    while (ps->flags & FLAG_BUSY) {
        blockdriver_mt_sleep();
    }

    ps->flags &= ~FLAG_SUSPENDED;
}

/*===========================================================================*
 *				port_issue				     *
 *===========================================================================*/
static void port_issue(struct port_state *ps, int cmd, clock_t timeout)
{
    if (ps == NULL) {
        return;
    }

    const unsigned int max_bits = (unsigned int)(sizeof(unsigned int) * 8u);
    if (cmd < 0 || (unsigned int)cmd >= max_bits) {
        return;
    }

    const unsigned int mask = 1u << (unsigned int)cmd;

    if ((ps->flags & FLAG_HAS_NCQ) != 0) {
        port_write(ps, AHCI_PORT_SACT, mask);
    }

    __insn_barrier();

    port_write(ps, AHCI_PORT_CI, mask);

    ps->pend_mask |= mask;

    set_timer(&ps->cmd_info[cmd].timer, timeout, port_timeout,
              BUILD_ARG(ps - port_state, cmd));
}

/*===========================================================================*
 *				port_exec				     *
 *===========================================================================*/
static int port_exec(struct port_state *ps, int cmd, clock_t timeout)
{
	struct cmd_info *ci;
	int result;

	assert(ps != NULL);
	assert(cmd >= 0);

	port_issue(ps, cmd, timeout);

	ci = &ps->cmd_info[cmd];
	ci->tid = blockdriver_mt_get_tid();

	blockdriver_mt_sleep();

	cancel_timer(&ci->timer);

	assert(!(ps->flags & FLAG_BUSY));

	result = ci->result;

	dprintf(V_REQ, ("%s: end of command -- %s\n",
	    ahci_portname(ps),
	    (result == RESULT_FAILURE) ? "failure" : "success"));

	return (result == RESULT_FAILURE) ? EIO : OK;
}

/*===========================================================================*
 *				port_alloc				     *
 *===========================================================================*/
static size_t safe_add_size(size_t a, size_t b)
{
	size_t c = a + b;
	if (c < a)
		panic("size overflow");
	return c;
}

static size_t align_up(size_t value, size_t alignment)
{
	size_t tmp;
	if (alignment == 0)
		return value;
	tmp = safe_add_size(value, alignment - 1);
	return tmp - (tmp % alignment);
}

static void port_alloc(struct port_state *ps)
{
	size_t fis_off, tmp_off, ct_off, mem_size;
	size_t ct_offs[NR_CMDS];
	u32_t cmd;
	size_t i;

	if (ps == NULL)
		panic("port_alloc: invalid port state");

	fis_off = align_up(AHCI_CL_SIZE, AHCI_FIS_SIZE);
	tmp_off = align_up(safe_add_size(fis_off, AHCI_FIS_SIZE), AHCI_TMP_ALIGN);

	ct_off = safe_add_size(tmp_off, AHCI_TMP_SIZE);
	for (i = 0; i < NR_CMDS; i++) {
		ct_off = align_up(ct_off, AHCI_CT_ALIGN);
		ct_offs[i] = ct_off;
		ct_off = safe_add_size(ct_off, AHCI_CT_SIZE);
	}
	mem_size = ct_off;

	ps->mem_base = alloc_contig(mem_size, AC_ALIGN4K, &ps->mem_phys);
	if (ps->mem_base == NULL)
		panic("unable to allocate port memory");
	ps->mem_size = mem_size;
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

	if (ps == NULL)
		return;

	cmd = port_read(ps, AHCI_PORT_CMD);

	if ((cmd & (AHCI_PORT_CMD_FR | AHCI_PORT_CMD_FRE)) != 0) {
		if ((cmd & AHCI_PORT_CMD_FRE) != 0) {
			port_write(ps, AHCI_PORT_CMD, cmd & ~AHCI_PORT_CMD_FRE);
		}
		SPIN_UNTIL((port_read(ps, AHCI_PORT_CMD) & AHCI_PORT_CMD_FR) == 0, PORTREG_DELAY);
	}

	if (ps->pad_base != NULL) {
		free_contig(ps->pad_base, ps->pad_size);
		ps->pad_base = NULL;
		ps->pad_size = 0;
	}

	if (ps->mem_base != NULL) {
		free_contig(ps->mem_base, ps->mem_size);
		ps->mem_base = NULL;
		ps->mem_size = 0;
	}
}

/*===========================================================================*
 *				port_init				     *
 *===========================================================================*/
static void port_init(struct port_state *ps)
{
	u32_t cmd;
	int i;
	int port_idx;
	u8_t *base;
	u8_t *port_base;

	if (ps == NULL)
		return;

	ps->queue_depth = 1;
	ps->state = STATE_SPIN_UP;
	ps->flags = FLAG_BUSY;
	ps->sector_size = 0;
	ps->open_count = 0;
	ps->pend_mask = 0;

	for (i = 0; i < NR_CMDS; i++)
		init_timer(&ps->cmd_info[i].timer);

	port_idx = (int)(ps - port_state);

	base = (u8_t *)hba_state.base;
	port_base = base + AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * port_idx;
	ps->reg = (u32_t *)port_base;

	port_alloc(ps);

	port_write(ps, AHCI_PORT_IE, AHCI_PORT_IE_PCE);

	cmd = port_read(ps, AHCI_PORT_CMD);
	port_write(ps, AHCI_PORT_CMD, cmd | AHCI_PORT_CMD_SUD);

	port_hardreset(ps);

	set_timer(&ps->cmd_info[0].timer, ahci_spinup_timeout, port_timeout, BUILD_ARG(port_idx, 0));
}

/*===========================================================================*
 *				ahci_probe				     *
 *===========================================================================*/
static int ahci_probe(int skip)
{
	int result, devind;
	u16_t vid, did;

	if (skip < 0)
		return -1;

	pci_init();

	result = pci_first_dev(&devind, &vid, &did);
	if (result <= 0)
		return -1;

	for (; skip > 0; --skip) {
		result = pci_next_dev(&devind, &vid, &did);
		if (result <= 0)
			return -1;
	}

	pci_reserve(devind);

	return devind;
}

/*===========================================================================*
 *				ahci_reset				     *
 *===========================================================================*/
static void ahci_reset(void)
{
	u32_t ghc = hba_read(AHCI_HBA_GHC);
	u32_t ghc_with_ae = ghc | AHCI_HBA_GHC_AE;
	u32_t ghc_with_hr = ghc_with_ae | AHCI_HBA_GHC_HR;
	u32_t ghc_after;

	hba_write(AHCI_HBA_GHC, ghc_with_ae);
	(void)hba_read(AHCI_HBA_GHC);

	hba_write(AHCI_HBA_GHC, ghc_with_hr);
	(void)hba_read(AHCI_HBA_GHC);

	SPIN_UNTIL(!(hba_read(AHCI_HBA_GHC) & AHCI_HBA_GHC_HR), RESET_DELAY);

	ghc_after = hba_read(AHCI_HBA_GHC);
	if (ghc_after & AHCI_HBA_GHC_HR)
		panic("unable to reset HBA");
}

/*===========================================================================*
 *				ahci_init				     *
 *===========================================================================*/
static void ahci_init(int devind)
{
	u32_t base, size, cap, ghc, mask, vs;
	int r, port, ioflag;

	r = pci_get_bar(devind, PCI_BAR_6, &base, &size, &ioflag);
	if (r != OK)
		panic("unable to retrieve BAR: %d", r);

	if (ioflag)
		panic("invalid BAR type");

	if (size < AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE)
		panic("HBA memory size too small: %u", size);

	{
		u32_t max_size = AHCI_MEM_BASE_SIZE + AHCI_MEM_PORT_SIZE * NR_PORTS;
		size = MIN(size, max_size);
	}

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

	vs = hba_read(AHCI_HBA_VS);

	dprintf(V_INFO, ("AHCI%u: HBA v%d.%d%d, %ld ports, %ld commands, "
		"%s queuing, IRQ %d\n",
		ahci_instance,
		(int)(vs >> 16),
		(int)((vs >> 8) & 0xFF),
		(int)(vs & 0xFF),
		(long)(((cap >> AHCI_HBA_CAP_NP_SHIFT) & AHCI_HBA_CAP_NP_MASK) + 1),
		(long)(((cap >> AHCI_HBA_CAP_NCS_SHIFT) & AHCI_HBA_CAP_NCS_MASK) + 1),
		hba_state.has_ncq ? "supports" : "no", hba_state.irq));

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
	int r;
	int port;

	for (port = 0; port < hba_state.nr_ports; port++) {
		struct port_state *ps = &port_state[port];

		if (ps->state == STATE_NO_PORT)
			continue;

		port_stop(ps);
		port_free(ps);
	}

	ahci_reset();

	r = vm_unmap_phys(SELF, (void *) hba_state.base, hba_state.size);
	if (r != OK)
		panic("unable to unmap HBA memory: %d", r);

	r = sys_irqrmpolicy(&hba_state.hook_id);
	if (r != OK)
		panic("unable to deregister IRQ: %d", r);
}

/*===========================================================================*
 *				ahci_alarm				     *
 *===========================================================================*/
static void ahci_alarm(const clock_t stamp)
{
	expire_timers(stamp);
}

/*===========================================================================*
 *				ahci_intr				     *
 *===========================================================================*/
static void ahci_intr(unsigned int UNUSED(unused_mask))
{
	u32_t status_mask;
	int r;
	int port;
	int nr_ports;
	int bit_width;
	int max_ports;
	struct port_state *ps;

	status_mask = hba_read(AHCI_HBA_IS);
	nr_ports = hba_state.nr_ports;
	bit_width = (int)(sizeof(status_mask) * 8);
	max_ports = nr_ports < bit_width ? nr_ports : bit_width;

	for (port = 0; port < max_ports; port++) {
		if ((status_mask & (1u << port)) != 0u) {
			ps = &port_state[port];
			port_intr(ps);
			if ((ps->flags & (FLAG_SUSPENDED | FLAG_BUSY)) == FLAG_SUSPENDED)
				blockdriver_mt_wakeup(ps->cmd_info[0].tid);
		}
	}

	hba_write(AHCI_HBA_IS, status_mask);

	r = sys_irqenable(&hba_state.hook_id);
	if (r != OK)
		panic("unable to enable IRQ: %d", r);
}

/*===========================================================================*
 *				ahci_get_params				     *
 *===========================================================================*/
static long parse_env_long(const char *name, long default_value, long min, long max)
{
	long value = default_value;
	if (env_parse(name, "d", 0, &value, min, max) != 0) {
		value = default_value;
	}
	return value;
}

static void ahci_get_params(void)
{
	size_t i, count;
	long v;

	v = parse_env_long("instance", 0, 0, 255);
	ahci_instance = (int)v;

	v = parse_env_long("ahci_verbose", V_ERR, V_NONE, V_REQ);
	ahci_verbose = (int)v;

	count = sizeof(ahci_timevar) / sizeof(ahci_timevar[0]);
	for (i = 0; i < count; i++) {
		v = parse_env_long(ahci_timevar[i].name, ahci_timevar[i].default_ms, 1, LONG_MAX);
		*ahci_timevar[i].ptr = millis_to_hz(v);
	}

	ahci_device_delay = millis_to_hz(DEVICE_DELAY);
	if (ahci_device_delay > 0) {
		ahci_device_checks = (ahci_device_timeout + ahci_device_delay - 1) / ahci_device_delay;
	} else {
		ahci_device_checks = 0;
	}
}

/*===========================================================================*
 *				ahci_set_mapping			     *
 *===========================================================================*/
static void ahci_set_mapping(void)
{
	char key[16];
	char val[32];
	char *p = NULL;
	unsigned int port = 0U;
	int i;
	int j = 0;

	strlcpy(key, "ahci0_map", sizeof(key));
	key[4] = (char)('0' + ahci_instance);

	for (i = 0; i < NR_PORTS && j < MAX_DRIVES; i++) {
		if (port_state[i].state != STATE_NO_PORT) {
			ahci_map[j++] = i;
		}
	}

	for (; j < MAX_DRIVES; j++) {
		ahci_map[j] = NO_PORT;
	}

	if (env_get_param(key, val, sizeof(val)) == OK) {
		p = val;

		for (i = 0; i < MAX_DRIVES; i++) {
			if (*p != '\0') {
				char *end = p;
				unsigned long v = strtoul(p, &end, 0);
				p = end;
				if (*p != '\0') p++;
				if (NR_PORTS > 0) {
					port = (unsigned int)v % NR_PORTS;
					ahci_map[i] = (int)port;
				} else {
					ahci_map[i] = NO_PORT;
				}
			} else {
				ahci_map[i] = NO_PORT;
			}
		}
	}

	for (i = 0; i < MAX_DRIVES; i++) {
		int mapped_port = ahci_map[i];
		if (mapped_port != NO_PORT) {
			port_state[mapped_port].device = i;
		}
	}
}

/*===========================================================================*
 *				sef_cb_init_fresh			     *
 *===========================================================================*/
static int sef_cb_init_fresh(int type, sef_init_info_t *UNUSED(info))
{
	ahci_get_params();

	const int devind = ahci_probe(ahci_instance);
	if (devind < 0) {
		panic("no matching device found");
		return EIO;
	}

	ahci_init(devind);
	ahci_set_mapping();
	blockdriver_announce(type);

	return OK;
}

/*===========================================================================*
 *				sef_cb_signal_handler			     *
 *===========================================================================*/
static int are_any_ports_open(void)
{
	int port;
	int n = hba_state.nr_ports;

	if (n <= 0) {
		return 0;
	}

	for (port = 0; port < n; ++port) {
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
static char *ahci_portname(struct port_state *ps)
{
	static char name[] = "AHCI0-P00";
	const int idx_inst = 4;
	const int idx_kind = 6;
	const int idx_d1 = 7;
	const int idx_d2 = 8;
	const int idx_term = 9;

	name[idx_inst] = (char)('0' + ahci_instance);

	if (ps == NULL) {
		name[idx_kind] = 'P';
		name[idx_d1] = '0';
		name[idx_d2] = '0';
		name[idx_term] = '\0';
		return name;
	}

	if (ps->device == NO_DEVICE) {
		int port_idx = (int)(ps - port_state);
		if (port_idx < 0) {
			port_idx = 0;
		}
		name[idx_kind] = 'P';
		name[idx_d1] = (char)('0' + (port_idx / 10));
		name[idx_d2] = (char)('0' + (port_idx % 10));
		name[idx_term] = '\0';
	} else {
		name[idx_kind] = 'D';
		name[idx_d1] = (char)('0' + ps->device);
		name[idx_d2] = '\0';
		name[idx_term] = '\0';
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

	if (dvp == NULL)
		return NULL;

	if (minor >= 0 && minor < NR_MINORS) {
		int index = minor;
		port = ahci_map[index / DEV_PER_DRIVE];
		if (port == NO_PORT)
			return NULL;
		ps = &port_state[port];
		*dvp = &ps->part[index % DEV_PER_DRIVE];
		return ps;
	}

	{
		devminor_t sub_minor = minor - MINOR_d0p0s0;
		if ((unsigned)sub_minor < NR_SUBDEVS) {
			port = ahci_map[sub_minor / SUB_PER_DRIVE];
			if (port == NO_PORT)
				return NULL;
			ps = &port_state[port];
			*dvp = &ps->subpart[sub_minor % SUB_PER_DRIVE];
			return ps;
		}
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
	struct port_state *ps;

	ps = ahci_get_port(minor);
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
			!!(ps->flags & FLAG_ATAPI));

		blockdriver_mt_set_workers(ps->device, ps->queue_depth);
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
	int port;

	ps = ahci_get_port(minor);
	if (ps == NULL) {
		dprintf(V_ERR, ("ahci_close: invalid port for minor %ld\n", (long)minor));
		return EINVAL;
	}

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
		(void)gen_flush_wcache(ps);
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
	dv = ahci_part(minor);

	if (ps == NULL || dv == NULL)
		return EIO;

	if (ps->state != STATE_GOOD_DEV || (ps->flags & FLAG_BARRIER))
		return EIO;

	if (count > NR_IOREQS)
		return EINVAL;

	if (position >= dv->dv_size)
		return OK;

	if (count > 0 && iovec == NULL)
		return EINVAL;

	pos = dv->dv_base + position;
	if (pos < dv->dv_base)
		return EIO;

	eof = dv->dv_base + dv->dv_size;
	if (eof < dv->dv_base)
		return EIO;

	return port_transfer(ps, pos, eof, endpt, (iovec_s_t *) iovec, count,
		do_write ? 1 : 0, flags);
}

/*===========================================================================*
 *				ahci_ioctl				     *
 *===========================================================================*/
static inline int ahci_port_ready(const struct port_state *ps)
{
	if (ps->state != STATE_GOOD_DEV) return EIO;
	if (ps->flags & FLAG_BARRIER) return EIO;
	return OK;
}

static int ahci_ioctl(devminor_t minor, unsigned long request,
	endpoint_t endpt, cp_grant_id_t grant, endpoint_t UNUSED(user_endpt))
{
	struct port_state *ps = ahci_get_port(minor);
	int r;
	int val = 0;

	if (ps == NULL)
		return EINVAL;

	switch (request) {
	case DIOCEJECT:
		r = ahci_port_ready(ps);
		if (r != OK)
			return r;

		if (!(ps->flags & FLAG_ATAPI))
			return EINVAL;

		return atapi_load_eject(ps, 0, FALSE);

	case DIOCOPENCT:
		return sys_safecopyto(endpt, grant, 0,
			(vir_bytes)&ps->open_count, sizeof(ps->open_count));

	case DIOCFLUSH:
		r = ahci_port_ready(ps);
		if (r != OK)
			return r;

		return gen_flush_wcache(ps);

	case DIOCSETWC:
		r = ahci_port_ready(ps);
		if (r != OK)
			return r;

		r = sys_safecopyfrom(endpt, grant, 0, (vir_bytes)&val, sizeof(val));
		if (r != OK)
			return r;

		return gen_set_wcache(ps, val);

	case DIOCGETWC:
		r = ahci_port_ready(ps);
		if (r != OK)
			return r;

		r = gen_get_wcache(ps, &val);
		if (r != OK)
			return r;

		return sys_safecopyto(endpt, grant, 0, (vir_bytes)&val, sizeof(val));
	}

	return ENOTTY;
}

/*===========================================================================*
 *				ahci_device				     *
 *===========================================================================*/
static int ahci_device(devminor_t minor, device_id_t *id)
{
	struct port_state *ps;
	struct device *dv = NULL;

	if (id == NULL)
		return EINVAL;

	ps = ahci_map_minor(minor, &dv);
	if (ps == NULL)
		return ENXIO;

	(void)dv;

	*id = ps->device;

	return OK;
}

/*===========================================================================*
 *				ahci_get_port				     *
 *===========================================================================*/
static struct port_state *ahci_get_port(devminor_t minor)
{
	struct device *dv;
	struct port_state *ps = ahci_map_minor(minor, &dv);

	if (ps == NULL) {
		panic("device mapping for minor %d disappeared", (int)minor);
	}

	(void)dv;
	return ps;
}

/*===========================================================================*
 *				main					     *
 *===========================================================================*/
int main(int argc, char **argv)
{
	env_setargs(argc, argv);
	sef_local_startup();
	(void)blockdriver_mt_task(&ahci_dtab);
	return 0;
}
