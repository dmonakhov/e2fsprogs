#ifndef _LARGEFILE_SOURCE
#define _LARGEFILE_SOURCE
#endif

#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "config.h"
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
extern char *optarg;
extern int optind;
#endif

#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "ext2fs/ext2_fs.h"
#include "ext2fs/ext2fs.h"
#include "ext2fs/rbtree.h"
#include "ext2fs/fiemap.h"
#include "e2p/e2p.h"
#include <linux/types.h>
#include <linux/unistd.h>

#include "jfs_user.h"
#include <uuid/uuid.h>

#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/time.h>
#include <sys/vfs.h>

#include "../version.h"
#include "nls-enable.h"

#ifndef O_DIRECTORY
#define O_DIRECTORY	00200000
#endif

#define __CHKMEM(p, who, commands...) \
do { \
	if (p == NULL) { \
		fprintf(stderr, "%s: Can't allocate memory: %m\n", who); \
		commands; \
	} \
} while(0);

#define CHKMEM(p, commands...)	\
	__CHKMEM(p, __func__, commands)

#define CHKMEM_PROG(p, commands...) \
	__CHKMEM(p, program_name, commands)

struct linux_dirent64
{
	__u64		d_ino;
	__s64		d_off;
	unsigned short	d_reclen;
	unsigned char	d_type;
	char		d_name[0];
};

static long __sys_getdents64(int fd, struct linux_dirent64 * dirp, int count)
{
	return syscall(__NR_getdents64, fd, dirp, count);
}

#if defined(HAVE_POSIX_FADVISE64)
#define posix_fadvise	posix_fadvise64
#elif defined(HAVE_FADVISE64)
#define posix_fadvise	fadvise64
#elif !defined(HAVE_POSIX_FADVISE)
#error posix_fadvise not available!
#endif

#ifndef EXT4_IOC_MOVE_EXT
struct move_extent {
	__s32 reserved;	/* original file descriptor */
	__u32 donor_fd;	/* donor file descriptor */
	__u64 orig_start;	/* logical start offset in block for orig */
	__u64 donor_start;	/* logical start offset in block for donor */
	__u64 len;	/* block length to be moved */
	__u64 moved_len;	/* moved block length */
};

#define EXT4_IOC_MOVE_EXT	_IOWR('f', 15, struct move_extent)
#endif


#ifndef HANDLE_LINUX_H
#define HANDLE_LINUX_H
#endif
#ifndef AT_EMPTY_PATH
#define AT_EMPTY_PATH		0x1000
#endif

#ifndef MAX_HANDLE_SZ
#define MAX_HANDLE_SZ 128
struct file_handle {
	__u32 handle_bytes;
	int handle_type;
	/* file identifier */
	unsigned char f_handle[0];
};

#if defined(__i386__)
#define __NR_name_to_handle_at  341
#define __NR_open_by_handle_at  342
#elif defined(__x86_64__)
#define __NR_name_to_handle_at  303
#define __NR_open_by_handle_at  304
#elif defined(__PPC64__)
#define __NR_name_to_handle_at  345
#define __NR_open_by_handle_at  346
#endif

static inline int name_to_handle_at(int mdirfd, const char *name,
				struct file_handle *handle,
				int *mnt_id,
				int flags)
{
	return syscall(__NR_name_to_handle_at, mdirfd, name, handle, mnt_id,
			flags);
}

static inline int open_by_handle_at(int mdirfd, struct file_handle *handle,
				    int flags)
{
	return syscall(__NR_open_by_handle_at, mdirfd, handle, flags);
}
#endif

/* Defragmentation terms.
 *
 * Main terms:
 * IntrA-file-Fragmentation(IAF): Fragmentation of a single file.
 * IntEr-file-Fragmentation(IEF): Fragmentation of a group of files
 * More info: "DFS: A de-fragmented file system", Woo Hyun Ahn, Kyungbaek Kim, Yongjin Choi, Daeyeon Park
 *
 *
 * Processing logic:
 *  pass1:  Sequential scan of the block bitmap tables. Collect used blocks (build spextent tree)
 *  pass2:  Scan filesystem hierarchy and collect extents ownership statistics.
 *  pass3:  Rescan filesystem tree prepare list of candidates for IEF defragmentation.
 *          Fix IntrA-file-Fragmentation(IAF) issues if discovered
 *
 *  pass4:  Process IEF list and perform actual defragmentation
 */
#define in_use(m, x)	(ext2fs_test_bit ((x), (m)))
#define ROOT_UID		0

/* Debug defines */
#define DEBUG_RB 1

#define E4D_ALLIGN(x,a) (((x) + (a)-1) & ~((a)-1))
#define E4D_ALLIGN_LOG(x,bit) (((x) + (1 << bit)-1) & ~((1 << bit)-1))

static const char * program_name = "e4defrag2";
static int dry_run = 0;
static int verbose = 0;
static int mem_usage = 0;
struct timeval time_start;
struct timeval time_end;
static unsigned debug_flag = 0;
static unsigned older_than = 0;
static unsigned int	current_uid;

/* Statistics */
unsigned long long dfstat_scanned_inodes = 0;
unsigned long long dfstat_scanned_directories = 0;
unsigned long long dfstat_scanned_extents = 0;
unsigned long long dfstat_scanned_blocks = 0;
unsigned long long dfstat_scanned_files = 0;
unsigned long long dfstat_directories = 0;
unsigned long long dfstat_donors = 0;
unsigned long long dfstat_iaf_candidates = 0;
unsigned long long dfstat_ief_candidates = 0;
unsigned long long dfstat_iaf_candidates_sz = 0;
unsigned long long dfstat_ief_candidates_sz = 0;
unsigned long long dfstat_iaf_groups = 0;
unsigned long long dfstat_ief_groups = 0;
unsigned long long dfstat_iaf_defragmented = 0;
unsigned long long dfstat_iaf_defragmented_sz = 0;
unsigned long long dfstat_iaf_defragmented_ex = 0;
unsigned long long dfstat_ief_defragmented = 0;
unsigned long long dfstat_ief_defragmented_sz = 0;
unsigned long long dfstat_ief_defragmented_ex = 0;
unsigned long long dfstat_ex_relocated = 0;
unsigned long long dfstat_ex_splitted = 0;
unsigned long long dfstat_ex_merged = 0;
unsigned long long dfstat_ex_collision = 0;
unsigned long long dfstat_fhandles = 0;
unsigned long long dfstat_spextents = 0;
unsigned long long dfstat_spextents_sz = 0;

enum debug_flags {
	DBG_RT = 0x1,
	DBG_SCAN = 0x2,
	DBG_MEMORY = 0x4,
	DBG_TREE = 0x8,
	DBG_FS = 0x10,
	DBG_FIEMAP = 0x20,
	DBG_BITMAP = 0x40,
	DBG_ERR = 0x80,
	DBG_CLUSTER = 0x100,
	DBG_TAG = 0x200,
	DBG_IAF = 0x400,
	DBG_IEF = 0x800,
};

/* The following macro is used for ioctl FS_IOC_FIEMAP
 * EXTENT_MAX_COUNT:	the maximum number of extents for exchanging between
 *			kernel-space and user-space per ioctl
 */
#define EXTENT_MAX_COUNT	512

struct fmap_extent
{
	__u32 len;	/* number of blocks covered by this extent */
	__u32 lblk;	/* logical block */
	__u64 pblk;	/* physical block */
};

#define DEFAULT_FMAP_CACHE_SZ 4
struct fmap_extent_cache
{
	unsigned fec_size;	/* map array size */
	unsigned fec_extents;	/* number of valid entries */
	struct fmap_extent fec_map[];
};

struct fmap_extent_stat
{
	unsigned hole;  /* Number of holes */
	unsigned frag;  /* Number of physical fragments */
	unsigned group; /* Number of groups, counter is speculative */
	unsigned local_ex; /* Number of extents from  the same group as inode */
	unsigned local_sz; /* Total len of local extents */
};

/* Used space and integral inode usage stats */
struct spextent
{
	struct rb_node		node;

	__u32 count;	/* number of blocks covered by this extent */
	__u64 start;	/* physical block */
	/* Integral statistics of owners of this extent */
	__u16 extents;	  /* total extents */
	__u16 dir_extents;/* total dir extents */
	__u16 local;	  /* inodes from the same group */
	__u16 ro_extents;
	__u16 old_extents;

	__u16 iaf_extents;
	__u16 found;	  /* number of blocks found for that extent */
	__u32 flags;
};

enum spext_flags
{
	SP_FL_FULL = 0x1,
	SP_FL_GOOD = 0x2,
	SP_FL_IEF_RELOC = 0x4,
	SP_FL_IGNORE = 0x8,
	SP_FL_LOCAL = 0x10,
	SP_FL_DIRLOCAL = 0x20,
	SP_FL_CSUM = 0x40,
	SP_FL_FMAP = 0x80,
	SP_FL_TP_RELOC = 0x100,
};

struct rb_fhandle
{
	struct rb_node node;
	__u32 flags;
	union	{
		__u32 csum;
		struct fmap_extent_cache *fec;
	};
	__u64 start;
	struct rb_fhandle *fs_next; /* Directory scan tree order */
	struct rb_fhandle *next;      /* processing list */
	unsigned char f_handle[0];
};

/*
 * This depends on the fact the struct rb_node is at the
 * beginning of the spextent structure.
 */
inline static struct spextent *node_to_spextent(struct rb_node *node)
{
	return (struct spextent *) node;
}

inline static struct rb_fhandle *node_to_fhandle(struct rb_node *node)
{
	return (struct rb_fhandle *) node;
}


struct donor_info
{
	int fd;
	int dir;
	__u64 offset; /* start working set in blocks */
	__u64 length;  /* File length in blocks */
	unsigned int is_local:1;
	struct fmap_extent_stat fest;
	struct fmap_extent_cache *fec;
};

#define GROUP_DIR_CACHE_SZ	32
struct group_info
{
	unsigned long		flags;
	struct rb_root		fh_root;
	struct rb_fhandle	*fs_head;
	struct rb_fhandle	*fs_tail;
	struct rb_fhandle	*next;
	__u32			ief_local;
	__u32			ief_inodes;
	__u32			ief_blocks;
	__u32			ief_extents;
	__u64			mtime;
	__u64			ctime;
	__u64			atime;
	unsigned		dir_cached;
	ext2_ino_t		dir_ino[GROUP_DIR_CACHE_SZ];
	unsigned char		dir_rawh[0];

};

enum ief_compact_algo {
	IEF_SORT_BLOCK = 0,
	IEF_SORT_INODE,
	IEF_SORT_FSTREE,
};

struct defrag_context
{
	/* general fs info */
	ext2_filsys		fs;
	const char		lost_found_dir[PATH_MAX + 1];
	int			root_fd;
	struct stat64		root_st;
	unsigned		blocksize_bits;
	struct file_handle	*root_fhp;
	int			root_mntid;
	unsigned int		ro_fs;

	/* fs-tree cache info */
	struct rb_root		sp_root; /* space extent root */
	struct group_info	**group;

	/* IAF defrag */
	__u32			iaf_cluster_size;
	/* IEF candidates info */
	struct rb_fhandle	*ief_head;
	struct rb_fhandle	*ief_tail;
	dgrp_t			ief_group;
	__u64			ief_blocks;
	int			ief_inodes;
	int			ief_i_groups;
	int			ief_b_groups;
	int			ief_compact_algo;
	unsigned		ief_force_local; /* Pack inodes according
						      to their metadata */
	unsigned		ief_reloc_grp_log; /* Relocate files only
						      inside that group, usually
						      inside flexbg */
	unsigned		cluster_size;
	unsigned		ief_reloc_cluster;
	unsigned		weight_scale;
	unsigned		tp_weight_scale;
	unsigned		extents_quality;
};

static inline __u64 dfx_sz2b(struct defrag_context *dfx, __u64 size)
{
	return (size + (1 << dfx->blocksize_bits) -1) >> dfx->blocksize_bits;
}

void df_show_stats()
{
	printf("Inodes  scanned:\t\t%llu \n", dfstat_scanned_inodes);
	printf("Directories  scanned:\t\t%llu \n", dfstat_scanned_directories);
	printf("Files  scanned:\t\t\t%llu \n", dfstat_scanned_files);
	printf("Extents scanned:\t\t%llu \n", dfstat_scanned_blocks);
	printf("Blocks scanned:\t\t\t%llu \n", dfstat_scanned_extents);

	printf("IAF inoded defragmented:\t\t%llu \n", dfstat_iaf_defragmented);
	printf("IAF blocks defragmented:\t\t%llu \n", dfstat_iaf_defragmented_sz);
	printf("IEF inoded defragmented:\t\t%llu \n", dfstat_ief_defragmented);
	printf("IEF blocks defragmented:\t\t%llu \n", dfstat_ief_defragmented_sz);
	if (mem_usage) {
		printf("Total extents:\t\t\t%llu \n", dfstat_spextents);
		printf("Total extents mem consumption:\t%llu Kb\n",
		       (dfstat_spextents * sizeof(struct spextent)) / 1024);
	}
}
static void print_spex(char *msg, struct spextent *ex)
{
	printf("%s ex:%p ->[%llu, %u] ex:%d d:%d ro:%d "
	       "old:%d fnd:%d iaf:%d flags:%x\n", msg,
	       ex, ex->start, ex->count,
	       (int)ex->extents, (int)ex->dir_extents,
	       (int)ex->ro_extents, (int)ex->old_extents,
	       (int)ex->found, (int)ex->iaf_extents,
	       (int)ex->flags);
}

static void print_tree_range(struct rb_node *first, struct rb_node *last)
{
	struct rb_node *node = NULL;
	struct spextent *ex;

	printf("\t\t\t=================================\n");
	for (node = first; node != NULL; node = ext2fs_rb_next(node)) {
		ex = node_to_spextent(node);
		print_spex("\t\t\t", ex);
		if (node == last)
			break;
	}
	printf("\t\t\t=================================\n");
}

static void print_tree(struct rb_root *root)
{
	print_tree_range(ext2fs_rb_first(root), NULL);
}

static unsigned ul_log2(unsigned long arg)
{
	unsigned l = 0;

	arg >>= 1;
	while (arg) {
		l++;
		arg >>= 1;
	}
	return l;
}

#ifdef DEBUG_RB
static void check_tree_range(struct rb_node *first, struct rb_node *last, const char *msg)
{
	struct rb_node *node;
	struct spextent *ext, *old = NULL;

	for (node = first; node && node; node = ext2fs_rb_next(node)) {
		ext = node_to_spextent(node);
		if (ext->count == 0) {
			printf("Tree Error: count is zero\n");
			printf("extent: %llu -> %llu (%u)\n", ext->start,
				ext->start + ext->count, ext->count);
			goto err_out;
		}

		if (ext->start + ext->count < ext->start) {
			printf("Tree Error: start or count is crazy\n");
			printf("extent: %llu -> %llu (%u)\n", ext->start,
				ext->start + ext->count, ext->count);
			goto err_out;
		}

		if (old) {
			if (old->start > ext->start) {
				printf("Tree Error: start is crazy\n");
				printf("extent: %llu -> %llu (%u)\n",
					old->start, old->start + old->count,
					old->count);
				printf("extent next: %llu -> %llu (%u)\n",
					ext->start, ext->start + ext->count,
					ext->count);
				goto err_out;
			}

			if ((old->start + old->count) > ext->start) {
				printf("Tree Error: extent overlap\n");
				printf("extent: %llu -> %llu (%u)\n",
					old->start, old->start + old->count,
					old->count);
				printf("extent next: %llu -> %llu (%u)\n",
					ext->start, ext->start + ext->count,
					ext->count);
				goto err_out;
			}
		}
		old = ext;
		if (node == last)
			break;
	}
	return;

err_out:
	printf("%s\n", msg);
	print_tree_range(first, last);
	exit(1);
}

static void check_tree(struct rb_root *root, const char *msg)
{
	check_tree_range(ext2fs_rb_first(root), NULL, msg);
}

static void check_node(struct rb_node *node, const char* msg)
{
	check_tree_range(ext2fs_rb_prev(node) ? ext2fs_rb_prev(node) : node,
			 ext2fs_rb_next(node), msg);
}
#else
#define check_tree(root, msg) do {} while (0)
#define check_tree_range(first, last, msg) do {} while (0)
#define check_node(node, msg) do {} while (0)
#endif

static inline struct spextent* search_spextent(struct rb_node *n, __u64 pblock)
{
	struct spextent * ex;

	while (n) {
		ex = node_to_spextent(n);
		if (pblock < ex->start)
			n = n->rb_left;
		else if (pblock >= ex->start + ex->count)
			n = n->rb_right;
		else {
			check_node(n, __FUNCTION__);
			return ex;
		}
	}

	return NULL;
}

static inline struct rb_fhandle* insert_fhandle(struct rb_root *root, struct rb_node *node)
{
	struct rb_node ** p = &root->rb_node;
	struct rb_node * parent = NULL;
	struct rb_fhandle *fh, *new = node_to_fhandle(node);

	while (*p) {
		parent = *p;
		fh = node_to_fhandle(parent);

		if (new->start < fh->start)
			p = &(*p)->rb_left;
		else if (new->start > fh->start)
			p = &(*p)->rb_right;
		else
			return fh;
	}
	ext2fs_rb_link_node(node, parent, p);
	ext2fs_rb_insert_color(node, root);

	return NULL;
}

static errcode_t alloc_fhandle(__u64 start, struct file_handle *fh,
			       struct rb_fhandle **sptr)
{
	errcode_t retval;

	retval = ext2fs_get_memzero(sizeof (struct rb_fhandle) + fh->handle_bytes, sptr);
	if (retval)
		return retval;

	dfstat_fhandles++;
	(*sptr)->start = start;
	memcpy(((*sptr)->f_handle), fh->f_handle, fh->handle_bytes);

	return 0;
}

static void free_fhandle(struct rb_fhandle* rbfh)
{
	if (!rbfh)
		return;
	if (rbfh->flags & SP_FL_FMAP)
		free(rbfh->fec);
	ext2fs_free_mem(&rbfh);
}

static int __do_open_f_handle(struct defrag_context *dfx, unsigned char *f_handle, int flags)
{
	int ret;
	struct file_handle *fh;

	fh = malloc(sizeof (struct file_handle) + dfx->root_fhp->handle_bytes);
	CHKMEM(fh, return -1);

	fh->handle_bytes = dfx->root_fhp->handle_bytes;
	fh->handle_type = dfx->root_fhp->handle_type;
	memcpy(fh->f_handle, f_handle, dfx->root_fhp->handle_bytes);
	ret = open_by_handle_at(dfx->root_fd, fh, flags);
	if (ret < 0 && verbose)
		printf("%s:%d open_by_handle_at failed :errno:%m\n", __func__, __LINE__);
	free(fh);

	return ret;
}

static int do_open_fhandle(struct defrag_context *dfx, struct rb_fhandle *rfh, int flags)
{
	return __do_open_f_handle(dfx, rfh->f_handle, flags);
}

static inline struct spextent* insert_spextent(struct rb_root *root, struct rb_node *node)
{
	struct rb_node ** p = &root->rb_node;
	struct rb_node * parent = NULL;
	struct spextent *ex, *new = node_to_spextent(node);;

	while (*p) {
		parent = *p;
		ex = node_to_spextent(parent);

		if (new->start < ex->start)
			p = &(*p)->rb_left;
		else if (new->start >= ex->start + ex->count)
			p = &(*p)->rb_right;
		else
			return ex;
	}
	ext2fs_rb_link_node(node, parent, p);
	ext2fs_rb_insert_color(node, root);
	check_node(node, __FUNCTION__);
	return NULL;
}

static errcode_t alloc_spextent(__u64 start, __u32 count, struct spextent **sptr)
{
	errcode_t retval;

	retval = ext2fs_get_memzero(sizeof (struct spextent), sptr);
	if (retval)
		return retval;

	dfstat_spextents++;
	(*sptr)->start = start;
	(*sptr)->count = count;
	return 0;
}

static void fmap_csum_init(ext2_filsys fs, struct stat64 *st,  __u32 *crc)
{
	*crc = ext2fs_crc32c_le(0xdeadbeef, (unsigned char *)&st->st_ino, sizeof(st->st_ino));
	*crc = ext2fs_crc32c_le(*crc, (unsigned char *)&st->st_size, sizeof(st->st_size));
	*crc = ext2fs_crc32c_le(*crc, (unsigned char *)&st->st_blocks, sizeof(st->st_blocks));
}

static void fmap_csum_ext(struct fmap_extent *fmap,  __u32 *crc)
{
	*crc = ext2fs_crc32c_le(*crc, (unsigned char *)&fmap->lblk, sizeof(__u64));
	*crc = ext2fs_crc32c_le(*crc, (unsigned char *)&fmap->pblk, sizeof(__u64));
	*crc = ext2fs_crc32c_le(*crc, (unsigned char *)&fmap->len, sizeof(__u32));
}

static inline dgrp_t e4d_group_of_ino(struct defrag_context *dfx, ext2_ino_t ino)
{
	return ext2fs_group_of_ino(dfx->fs, ino) >> dfx->ief_reloc_grp_log;
}

static inline dgrp_t e4d_group_of_blk(struct defrag_context *dfx, blk64_t blk)
{
	return ext2fs_group_of_blk2(dfx->fs, blk) >> dfx->ief_reloc_grp_log;
}


static errcode_t init_one_group(struct defrag_context *dfx, unsigned long group,
				char * bitmap,  unsigned long num, unsigned long offset,
				int ratio)
{
	struct spextent *new;
	struct spextent *se;
	unsigned long i;
	unsigned long j;
	errcode_t retval;

	offset /= ratio;
	offset += group * num;

	if (debug_flag & DBG_BITMAP)
		printf(" %s grp:%lu, num:%lu, off:%lu, ratio:%d \n",
				__func__, group, num, offset, ratio);

	for (i = 0; i < num; i++) {
		if (in_use(bitmap, i)) {
			for (j = i; j < num && in_use(bitmap, j); j++);

			retval = alloc_spextent((i + offset)*ratio, (j-i)*ratio,
						&new);
			if (retval)
				return retval;

			se = insert_spextent(&dfx->sp_root, &new->node);
			if (se) { /* Collision, this should not happen during initialization */
				fprintf(stderr, "Collision while insert: [%llu,%d], exist [%llu, %d]\n",
				       se->start, se->count, new->start, new->count);
				print_tree(&dfx->sp_root);
				exit(1);
			}
			i = j;
		}
	}

	return 0;
}

static int __get_inode_fiemap(struct defrag_context *dfx, int fd,
			      struct stat64 *st, unsigned blksz_log,
			      __u32 flags, struct fmap_extent_cache **fec,
			      struct fmap_extent_stat *fest)
{
	__u32 i;
	__u64 pos = 0;
	__u64 pblk_last = 0, lblk_last = 0;
	int ext_buf_size, fie_buf_size, ret = 0;
	struct fiemap	*fiemap_buf = NULL;
	struct fiemap_extent	*ext_buf = NULL;
	dgrp_t prev_blk_grp, ino_grp = e4d_group_of_ino(dfx, st->st_ino);

	ext_buf_size = EXTENT_MAX_COUNT * sizeof(struct fiemap_extent);
	fie_buf_size = sizeof(struct fiemap) + ext_buf_size;

	fiemap_buf = malloc(fie_buf_size);
	CHKMEM(fiemap_buf, return -1);

	if (!(*fec)) {
		*fec = malloc(sizeof(struct fmap_extent_cache) +
			      sizeof(struct fmap_extent) * DEFAULT_FMAP_CACHE_SZ);
		CHKMEM(*fec, ret = -1; goto out);
		(*fec)->fec_size = DEFAULT_FMAP_CACHE_SZ;
		(*fec)->fec_extents = 0;
	}
	if (fest)
		memset(fest, 0 , sizeof(*fest));

	ext_buf = fiemap_buf->fm_extents;
	memset(fiemap_buf, 0, fie_buf_size);
	fiemap_buf->fm_length = FIEMAP_MAX_OFFSET;
	fiemap_buf->fm_flags |= FIEMAP_FLAG_SYNC;
	fiemap_buf->fm_extent_count = EXTENT_MAX_COUNT;

	do {
		fiemap_buf->fm_start = pos;
		memset(ext_buf, 0, ext_buf_size);
		ret = ioctl(fd, FS_IOC_FIEMAP, fiemap_buf);
		if (ret < 0 || fiemap_buf->fm_mapped_extents == 0) {
			if (debug_flag & DBG_FIEMAP) {
				fprintf(stderr, "%s: Can't get extent info for inode:%ld ret:%d mapped:%d\n",
				       __func__, st->st_ino, ret, fiemap_buf->fm_mapped_extents);
			}
			goto out;
		}

		for (i = 0; i < fiemap_buf->fm_mapped_extents; i++) {
			__u64 lblk = ext_buf[i].fe_logical >> blksz_log;
			__u64 pblk = ext_buf[i].fe_physical >> blksz_log;
			__u32 len = ext_buf[i].fe_length >> blksz_log;
			if (fest) {
				dgrp_t blk_grp = e4d_group_of_blk(dfx, pblk);

				if (lblk != lblk_last)
					fest->hole++;
				if (pblk != pblk_last)
					fest->frag++;
				if (ino_grp == blk_grp) {
					fest->local_ex++;
					fest->local_sz += len;
				}
				if (prev_blk_grp != blk_grp) {
					/* Speculative accounting */
					fest->group++;
					prev_blk_grp = blk_grp;
				}
			}

			if ((*fec)->fec_extents && lblk == lblk_last && pblk == pblk_last) {
				/* Merge */
				(*fec)->fec_map[(*fec)->fec_extents -1].len += len;
			} else {
				/* Add new extent */
				(*fec)->fec_map[(*fec)->fec_extents].lblk = lblk;
				(*fec)->fec_map[(*fec)->fec_extents].pblk = pblk;
				(*fec)->fec_map[(*fec)->fec_extents].len = len;
				(*fec)->fec_extents++;
				if ((*fec)->fec_extents == (*fec)->fec_size) {
					/* More space required */
					int new_sz = (*fec)->fec_size + ((*fec)->fec_size >> 1);

					*fec = realloc(*fec, sizeof(struct fmap_extent_cache) +
						       sizeof(struct fmap_extent) * new_sz);
					CHKMEM(*fec, ret = -1; goto out);
					(*fec)->fec_size = new_sz;
				}
			}
			if (ext_buf[i].fe_flags & ~(flags | FIEMAP_EXTENT_LAST)) {
				if (debug_flag & DBG_FIEMAP)
					fprintf(stderr, "Bad extent found ino:%ld"
						" lblk:%lld, len:%u, pblk:%lld flags:%x\n",
						st->st_ino, lblk, len, pblk,
						ext_buf[i].fe_flags);
				ret = -1;
			}
			lblk_last = lblk + len;
			pblk_last = pblk + len;
			dfstat_scanned_extents++;
			dfstat_scanned_blocks += len;
		}
		/* Record file's logical offset this time */
		pos = ext_buf[EXTENT_MAX_COUNT-1].fe_logical +
			ext_buf[EXTENT_MAX_COUNT-1].fe_length;
		/*
		 * If fm_extents array has been filled and
		 * there are extents left, continue to cycle.
		 */
	} while (fiemap_buf->fm_mapped_extents == EXTENT_MAX_COUNT &&
		 !(ext_buf[EXTENT_MAX_COUNT-1].fe_flags & FIEMAP_EXTENT_LAST));
out:
	/////////////FIXME:DEBUG
	if (debug_flag & DBG_FIEMAP && fest)
		printf("%s fmap stat ino:%ld hole:%d frag:%d local_ex:%d local_sz:%d group:%d\n",
		       __func__, st->st_ino, fest->hole, fest->frag,
		       fest->local_ex, fest->local_sz, fest->group);

	free(fiemap_buf);

	return ret;

}
static inline int get_inode_fiemap(struct defrag_context *dfx, int fd,
				   struct stat64 *st, unsigned blksz_log,
				   struct fmap_extent_cache **fec,
				    struct fmap_extent_stat *fest)
{
	return __get_inode_fiemap(dfx, fd, st, blksz_log, FIEMAP_EXTENT_UNWRITTEN,
				  fec, fest);
}
static int check_iaf(struct defrag_context *dfx, struct stat64 *stat,
		     struct fmap_extent_cache *fec, struct fmap_extent_stat *fest);

static int do_iaf_defrag_one(struct defrag_context *dfx, int dirfd, const char *name,
			     struct stat64 *stat, struct fmap_extent_cache *fec,
			     struct fmap_extent_stat *fest);

typedef int proc_inode_t (struct defrag_context *dfx, int fd, struct stat64 *st,
			  int dirfd, const char *name);


static int group_info_init(struct defrag_context *dfx, dgrp_t group)
{
	dfx->group[group] = malloc(sizeof(struct group_info) +
				   dfx->root_fhp->handle_bytes * GROUP_DIR_CACHE_SZ);
	CHKMEM(dfx->group[group], return 0);
	memset(dfx->group[group], 0, sizeof(struct group_info));
	dfx->group[group]->fh_root = RB_ROOT;

	return 1;
}
/* Add ief candidate for later processing */
static int group_add_ief_candidate(struct defrag_context *dfx, int dirfd, const char* name,
				   dgrp_t group, __u64 pblock,
				   __u32 blocks, __u32 extents, int flags,
				   struct rb_fhandle **out_fh)
{
	int mnt;
	int ret = 0;
	struct rb_fhandle *rbfh = NULL;
	struct file_handle *fhp;
	errcode_t retval;

	assert(flags & SP_FL_IEF_RELOC);

	if (!dfx->group[group] && !group_info_init(dfx, group))
		return -1;

	if (!dfx->group[group]) {
		dfx->group[group] = malloc(sizeof(struct group_info) +
					   dfx->root_fhp->handle_bytes * GROUP_DIR_CACHE_SZ);
		CHKMEM(dfx->group[group], return -1);
		memset(dfx->group[group], 0, sizeof(struct group_info));
		dfx->group[group]->fh_root = RB_ROOT;
	}

	fhp = malloc(sizeof(struct file_handle) + dfx->root_fhp->handle_bytes);
	CHKMEM(fhp);
	memset(fhp, 0, sizeof(*fhp));

	fhp->handle_bytes = dfx->root_fhp->handle_bytes;
	ret = name_to_handle_at(dirfd, name, fhp, &mnt, 0);
	if (ret) {
		if (debug_flag & (DBG_SCAN|DBG_IEF))
			fprintf(stderr, "Unexpected result from name_to_handle_at()\n");
		goto free_fh;
	}
	retval = alloc_fhandle(pblock, fhp, &rbfh);
	if (retval) {
		fprintf(stderr, "Can not alloc mem err:%d\n", errno);
		ret = -1;
		goto free_fh;
	}

	if (insert_fhandle(&dfx->group[group]->fh_root, &rbfh->node)) {
		/* Inode is already in the list, likely nlink > 1 */
		if (debug_flag & (DBG_SCAN|DBG_IEF))
			fprintf(stderr, "File is already in the list, nlink > 1,"
				" Not an error\n");
		ext2fs_free_mem(&rbfh);
		goto free_fh;
	}
	rbfh->flags = flags;
	if (!dfx->group[group]->fs_head) {
		dfx->group[group]->fs_head = rbfh;
		dfx->group[group]->fs_tail = rbfh;
	} else{
		dfx->group[group]->fs_tail->fs_next = rbfh;
		dfx->group[group]->fs_tail = rbfh;
	}
	dfx->group[group]->ief_inodes++;
	dfx->group[group]->ief_blocks += blocks;
	dfx->group[group]->ief_extents += extents;
	if (flags & SP_FL_LOCAL)
		dfx->group[group]->ief_local++;
	if (!(dfx->group[group]->flags & SP_FL_IEF_RELOC)) {
		dfx->group[group]->flags |= SP_FL_IEF_RELOC;
		dfstat_ief_groups++;
	}
	dfstat_ief_candidates_sz += blocks;
	dfstat_ief_candidates++;

	*out_fh = rbfh;

free_fh:
	free(fhp);

	return ret;
}

static int scan_inode_pass1(struct defrag_context *dfx, int fd,
			    struct stat64 *stat, int dirfd, const char *name)
{
	int i, ret = 0;
	unsigned is_old = 0;
	unsigned is_rdonly = 0;
	struct fmap_extent_stat fest;
	struct fmap_extent_cache *fec = NULL;
	struct spextent *se;
	dgrp_t ino_grp = e4d_group_of_ino(dfx, stat->st_ino);

	/*
	 * From defragmentation point of view, both a readonly inode and
	 * an inode with old_mtime are good candidates for IEF defragmentation.
	 * The inode is likely belongs to both sets, but it is reasonable
	 * to make two sets mutually exclusive in order to make SUM operation
	 * correct for these two sets.
	 * Readonly-ness is more fundamental feature because it is always
	 * meaningful, while mtime is less important as it may be screwed
	 * via utime(2).
	 */
	if (stat->st_mtime  < older_than)
		is_old = 1;

	if ((stat->st_mode & (S_IXUSR|S_IXGRP|S_IXOTH)) ||
	    !(stat->st_mode & (S_IWUSR|S_IWGRP|S_IWOTH))) {
		is_rdonly = 1;
		is_old = 0;
	}

	if (debug_flag & DBG_SCAN) {
		printf("%s:%ld inode:%ld mode:%lx mtime: %lu old:%d ro:%d\n",
			__func__, (long)time_start.tv_sec - (long)older_than,
			stat->st_ino, (unsigned long)stat->st_mode,
			(unsigned long)stat->st_mtime, is_old,
			(stat->st_mode & (S_IXUSR|S_IXGRP|S_IXOTH)) ||
			!(stat->st_mode & (S_IWUSR|S_IWGRP|S_IWOTH)));
	}
	ret = get_inode_fiemap(dfx, fd, stat, dfx->blocksize_bits, &fec, &fest);
	if (ret || !fec->fec_extents)
		goto out;

	for (i = 0; i < fec->fec_extents; i++) {
		dgrp_t blk_grp = e4d_group_of_blk(dfx, fec->fec_map[i].pblk);

		se = search_spextent(dfx->sp_root.rb_node, fec->fec_map[i].pblk);
		if (!se || se->start + se->count < fec->fec_map[i].pblk + fec->fec_map[i].len) {
			if (debug_flag & DBG_SCAN) {
				fprintf(stderr, "%s: Cache collision for inode:%ld"
					", lblock:%u pblock:%llu len:%u. "
					"Skip it...\n", __func__,
					stat->st_ino, fec->fec_map[i].lblk,
					fec->fec_map[i].pblk, fec->fec_map[i].len);
			}
			break;
		}

		se->extents++;
		se->found += fec->fec_map[i].len;
		if (S_ISDIR(stat->st_mode))
			se->dir_extents++;
		if (is_rdonly)
			se->ro_extents++;
		if (is_old)
			se->old_extents++;
		if (blk_grp == ino_grp)
			se->local++;
		if (fest.frag > 1 )
			se->iaf_extents++;
		if (debug_flag & DBG_SCAN) {
			printf("ino:%ld %u ->[%llu, %u] EX:%p [%llu, %u]\n",
				stat->st_ino, fec->fec_map[i].lblk,
				fec->fec_map[i].pblk, fec->fec_map[i].len,
				se, se->start, se->count);
		}
	}

	if (debug_flag & DBG_SCAN) {
		printf("ino:%ld type:%lx sz:%lu blk:%lu extents:%u holes:%u frag:%u\n",
			stat->st_ino, (unsigned long)stat->st_mode,
			stat->st_size / dfx->root_st.st_blksize,
			stat->st_blocks / (dfx->root_st.st_blksize >> 9),
			fec->fec_extents, fest.hole, fest.frag);
	}
out:
	free(fec);

	return 0;
}

/*
 * Cache directories from known groups. These directories will be used later
 * for allocating donor files.
 *
 * TODO: The only reason we do all this caching crap is to force block allocator
 *       to allocate donor's blocks from some specific group. IMHO it is
 *       reasonable to implement an IOCTL to advise the goal block.
 */
static void group_add_dircache(struct defrag_context *dfx, int dirfd, struct stat64 *stat, const char *name)
{
	int mnt;
	int i,ret;
	struct file_handle *fhp = NULL;
	dgrp_t grp = e4d_group_of_ino(dfx, stat->st_ino);

	if (!dfx->group[grp] && !group_info_init(dfx, grp))
		return;

	if (dfx->group[grp]->dir_cached == GROUP_DIR_CACHE_SZ)
		return;

	for (i = 0; i < dfx->group[grp]->dir_cached; i++) {
		if (dfx->group[grp]->dir_ino[i] == stat->st_ino)
			return;
	}

	fhp = malloc(sizeof(struct file_handle) + dfx->root_fhp->handle_bytes);
	CHKMEM(fhp, exit(1));
	memset(fhp, 0, sizeof(*fhp));

	fhp->handle_bytes = dfx->root_fhp->handle_bytes;
	ret = name_to_handle_at(dirfd, name, fhp, &mnt, 0);
	if (ret) {
		if (debug_flag & DBG_SCAN)
			fprintf(stderr, "Unexpected result from name_to_handle_at()\n");
		free(fhp);
		return;
	}
	if (debug_flag & DBG_FS)
		printf("%s group:%d cache_idx:%d inode:%d\n", __func__, grp, stat->st_ino,
		       dfx->group[grp]->dir_cached);

	memcpy(dfx->group[grp]->dir_rawh +
	       dfx->root_fhp->handle_bytes * dfx->group[grp]->dir_cached,
	       fhp->f_handle, dfx->root_fhp->handle_bytes);
	dfx->group[grp]->dir_ino[dfx->group[grp]->dir_cached] = stat->st_ino;
	dfx->group[grp]->dir_cached++;

}

static int scan_inode_pass3(struct defrag_context *dfx, int fd,
			    struct stat64 *stat, int dirfd, const char *name)
{
	__u32 i, csum;
	struct fmap_extent_stat fest;
	struct fmap_extent_cache *fec = NULL;
	int ret = 0;
	int is_old = 0;
	int is_rdonly = 0;
	__u64 ief_blocks = 0;
	__u64 tp_blocks = 0;
	__u32 ino_flags = 0;
	__u64 size_blk = dfx_sz2b(dfx, stat->st_size);
	__u64 used_blk = dfx_sz2b(dfx, stat->st_blocks << 9);
	dgrp_t ino_grp = e4d_group_of_ino(dfx, stat->st_ino);

	if (S_ISDIR(stat->st_mode)) {
		group_add_dircache(dfx, dirfd, stat, name);
		return 0;
	}

	if (!S_ISREG(stat->st_mode))
		return 0;

	ret = get_inode_fiemap(dfx, fd, stat, dfx->blocksize_bits, &fec, &fest);
	if (ret || !fec->fec_extents)
		goto out;

	/* IAF inodes can be fixed independently */
	if (check_iaf(dfx, stat, fec, &fest)) {
		struct stat64 dst;

		ret = fstat64(dirfd, &dst);
		if (ret)
			goto out;

		group_add_dircache(dfx, dirfd, &dst, ".");
		ret = do_iaf_defrag_one(dfx, dirfd, name, stat, fec, &fest);
		if (!ret)
			goto out;
		
	}

	if (stat->st_mtime  < older_than)
		is_old = 1;
	if ((stat->st_mode & (S_IXUSR|S_IXGRP|S_IXOTH)) ||
	    !(stat->st_mode & (S_IWUSR|S_IWGRP|S_IWOTH))) {
		is_rdonly = 1;
		is_old = 0;
	}

	/*
	 * At this point it is too expensive to store fiemap cache for each
	 * IEF candidate, store it's fiemap csum
	 */
	fmap_csum_init(dfx->fs, stat, &csum);

	for (i = 0; i < fec->fec_extents; i++) {
		struct spextent *se;

		se = search_spextent(dfx->sp_root.rb_node, fec->fec_map[i].pblk);
		if (!se || se->start + se->count < fec->fec_map[i].pblk + fec->fec_map[i].len) {
			if (debug_flag & DBG_SCAN) {
				fprintf(stderr, "%s: Cache collision for inode:%ld"
					", lblock:%u pblock:%llu len:%u. "
					"Skip it...\n", __func__,
					stat->st_ino, fec->fec_map[i].lblk,
					fec->fec_map[i].pblk, fec->fec_map[i].len);
			}
			goto out;
		}
		if (se->flags & SP_FL_IEF_RELOC)
			ief_blocks += fec->fec_map[i].len;
		if (se->flags & SP_FL_TP_RELOC)
			tp_blocks += fec->fec_map[i].len;

		fmap_csum_ext(fec->fec_map + i, &csum);
	}

	if (fest.local_ex == fec->fec_extents)
		ino_flags |= SP_FL_LOCAL;

	if (ief_blocks || tp_blocks) {
		if (debug_flag & DBG_SCAN && ief_blocks != size_blk)
			printf("%s ENTER %lu to IEF set ief:%lld "
			       "size_blk:%lld used_blk:%lld\n",
			       __func__, stat->st_ino, ief_blocks,
			       size_blk, used_blk);

		/*
		 * Even if some extents belong to IEF cluster, it is not a good
		 * idea to relocate the whole file. From other point of view,
		 * if more than half of extents belong to IEF set, then
		 * the whole inode is suitable for IEdefragmentation.
		 *
		 * TODO: Think a bit more about maximum file size suitable
		 *       for relocation
		 */
		if (ief_blocks *2 > size_blk && ( is_rdonly || is_old ||
						  size_blk < dfx->cluster_size)) {
			ino_flags |= SP_FL_IEF_RELOC;
			if (debug_flag & DBG_SCAN && ief_blocks != size_blk)
				printf("%s Force add %lu to IEF set ief:%lld "
				       "size_blk:%lld used_blk:%lld\n",
				       __func__, stat->st_ino, ief_blocks,
				       size_blk, used_blk);
		} else if (tp_blocks * 4 > size_blk) {
			ino_flags |= SP_FL_IEF_RELOC | SP_FL_TP_RELOC;
			if (debug_flag & DBG_SCAN && ief_blocks != size_blk)
				printf("%s Force add %lu to IEF/TP set ief:%lld "
				       "size_blk:%lld used_blk:%lld\n",
				       __func__, stat->st_ino, ief_blocks,
				       size_blk, used_blk);
		} else if (debug_flag & DBG_SCAN) {
			printf("%s Reject %lu from IEF set ief:%lld "
			       "size_blk:%lld used_blk:%lld\n",
			       __func__, stat->st_ino, ief_blocks,
			       size_blk, used_blk);
		}
		if (debug_flag & DBG_SCAN && ief_blocks != size_blk)
			printf("%s ENTER %lu to IEF set ief:%lld "
			       "size_blk:%lld used_blk:%lld fl:%lx\n",
			       __func__, stat->st_ino, ief_blocks,
			       size_blk, used_blk, ino_flags);

	}

	if (ino_flags & SP_FL_IEF_RELOC) {
		struct stat dst;
		struct rb_fhandle *rbfh = NULL;
		/* FIXME: Is it any better way to find directory inode num? */
		ret = fstat(dirfd, &dst);
		if (!ret && ino_grp ==  e4d_group_of_ino(dfx, dst.st_ino))
			ino_flags |= SP_FL_DIRLOCAL;

		ret = group_add_ief_candidate(dfx, dirfd, name, ino_grp,
					      fec->fec_map[0].pblk, size_blk,
					      fec->fec_extents, ino_flags, &rbfh);
		if (ret || rbfh == NULL)
			goto out;
		rbfh->flags |= SP_FL_CSUM;
		rbfh->csum = csum;
	}
out:
	free(fec);

	return ret;
}

static int scan_one_dentry(struct defrag_context *dfx, int dirfd,
			   const char *name, proc_inode_t scan_ino_fn)
{
	int	fd;
	int	ret;
	struct stat64	stat;

	dfstat_scanned_inodes++;

	if (dfx->lost_found_dir[0] != '\0' &&
	    !memcmp(name, dfx->lost_found_dir, strnlen(dfx->lost_found_dir, PATH_MAX))) {
		if (debug_flag & DBG_SCAN)
			printf("%s skip %s", __func__, name);
		return 0;
	}

	fd = openat(dirfd, name, O_RDONLY);
	if (fd < 0) {
		if (debug_flag & DBG_SCAN)
			fprintf(stderr, "Can not open file %s, errno:%d\n", name, errno);
		return 1;
	}

	ret = fstat64(fd, &stat);
	if (ret) {
		if (debug_flag & DBG_SCAN)
			fprintf(stderr,	"Error while fstat for %s, errno:%d\n", name, errno);
		goto out;
	}

	if (S_ISDIR(stat.st_mode))
		dfstat_scanned_directories++;
	if (!S_ISREG(stat.st_mode) && !S_ISDIR(stat.st_mode))
		goto out;
	if (stat.st_size == 0)
		goto out;
	if (stat.st_blocks == 0)
		goto out;
	if (stat.st_dev != dfx->root_st.st_dev)
		goto out;

	/* Access authority */
	if (current_uid != ROOT_UID &&
		stat.st_uid != current_uid) {
		ret = 1;
		if (debug_flag & DBG_SCAN) {
			fprintf(stderr,	"File %s is not current user's file,"
				" or current user is not root\n", name);
		}
		goto out;
	}

	ret = scan_ino_fn(dfx, fd, &stat, dirfd, name);
	if (ret < 0) {
		if (debug_flag & DBG_SCAN)
			fprintf(stderr, "Failed to get extents info for %s\n", name);
		goto out;
	}
	dfstat_scanned_files++;
out:
	close(fd);

	return 0;
}

static int scan_one_directory(struct defrag_context * dfx, char *buf, int count, int dirfd,
		proc_inode_t scan_fn);

static int walk_subtree(struct defrag_context * dfx, int fd, proc_inode_t scan_fn)
{
	struct stat64 stb;
	char *buf;
	int err = 0;
	int bufsz;
	int offset;
	int space;

	if (fstat64(fd, &stb)) {
		fprintf(stderr, "%s fstat64: %m\n", __func__);
		return -1;
	}

	if (stb.st_dev != dfx->root_st.st_dev)
		return 0;

	bufsz = stb.st_size + getpagesize();
	buf = malloc(bufsz);
	CHKMEM(buf, return -1);

	offset = 0;
	space = bufsz;
	lseek(fd, 0 , SEEK_SET);
	for (;;) {
		err = __sys_getdents64(fd, (struct linux_dirent64*)(buf + offset),
				       space);
		if (err < 0)
			goto scan_error;

		if (err == 0)
			break;

		offset += err;
		space  -= err;

		if (256 + sizeof(struct linux_dirent64) < space)
			continue;

		buf = realloc(buf, bufsz*2);
		CHKMEM(buf, err = -1; break);

		space += bufsz;
		bufsz *= 2;
	}
	err = scan_one_directory(dfx, buf, offset, fd, scan_fn);

scan_error:
	free(buf);

	return err;
}

/* Directory scanner, dents are stored in the buffer. */
static int scan_one_directory(struct defrag_context * dfx, char *buf, int count, int dirfd,
		proc_inode_t scan_fn)
{
	int err = 0;
	struct linux_dirent64 * de;

	for (de = (struct linux_dirent64*)buf;
			(char*)de < buf + count;
			de = (struct linux_dirent64 *)((char*)de + de->d_reclen)) {

		if (strcmp(de->d_name, ".") == 0 ||
		    strcmp(de->d_name, "..") == 0)
			continue;

		if (de->d_type == DT_DIR || de->d_type == DT_REG) {
			if (de->d_type == DT_DIR) {
				int fd;

				fd = openat(dirfd, de->d_name, O_RDONLY|O_DIRECTORY);
				if (fd < 0)
					return -1;

				err = walk_subtree(dfx, fd, scan_fn);
				close(fd);
				if (err)
					break;
			}

			err = scan_one_dentry(dfx, dirfd, de->d_name, scan_fn);
			if (err)
				break;
		}
	}

	return err;
}

/*
 * EXT4_IOC_MOVE_EXT ioctl will want page cache for given inode
 * This is very important for small files, poke readahead.
 */
static void defrag_fadvise(int fd, loff_t off, loff_t len, int is_sync)
{
	int	sync_flag = SYNC_FILE_RANGE_WRITE;

	if (is_sync)
		sync_flag |= SYNC_FILE_RANGE_WAIT_BEFORE |
			    SYNC_FILE_RANGE_WAIT_AFTER;

	sync_file_range(fd, off, len, sync_flag);
	if (posix_fadvise(fd, off, len, POSIX_FADV_SEQUENTIAL) < 0) {
		if (debug_flag & DBG_SCAN)
			fprintf(stderr, "\tFailed to fadvise");
	}

	if (posix_fadvise(fd, off, len, POSIX_FADV_WILLNEED) < 0) {
		if (debug_flag & DBG_SCAN)
			fprintf(stderr, "\tFailed to fadvise");
	}
}

static int ief_defrag_prep_one(struct defrag_context *dfx, dgrp_t group,
			       int fd, struct rb_fhandle *fhandle,
			       struct stat64 *stat)
{
	int i, ret;
	__u32 csum;
	struct fmap_extent_cache *fec = NULL;
	struct fmap_extent_stat fest;
	__u64  size_blk = dfx_sz2b(dfx, stat->st_size);

	assert((fhandle->flags & (SP_FL_CSUM|SP_FL_FMAP)) != (SP_FL_CSUM|SP_FL_FMAP));

	ret = get_inode_fiemap(dfx, fd, stat, dfx->blocksize_bits, &fec, &fest);
	if (ret || !fec->fec_extents)
		goto changed;

	if (fhandle->flags & SP_FL_CSUM) {
		fmap_csum_init(dfx->fs, stat, &csum);
		for (i = 0; i < fec->fec_extents; i++)
			fmap_csum_ext(fec->fec_map + i, &csum);
		if (fhandle->csum != csum)
			goto changed;
	}

	if (fhandle->flags & SP_FL_FMAP) {
		if (fec->fec_extents  != fhandle->fec->fec_extents)
			goto changed;
		if (memcmp((char*)fec->fec_map, (char*)fhandle->fec->fec_map,
			   sizeof(struct fmap_extent) * fhandle->fec->fec_extents))
			goto changed;
	}

	fhandle->flags &= ~SP_FL_CSUM;
	fhandle->flags |= SP_FL_FMAP;
	fhandle->fec = fec;

	/* This for averaging */
	dfx->group[group]->mtime += stat->st_mtime;
	dfx->group[group]->ctime += stat->st_ctime;
	dfx->group[group]->atime += stat->st_ctime;

	/* Update statistics */
	dfx->group[group]->ief_blocks += size_blk;
	dfx->group[group]->ief_inodes++;
	dfx->group[group]->ief_extents += fec->fec_extents;
	if (fhandle->flags & SP_FL_LOCAL)
		dfx->group[group]->ief_local++;

	if (debug_flag & (DBG_SCAN | DBG_IEF))
		printf("%s Check inode %lu flags:%x, OK...\n",
		       __func__, stat->st_ino, fhandle->flags);

	defrag_fadvise(fd, 0, stat->st_size, 0);

	return 0;

changed:

	if (verbose)
		printf("%s inode %u flags:%lx changed under us, skip\n",
		       __func__, fhandle->flags, (unsigned long) stat->st_ino);
	fhandle->flags |= SP_FL_IGNORE;
	free(fec);

	return 0;
}

/*
 * Sequential scan of the block bitmap tables. Built spextent tree
 */
static void pass1(struct defrag_context *dfx)
{
	unsigned long i;
	blk64_t	first_block, last_block;
	char *block_bitmap = NULL;
	int		block_nbytes;
	blk64_t		blk_itr = EXT2FS_B2C(dfx->fs, dfx->fs->super->s_first_data_block);
	errcode_t	retval;

	block_nbytes = EXT2_CLUSTERS_PER_GROUP(dfx->fs->super) / 8;

	retval = ext2fs_read_bitmaps (dfx->fs);
	if (retval || !dfx->fs->block_map) {
		fprintf(stderr, "Error while allocating block bitmap");
		exit(1);
	}

	block_bitmap = malloc(block_nbytes);
	CHKMEM(block_bitmap, exit(1));

	first_block = dfx->fs->super->s_first_data_block;

	if (verbose)
		printf("Pass1:  Scanning bitmaps\n");

	for (i = 0; i < dfx->fs->group_desc_count; i++) {
		if (ext2fs_bg_flags_test(dfx->fs, i, EXT2_BG_BLOCK_UNINIT) ||
		    !ext2fs_block_bitmap_loc(dfx->fs, i)) {
			if (debug_flag & DBG_SCAN)
				printf("Pass1: group %lu uninitialized,"
				       " skip it...\n", i);
			blk_itr += dfx->fs->super->s_clusters_per_group;
			continue;
		}
		first_block = ext2fs_group_first_block2(dfx->fs, i);
		last_block = ext2fs_group_last_block2(dfx->fs, i);

		if (block_bitmap) {
			retval = ext2fs_get_block_bitmap_range2(dfx->fs->block_map,
				 blk_itr, block_nbytes << 3, block_bitmap);
			if (retval) {
				com_err("pass1", retval,
					"while reading block bitmap");
				exit(1);
			}
			retval = init_one_group(dfx, i, block_bitmap,
						dfx->fs->super->s_clusters_per_group,
						dfx->fs->super->s_first_data_block,
						EXT2FS_CLUSTER_RATIO(dfx->fs));
			if (retval) {
				fprintf(stderr, "Error while init_one_group");
				exit(1);
			}

			blk_itr += dfx->fs->super->s_clusters_per_group;
		}
	}
	free(block_bitmap);
	check_tree(&dfx->sp_root, "pass1:");

	if (verbose)
		printf("Pass1:  Finished. Collected %llu space fragments\n",
		       dfstat_spextents);

	if (debug_flag & DBG_TREE)
		print_tree(&dfx->sp_root);
}

/*
 * Scan filesystem tree and collect extents ownership statistics.
 * Number of separated file extents covered by this file
 *
 */
static void pass2(struct defrag_context *dfx)
{
	int err;

	if (verbose)
		printf("Pass2:  Scanning directory hierarchy\n");

	err = scan_inode_pass1(dfx, dfx->root_fd, &dfx->root_st, dfx->root_fd, ".");
	if (err) {
		fprintf(stderr, "Pass2: Can not scan root dentry\n");
		exit(1);
	}

	err = walk_subtree(dfx, dfx->root_fd, scan_inode_pass1);
	if (err) {
		fprintf(stderr, "Pass2: failed \n");
		exit(1);
	}

	if (verbose)
		printf("Pass2:  Finished. Scanned %llu extents of %llu inodes\n",
		       dfstat_scanned_extents, dfstat_scanned_inodes);

	if (debug_flag & DBG_TREE)
		print_tree(&dfx->sp_root);
}

/*
 * Pass3 prep (IEF preparation stage)
 * At this moment we know enough info about each space fragment.
 * It is time to make relocation decisions.
 * Literature is luck of good relocation/defratmentation algorithms.
 * ZFS is known to use lru list but there is no specific info about this.
 *
 *  pass3_prep simply scans cached extents and:
 *      a) Divides extents in to defragmentation clusters.
 *      b) Calculates weight and quality of each cluster
 *      d) If a defrag cluster is good enough, marks all extents
 *         in that cluster as candidates for IEF relocation
 *
 */
static void pass3_prep(struct defrag_context *dfx)
{
	struct rb_node *node, *cluster_node = NULL;
	struct spextent *ex;
	__u64 cluster_mask = ~((__u64)dfx->cluster_size - 1ULL);
	__u64 cluster, prev_cluster = 0;
	__u64 ext_to_move = 0;
	__u64 blocks_to_move = 0;
	__u64 clusters_to_move = 0;
	unsigned used = 0;
	unsigned good = 0;
	unsigned mdata = 0;
	unsigned count = 0;
	unsigned found = 0;
	unsigned ief_ok = 0;
	unsigned force_reloc = 0;

	if (verbose)
		printf("Pass3_prep:  Scan and rate cached extents\n");

	cluster_mask = ~((__u64)dfx->cluster_size - 1ULL);
	for (node = ext2fs_rb_first(&dfx->sp_root); node != NULL;
	     node = ext2fs_rb_next(node)) {
		ex = node_to_spextent(node);
		if (ex->ro_extents + ex->old_extents == ex->extents)
			ex->flags |= SP_FL_GOOD;
		if (ex->found == ex->count)
			ex->flags |= SP_FL_FULL;
		cluster = (ex->start + ex->count) & cluster_mask;

		if (debug_flag & DBG_CLUSTER)
			print_spex("\t\t\t", ex);

		if (prev_cluster != cluster) {
			force_reloc = ief_ok = 0;
			/* Is cluster has enough RO(good) data blocks ?*/
			if (dfx->cluster_size  >= used * dfx->weight_scale &&
			    good * 1000 >= count * dfx->extents_quality)
				ief_ok = 1;

			/* Thin provision corner case: If cluster has low number
			 * of data blocks it should be relocated regardless to
			 * block's quality in order to improve space efficency */
			if (dfx->cluster_size  >= used * dfx->tp_weight_scale) {
				ief_ok = 1;
				force_reloc = 1;
			}

			if (ief_ok && cluster_node) {
				while (cluster_node != node) {
					struct spextent *se =
						node_to_spextent(cluster_node);
					se->flags |= SP_FL_IEF_RELOC;
					if (force_reloc)
						se->flags |= SP_FL_TP_RELOC;
					if (debug_flag & DBG_CLUSTER)
						print_spex("\t\t\t->IEF", se);
					ext_to_move++;
					blocks_to_move += se->count;
					cluster_node =
						ext2fs_rb_next(cluster_node);
				}
				clusters_to_move++;
			}
			if (debug_flag & DBG_CLUSTER)
				printf("Cluster %lld %lld] group:%ld stats "
				       "{count:%d used:%d good:%d found:%d "
				       "mdata:%u ief:%d force_reloc:%d}\n",
				       prev_cluster,
				       prev_cluster + dfx->cluster_size,
				       e4d_group_of_blk(dfx, prev_cluster),
				       count, used, good, found, mdata,
				       ief_ok, force_reloc);
			good = 0;
			count = 0;
			used  = 0;
			found = 0;
			mdata = 0;
			cluster_node = node;
			prev_cluster = cluster;
		}
		count++;
		used += ex->count;
		found += ex->found;
		mdata += ex->dir_extents;

		if (ex->flags & SP_FL_GOOD && !(ex->flags & SP_FL_IGNORE))
			good++;
	}
	if (verbose) {
		printf("Pass3_prep %llu blocks of %llu extents from %llu clusters may be relocated\n",
		       blocks_to_move, ext_to_move, clusters_to_move);
	}
	check_tree(&dfx->sp_root, __func__);
	if (debug_flag & DBG_TREE)
		print_tree(&dfx->sp_root);
}

static void pass3(struct defrag_context *dfx)
{
	int err;

	if (verbose)
		printf("Pass3:  Rescan directory hierarchy, scan inodes for "
		       "relocation\n");

	err = walk_subtree(dfx, dfx->root_fd, scan_inode_pass3);
	if (err) {
		fprintf(stderr, "Pass3: Rescan subtree failed\n");
		exit(1);
	}
	err = scan_inode_pass3(dfx, dfx->root_fd, &dfx->root_st, dfx->root_fd, ".");
	if (err) {
		fprintf(stderr, "Pass3: Can not rescan root dentry\n");
		exit(1);
	}

	if (verbose)
		printf("Pass3:  Finished. Found %llu inodes and %llu blocks suitable"
		       "  for IEF relocation\n",
		       dfstat_ief_candidates, dfstat_ief_candidates_sz);

	if (debug_flag & DBG_TREE)
		print_tree(&dfx->sp_root);

}

static void release_donor_space(struct donor_info *donor)
{
	int rc;

	if (donor->fd != -1) {
		rc = ftruncate(donor->fd, 0);
		if (rc)
			fprintf(stderr, "%s: Failed to ftruncate(0): %m\n", __func__);
	}
	free(donor->fec);
	donor->fec = NULL;
	donor->length = 0;
	donor->offset = 0;
}

static void close_donor(struct donor_info *donor)
{
	release_donor_space(donor);
	if (donor->fd != -1) {
		close(donor->fd);
		donor->fd = -1;
	}
}

static int do_alloc_donor_space(struct defrag_context *dfx, dgrp_t group,
			  struct donor_info *donor, __u64 blocks,
			  unsigned force_local, unsigned max_frag)
{
	int ret;
	struct stat64 stat;
	struct fmap_extent_cache *fec = NULL;

	assert(donor->fd >= 0);
	assert(donor->length == 0);
	assert(donor->fec == 0);

	if (debug_flag & DBG_RT)
		printf("%s grp:%u donor_fd:%d blocks:%llu local:%d frag:%u\n",
		       __func__, group, donor->fd, blocks, force_local, max_frag);

	ret = fallocate(donor->fd, 0, 0, blocks << dfx->blocksize_bits);
	if (ret < 0) {
		if (debug_flag & DBG_FS)
			fprintf(stderr, "Can't fallocate space for donor file err:%d\n", errno);
		return ret;
	}

	ret = fstat64(donor->fd, &stat);
	if (ret) {
		fprintf(stderr,	"Error while fstat errno:%d\n", errno);
		return ret;
	}

	ret = get_inode_fiemap(dfx, donor->fd, &stat, dfx->blocksize_bits, &fec, &donor->fest);
	if (ret || !fec->fec_extents) {
		goto err;
	}
	////TODO:  Checks are sufficient for good donor?
	if (force_local && donor->fest.local_ex != fec->fec_extents) {
		ret = -2;
		goto err;
	}
	if (donor->fest.frag > max_frag) {
		ret = -3;
		goto err;
	}

	if (debug_flag & DBG_FS)
		printf("%s: Create donor file is_local:%d blocks:%lld\n", __func__,
		       donor->fest.local_ex == fec->fec_extents, blocks);

	donor->offset = 0;
	donor->length = blocks;
	donor->fec = fec;
	return 0;
err:
	if (debug_flag & DBG_ERR)
		printf("%s:%d REJECT donor grp:%u donor_fd:%d blocks:%llu local:%d frag:%u ret:%d\n",
		       __func__, __LINE__,  group, donor->fd, blocks, force_local, max_frag, ret);

	free(fec);

	return -1;
}

/* Walk directory cache for a group and try to allocate local donor file */
static int do_find_donor(struct defrag_context *dfx, dgrp_t group,
			  struct donor_info *donor, __u64 blocks,
			  unsigned force_local, unsigned max_frag)
{
	int dir, i, ret = 0;
	struct stat64 st;
	dgrp_t donor_grp;
	int dir_retries = 3;
	unsigned char *raw_fh = dfx->group[group]->dir_rawh;
	const char *dfname = ".e4defrag2_donor.tmp";

	if (!dfx->group[group])
		return -1;

	if (debug_flag & DBG_RT) {
		printf("%s ENTER grp:%u donor_fd:%d blocks:%llu local:%d frag:%u\n",
		       __func__, group, donor->fd, blocks, force_local, max_frag);
	}

	for (i = 0; i < dfx->group[group]->dir_cached;
	     i++, raw_fh += dfx->root_fhp->handle_bytes) {
		dir = __do_open_f_handle(dfx, raw_fh, O_RDONLY);
		if(dir < 0) {
			if (debug_flag & DBG_SCAN)
				fprintf(stderr, "%s: Can not open parent handle for "
					"grp:%d cache_id:%d inode:%d fid[0]\n"
					", %m\n", __func__, group, i,
					dfx->group[group]->dir_ino,
					((int*)raw_fh)[0]);
			continue;
		}

		donor->fd = openat(dir, dfname, O_RDWR|O_CREAT|O_EXCL, 0700);
		if (donor->fd < 0) {
			if (debug_flag & DBG_RT)
				fprintf(stderr,"Can not create donor file %m\n");
			close(dir);
			continue;
		}
		if (debug_flag & DBG_RT) {
			printf("%s TRY grp:%u donor_fd:%d blocks:%llu local:%d frag:%u\n",
			       __func__, group, donor->fd, blocks, force_local, max_frag);
		}

		if (unlinkat(dir, dfname, 0)) {
			ret = -1;
			fprintf(stderr, "Can not unlink donor file, %m\n");
			goto try_next;

		}
		close(dir);
		dir = -1;
		ret = fstat64(donor->fd, &st);
		if (ret) {
			ret = 0;
			if (debug_flag & DBG_SCAN)
				fprintf(stderr,"fstat failed %m\n");
			goto try_next;
		}
		donor_grp = e4d_group_of_ino(dfx, st.st_ino);
		if (force_local && donor_grp != group) {
			if (debug_flag & DBG_SCAN)
				printf("%s donor is from %d, want:%d. skip\n",
				       __func__, donor_grp, group);
			goto try_next;
		}
		ret = do_alloc_donor_space(dfx, group, donor, blocks, force_local, max_frag);
		if (!ret)
			return 0;
		else
			ret = 0;
	try_next:
		close(dir);
		close_donor(donor);
		if (ret || !dir_retries--)
			return -1;
	}

	return -1;
}

static int prepare_donor(struct defrag_context *dfx, dgrp_t group,
			 struct donor_info *donor, __u64 blocks,
			 unsigned force_local, unsigned max_frag)
{
	int i, ret;
	dgrp_t nr_groups = dfx->fs->group_desc_count >> dfx->ief_reloc_grp_log;


	if (debug_flag & DBG_RT) {
		printf("%s grp:%u donor_fd:%d blocks:%llu frag:%u\n",
		       __func__, group, donor->fd, blocks, max_frag);
	}
	assert(blocks);

	/* First try to reuse existing donor if available */
	if (donor->fd != -1) {
		release_donor_space(donor);
		ret = do_alloc_donor_space(dfx, group, donor, blocks, 1, max_frag);
		if (!ret)
			return 0;
		close_donor(donor);
	}
	ret = do_find_donor(dfx, group, donor, blocks, 1, max_frag);
	if (!ret)
		return 0;

	/* Can not find local donor, nothing to try. */
	if (force_local)
		return -1;

	/* Sequentially search groups and create first available */
	for (i = 1; i < 16; i++) {
		if (dfx->group[(group + i) % nr_groups]) {
			ret = do_find_donor(dfx, (group + i) % nr_groups,
					    donor, blocks, 0, max_frag);
			if (!ret)
				return ret;
		}
	}

	return -1;
}

/* FIXME: This check definitely should be smarter */
static int check_iaf(struct defrag_context *dfx, struct stat64 *stat,
		  struct fmap_extent_cache *fec, struct fmap_extent_stat *fest)
{
	__u64 eof_lblk;
	//// FIXME free_space_average should be tunable
	__u64 free_space_average = 64;
	int ret  = 1;

	if (!S_ISREG(stat->st_mode))
		ret = 0;
	if (fec->fec_extents < 2)
		ret = 0;
	if (fest->hole)
		ret = 0;


	eof_lblk = fec->fec_map[fec->fec_extents -1].lblk +
		fec->fec_map[fec->fec_extents -1].len;

	if (eof_lblk / fest->frag > free_space_average)
		ret = 0;


	if (debug_flag & DBG_RT)
		printf("%s ino:%ld frag:%d eof_blk:%lld free_space_aver:%d ret:%d\n",
		       __FUNCTION__, stat->st_ino, eof_lblk, fest->frag,
		       free_space_average, ret);

	return ret;

}

static int do_defrag_one(struct defrag_context *dfx, int fd,  struct stat64 *stat,
			 struct fmap_extent_cache *fec, struct fmap_extent_stat *fest,
			 __u64 eof_lblk, struct donor_info *donor)
{
	int ret, retry;
	struct move_extent mv_ioc;
	__u64 moved = 0;

	assert(donor->length >= eof_lblk);

	if (debug_flag & (DBG_RT | DBG_IEF| DBG_IAF))
		printf("%s perform inode:%ld\n", __func__, stat->st_ino);

	if (dfx->ro_fs) {
		if (debug_flag & DBG_RT)
			printf("Fileystem is readonly, skip actual defrag");
		return 0;
	}
	mv_ioc.donor_fd = donor->fd;
	mv_ioc.orig_start = 0;
	mv_ioc.donor_start = donor->offset;
	mv_ioc.moved_len = 0;
	mv_ioc.len = eof_lblk;

	if (debug_flag & DBG_RT)
		printf("%s inode:%lld start:%lld eof:%lld donor [%lld, %lld]\n",
		       __func__, (unsigned long long)stat->st_ino,
		       (unsigned long long)fec->fec_map[0].pblk,
		       (unsigned long long)eof_lblk,
		       (unsigned long long)donor->offset,
		       (unsigned long long)donor->length);
	retry= 3;
	do {
		ret = ioctl(fd, EXT4_IOC_MOVE_EXT, &mv_ioc);
		if (ret < 0) {
			if (verbose)
				fprintf(stderr, "%s: EXT4_IOC_MOVE_EXT failed %m\n",
					__func__);
			if (errno != EBUSY || !retry--)
				break;
		} else
			retry = 3;

		assert(mv_ioc.len >= mv_ioc.moved_len);
		mv_ioc.len -= mv_ioc.moved_len;
		mv_ioc.orig_start += mv_ioc.moved_len;
		mv_ioc.donor_start = mv_ioc.orig_start;
		moved += mv_ioc.moved_len;;
	} while (mv_ioc.len);

	donor->length -= moved;
	donor->offset += moved;
	if (debug_flag & (DBG_RT | DBG_IAF | DBG_IEF))
		printf("%s inode:%lld start:%lld eof:%lld donor [%lld, %lld] ret:%d\n",
		       __func__, (unsigned long long)stat->st_ino,
		       (unsigned long long)fec->fec_map[0].pblk,
		       (unsigned long long)eof_lblk,
		       (unsigned long long)donor->offset,
		       (unsigned long long)donor->length, ret);
	return ret;
}
/*
 * Single file defragmentation
 */
static int do_iaf_defrag_one(struct defrag_context *dfx, int dirfd, const char *name,
			     struct stat64 *stat, struct fmap_extent_cache *fec,
			     struct fmap_extent_stat *fest)
{
	int force_local, ret, fd;
	struct donor_info donor;
	struct stat64 st2;
	dgrp_t ino_grp =  e4d_group_of_ino(dfx, stat->st_ino);
	__u64 eof_lblk = fec->fec_map[fec->fec_extents -1].lblk +
		fec->fec_map[fec->fec_extents -1].len;

	assert(fest->frag >= 2);
	ret  = 0;

	/* Need to reopen file for RW */
	fd = openat(dirfd, name, O_RDWR);
	if (fd < 0) {
		if (debug_flag & (DBG_RT|DBG_IAF))
			fprintf(stderr, "%s: can not open candidate\n", __func__);
		return 0;
	}

	if (fstat64(fd, &st2)) {
		if (debug_flag & (DBG_RT|DBG_IAF))
			fprintf(stderr, "%s: stat failed err:%d\n", __func__,
				errno);
		goto out_fd;
	}

	if (st2.st_ino != stat->st_ino) {
 		if (debug_flag & (DBG_RT|DBG_IAF))
			fprintf(stderr, "%s: Race while reopen\n", __func__);
		goto out_fd;
	}

	donor.fd = -1;
	donor.fec = NULL;
	donor.length = 0;
	donor.offset = 0;

	/* Force local group for small files
	 * FIXME: This should be tunable
	 */
	force_local = eof_lblk < 4;

	if (debug_flag & (DBG_SCAN|DBG_IAF)) {
		int i;
		printf("%s ENTER inode:%ld eof:%llu force_local:%d frag:%u local_ex:%u\n",
		       __func__, stat->st_ino, (unsigned long long) eof_lblk,
		       force_local, fest->frag, fest->local_ex);
		for (i = 0; i < fec->fec_extents; i++)
			printf("%u [%u, %u] -> %llu\n",
			       i, fec->fec_map[i].lblk, fec->fec_map[i].len,
			       fec->fec_map[i].pblk);
	}

	ret = prepare_donor(dfx, ino_grp, &donor, eof_lblk, force_local, fest->frag / 2);
	if (ret) {
		if (debug_flag & (DBG_SCAN|DBG_IAF))
			fprintf(stderr, "%s: group:%u Can not allocate donor"
				" file\n", __func__,  ino_grp);
		goto out_fd;
	}

	if (debug_flag & (DBG_SCAN|DBG_IAF)) {
		int i;
		printf("%s FOUND DONOR inode:%ld eof:%llu force_local:%d frag:%u local_ex:%u\n",
		       __func__, stat->st_ino, (unsigned long long) eof_lblk,
		       force_local, fest->frag, fest->local_ex);

		for (i = 0; i < donor.fec->fec_extents; i++)
			printf("%u [%u, %u] -> %llu [%u, %u]\n",
			       i, donor.fec->fec_map[i].lblk,
			       donor.fec->fec_map[i].len,
			       donor.fec->fec_map[i].pblk);

	}

	defrag_fadvise(fd, 0 , eof_lblk << dfx->blocksize_bits, 1);

	ret = do_defrag_one(dfx, fd, stat, fec, fest, eof_lblk, &donor);
	if (!ret) {
		dfstat_iaf_defragmented++;
		dfstat_iaf_defragmented_sz += eof_lblk;
		if (verbose)
			printf("%s SUCCESS ino:%lu blocs:%lld old_frag:%u new_frag:%u\n",
			       __func__,  (unsigned long )stat->st_ino,
			       (unsigned long long)eof_lblk, fest->frag,
			       donor.fest.frag);

	} else {
		if (verbose)
			printf("%s FAIL ino:%lu blocs:%lld frag:%u donor_frag:%u\n",
			       __func__, (unsigned long )stat->st_ino,
			       (unsigned long long)eof_lblk,
			       fest->frag, donor.fest.frag);
	}
	close_donor(&donor);
out_fd:
	close(fd);

	return ret;
}

/* Pack one file
 */
static int do_ief_defrag_one(struct defrag_context *dfx, dgrp_t group,
			     struct rb_fhandle *rfh, struct donor_info *donor)
{
	struct stat64 st;
	int ret, fd;
	__u64 eof_lblk;
	struct fmap_extent_stat fest;
	struct fmap_extent_cache *fec = NULL;

	assert(rfh->flags & SP_FL_FMAP);
	
	fd = do_open_fhandle(dfx, rfh, O_RDWR);
	if (fd < 0) {
		/* Propably file was unlinked, renamed "
		   " or modified, simply ignore it */
		if (debug_flag & DBG_RT)
			fprintf(stderr, "%s Can not open file %d, ignore\n",
				__func__, errno);
		rfh->flags |= SP_FL_IGNORE;
		return 0;
	}

	ret = fstat64(fd, &st);
	if (ret) {
		fprintf(stderr, "Fstat failed %m\n");
		rfh->flags |= SP_FL_IGNORE;
		close(fd);
		return 0;
	}

	/* Recheck extent tree */
	ret = get_inode_fiemap(dfx, fd, &st, dfx->blocksize_bits, &fec, &fest);
	if (ret || !fec->fec_extents)
		goto out;
	if (fec->fec_extents  != rfh->fec->fec_extents)
		goto out;
	if (memcmp((char*)fec->fec_map, (char*)rfh->fec->fec_map,
		   sizeof(struct fmap_extent) * rfh->fec->fec_extents))
		goto out;

	eof_lblk = fec->fec_map[fec->fec_extents -1].lblk +
		fec->fec_map[fec->fec_extents -1].len;

	defrag_fadvise(fd, 0 , eof_lblk << dfx->blocksize_bits, 1);

	ret = do_defrag_one(dfx, fd, &st, fec, &fest, eof_lblk, donor);

	if (debug_flag & (DBG_RT| DBG_IEF))
		printf("%s process inode %lu flags:%x, ret:%d\n",
		       __func__, st.st_ino, rfh->flags, ret);


	if (!ret) {
		dfx->group[group]->ief_inodes++;
		dfx->group[group]->ief_blocks += st.st_size >> dfx->blocksize_bits;
		if (donor->fest.local_ex)
			dfx->group[group]->ief_local++;
	} else
		ret = 0;
out:
	free(fec);
	close(fd);

	return ret;
}

/*
 * Perform fragmentation for IEF inodes
 *
 * We already collected number of candidates, and we want to pack it effectively
 * EXAMPLE:
 * [ino1].....[ino2]...[ino3]..[ino4]    ==> [ino1][ino2][ino3][ino4]
 *                                      OR=> [ino2][ino4][ino3][ino1]
 *
 * Rational assuptions for effective packing:
 * 1) Inodes should be from same group in order to maintain mdata/data locality
 * 2) Inodes should be packed in optimal order in order to reduce number of seeks
 * There are several ordering stategies:
 *  2a) Pack according to first block :
 *      Stability: Very good, original order is preserved. So number of seeks
 *             will be less or equal.
 *      Optimization potential: low because will fix only create/unlink scenario
 *	for two types of files, stable and temporary
 *  2b) Pack according to inode number: Almost the same advantages as (2a) but
 *      tend to fix some deviations which were introduced by the block allocator.
 *  2c) Pack according to directory order.
 *      Stability: may result in IO performance degradation for some workloads.
 *      Optimization potential: Will likely fix the rename scenario.
 * 3) Donor file should be from the same group.
 *
 * It is reasonable to separate IEF defragmentation in two phases
 *  1) Sort inodes according to some strategy:  ief_defgar_prep()
 *  2) Finally compact inodes in spacified order: ief_defrag_finish() FIXME!!! fix comment
 */

static int ief_defrag_group(struct defrag_context *dfx, dgrp_t idx)
{
	int fd, ret = 0;
	struct rb_node *node;
	struct rb_fhandle *rfh, *prev;
	struct stat64 st;
	struct donor_info donor;
	struct group_info * group = dfx->group[idx];
	__u64 blocks;
	int force_local = 0;
	/*
	 * Prepare stage
	 * Walk inodes in block order in order to warm up the page cache
	 */
	group->ief_inodes = 0;
	group->ief_local = 0;
	group->ief_extents = 0;
	group->ief_blocks = 0;
	dfstat_directories += dfx->group[idx]->dir_cached;

	for (node = ext2fs_rb_first(&group->fh_root); node != NULL;
	     node = ext2fs_rb_next(node)) {
		rfh = node_to_fhandle(node);
		if (rfh->flags & SP_FL_IGNORE || !(rfh->flags & SP_FL_IEF_RELOC))
			continue;

		fd = do_open_fhandle(dfx, rfh, O_RDONLY);
		if (fd < 0) {
			/* Probably file was unlinked, renamed "
			   " or modified, simply ignore it */
			if (debug_flag & DBG_RT)
				fprintf(stderr, "%s Can not open file %d, ignore\n",
					__func__, errno);
			rfh->flags |= SP_FL_IGNORE;
			continue;
		}

		ret = fstat64(fd, &st);
		if (ret) {
			fprintf(stderr, "Fstat failed %m\n");
			rfh->flags |= SP_FL_IGNORE;
			close(fd);
			continue;
			return 0;
		}
		ret = ief_defrag_prep_one(dfx, idx, fd, rfh, &st);
		close(fd);
	}
	if (debug_flag & DBG_RT) {
		printf("%s free_b:%u free_i:%u  dir:%u \n", __func__,
		       ext2fs_bg_free_blocks_count(dfx->fs, idx),
		       ext2fs_bg_free_inodes_count(dfx->fs, idx),
		       ext2fs_bg_used_dirs_count(dfx->fs, idx));
		printf("%s group:%u prepare relocation dir:%u inodes:%u, local:%u"
		       " extents: %u, blocks %u\n", __func__, idx,
		       group->dir_cached, group->ief_inodes,
		       group->ief_local, group->ief_extents,
		       group->ief_blocks);
	}

	/* Ordering stage */
	switch (dfx->ief_compact_algo) {
	case IEF_SORT_BLOCK:
		prev = NULL;
		for (node = ext2fs_rb_first(&group->fh_root); node != NULL;
		     node = ext2fs_rb_next(node)) {
			rfh = node_to_fhandle(node);
			if (!(rfh->flags & SP_FL_IEF_RELOC) || rfh->flags & SP_FL_IGNORE)
				continue;
			if (!prev)
				group->next = rfh;
			else
				prev->next = rfh;
			prev = rfh;
		}
		break;
	case IEF_SORT_INODE:
		//// FIXME:TODO reorder tree
		break;
	case IEF_SORT_FSTREE:
		prev = NULL;
		for (rfh = group->fs_head; rfh != NULL; rfh = rfh->fs_next) {
			if (!(rfh->flags & SP_FL_IEF_RELOC) || rfh->flags & SP_FL_IGNORE)
				continue;
			if (!prev)
				group->next = rfh;
			else
				prev->next = rfh;
			prev = rfh;
		}
	}

	/* Relocation stage */
	////////////FIXME: Cleanup statistics
	group->ief_inodes = 0;
	group->ief_local = 0;
	group->ief_extents = 0;
	group->ief_blocks = 0;

	donor.fd = -1;
	donor.fec = NULL;
	donor.length = 0;
	donor.offset = 0;
next_cluster:
	blocks = 0;
	force_local = dfx->ief_force_local;
	/* Divide inodes in to reallocation clusters */
	for (rfh = group->next; rfh != NULL; rfh = rfh->next) {
		assert(!(rfh->flags & SP_FL_IGNORE));
		assert(rfh->flags & (SP_FL_FMAP | SP_FL_IEF_RELOC));
		if (blocks >= dfx->ief_reloc_cluster)
			break;
		blocks += rfh->fec->fec_map[rfh->fec->fec_extents -1].lblk +
			rfh->fec->fec_map[rfh->fec->fec_extents -1].len;
		if (rfh->flags &  SP_FL_TP_RELOC)
			force_local = 0;
	}
	prev = rfh;

	/* Do we have somthing to relocate */
	if (!blocks)
		return 0;

	ret = prepare_donor(dfx, idx, &donor, blocks, force_local, 2);
	if (ret) {
		if (debug_flag & DBG_SCAN)
			fprintf(stderr, "%s group:%u Can not allocate donor"
				" file\n", __func__,  idx);
		return 0;
	}

	assert (group->next);
	while (group->next && group->next != prev) {
		do_ief_defrag_one(dfx, idx, group->next , &donor);
		group->next = group->next->next;
	}

	if (group->next)
		goto next_cluster;

	dfstat_ief_defragmented += group->ief_inodes;
	dfstat_ief_defragmented_sz += group->ief_blocks;
	close_donor(&donor);
	return 0;
}

/*
 * Pass4:  Perform fragmentation for IEF inodes
 *
 */
static void pass4(struct defrag_context *dfx)
{
	int ret = 0;
	struct rb_fhandle *rfh;
	dgrp_t i, groups;

	if (verbose)
		printf("Pass4: Process defrag list \n");

	groups = dfx->fs->group_desc_count >> dfx->ief_reloc_grp_log;
	for (i = 0; i < groups; i++) {
		if (!dfx->group[i]) {
			if (debug_flag & DBG_SCAN)
				printf("pass4 scan group:%d empty\n", i);
			continue;
		}
		if (debug_flag & DBG_SCAN)
			printf("pass4 scan group:%d fl:%lx ief_inodes:%u "
			       "ief_local:%u, ief_blocks:%u, ief_extents:%u\n",
			       i, dfx->group[i]->flags, dfx->group[i]->ief_inodes,
			       dfx->group[i]->ief_local, dfx->group[i]->ief_blocks,
			       dfx->group[i]->ief_extents);

		ret = ief_defrag_group(dfx, i);
		if (ret) {
			fprintf(stderr, "%s: Error defragmenting %u group\n",
				__func__, i);
			exit(1);
		}
		dfx->group[i]->fh_root = RB_ROOT;
		while (dfx->group[i]->fs_head) {
			rfh = dfx->group[i]->fs_head;
			dfx->group[i]->fs_head = rfh->fs_next;
			free_fhandle(rfh);
		}
		if (verbose) {
			printf("%s group:%u Relocated inodes:%u, local:%u"
			       " extents: %u, blocks %u\n", __func__, i,
			       dfx->group[i]->ief_inodes,
			       dfx->group[i]->ief_local,
			       dfx->group[i]->ief_extents,
			       dfx->group[i]->ief_blocks);
		}
	}
}

static void close_device(char *device_name, ext2_filsys fs)
{
	int retval = ext2fs_close(fs);

	if (retval)
		com_err(device_name, retval, "while closing the filesystem");
}

static void open_device(char *device_name, ext2_filsys *fs, dev_t devno)
{
	struct stat64 st;
	int retval;
	int flag = EXT2_FLAG_FORCE | EXT2_FLAG_64BITS;

	retval = stat64(device_name, &st);
	if (retval < 0) {
		com_err(device_name, retval, "while opening filesystem image");
		exit(1);
	}
	if (!S_ISBLK(st.st_mode)) {
		com_err(device_name, retval, "while opening filesystem, block device expected");
		exit(1);
	}
	if (st.st_rdev != devno) {
		com_err(device_name, retval, "while opening filesystem, wrong block device");
		exit(1);
	}

	retval = ext2fs_open(device_name, flag, 0, 0, unix_io_manager, fs);
	if (retval) {
		com_err(device_name, retval, "while opening filesystem");
		exit(1);
	}
      
	(*fs)->default_bitmap_type = EXT2FS_BMAP64_RBTREE;
}

static void usage(void)
{
	fprintf(stderr, "Usage: %s [-v] [-n] [-d flags] [-t seconds] -t [time]  device root_dir\n", program_name);
	fprintf(stderr, "\t-a: minimal fragment size\n");
	fprintf(stderr, "\t-c: cluster size\n");
	fprintf(stderr, "\t-d: debug flags\n");
	fprintf(stderr, "\t-f: Enable default flex group optimization\n");
	fprintf(stderr, "\t-F: same as '-f' but provides manual flex grp log size\n");
	fprintf(stderr, "\t-l: force new blocks only inside single meta group\n");
	fprintf(stderr, "\t-m: dump fs and memory statistics at the end\n");
	fprintf(stderr, "\t-n: dry run\n");
	fprintf(stderr, "\t-s: scale factor\n");
	fprintf(stderr, "\t-S: thin provision scale factor\n");
	fprintf(stderr, "\t-q: defragmentation quality factor\n");
	fprintf(stderr, "\t-t: interpret inodes modified earlier than N seconds ago as RO files\n");
	fprintf(stderr, "\t-T: same as '-t' but use an absolute value\n");
	exit (1);
}

int main(int argc, char *argv[])
{
	struct defrag_context dfx;
	int c;
	char *device_name;
	char *root_dir;
	char *end;
	struct file_handle *fhp = NULL;
	int cluster_size = 1 << 20;
	int reloc_cluster_size = 0;
	int scale = 2;
	int tp_scale = 32; /* 1/32 ==> 3% */
	int quality = 700;
	dgrp_t nr_grp;
	int flex_bg = 0;
	unsigned long long min_frag_size = 0;
	memset(&dfx, 0, sizeof(dfx));
	add_error_table(&et_ext2_error_table);
	gettimeofday(&time_start, 0);

	while ((c = getopt(argc, argv, "a:C:c:d:fF:hl:mnt:s:S:T:vq:")) != EOF) {
		switch (c) {
		case 'a':
			min_frag_size = strtoul(optarg, &end, 0);
			break;

		case 'c':
			cluster_size = strtoul(optarg, &end, 0);
			if (!cluster_size || (cluster_size & (cluster_size - 1))) {
				fprintf(stderr, "Defragmentation cluster size must be power of 2");
				usage();
			}
			break;
		case 'C':
			reloc_cluster_size = strtoul(optarg, &end, 0);
			if (!reloc_cluster_size || (reloc_cluster_size & (reloc_cluster_size - 1))) {
				fprintf(stderr, "Relocation cluster size must be power of 2");
				usage();
			}
			break;
		case 'd':
			debug_flag = strtoul(optarg, &end, 0);
			break;
		case 'f':
			flex_bg = -1;
			break;
		case 'F':
			flex_bg = strtoul(optarg, &end, 0);
			break;

		case 't':
			older_than = time_start.tv_sec - strtoul(optarg, &end, 0);
			break;
		case 'T':
			older_than = strtoul(optarg, &end, 0);
			if (older_than > time_start.tv_sec) {
				if (verbose)
					fprintf(stderr, "WARNING: timestamp is "
						"%lu seconds in the future\n",
						(unsigned long)older_than - time_start.tv_sec);
			}
			break;
		case 's':
			scale = strtoul(optarg, &end, 0);
			break;

		case 'S':
			tp_scale = strtoul(optarg, &end, 0);
			break;

		case 'q':
			quality = strtoul(optarg, &end, 0);
			if (quality > 1000)
				usage();
			break;
		case 'l':
			dfx.ief_force_local = !!strtoul(optarg, &end, 0);
			break;

		case 'n':
			dry_run = 1;
			break;
		case 'm':
			mem_usage = 1;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'h':
		default:
			usage();
			break;
		}
	}
	if (optind + 2 != argc) {
		fprintf(stderr, "%s: missing device name\n", program_name);
		usage();
	}

	current_uid = getuid();
	device_name = argv[optind];
	root_dir = argv[optind + 1];
	dfx.root_fd = open(root_dir, O_RDONLY|O_DIRECTORY);
	if (dfx.root_fd < 0) {
		fprintf(stderr, "%s: can not open directory at:%s, errno:%d\n",
			program_name, root_dir, errno);
		exit(1);
	}

	if (fstat64(dfx.root_fd, &dfx.root_st)) {
		fprintf(stderr, "%s: can't fstat root:%s, %m\n",
			program_name, root_dir);
		exit(1);
	}
	open_device(device_name, &dfx.fs, dfx.root_st.st_dev);
	dfx.blocksize_bits = ul_log2(dfx.root_st.st_blksize);
	if (cluster_size < dfx.root_st.st_blksize) {
		fprintf(stderr, "%s: Defragmentation cluster can not be less than fs"
			"block size\n", program_name);
		exit(1);
	}

	/* Allocate max possible handle size */
	fhp = malloc(sizeof(struct file_handle) + MAX_HANDLE_SZ);
	CHKMEM_PROG(fhp, exit(1));
	fhp->handle_bytes = MAX_HANDLE_SZ;
	if (name_to_handle_at(dfx.root_fd, ".", fhp,
			   &dfx.root_mntid, 0) < 0) {
		fprintf(stderr, "Unexpected result from name_to_handle_at()\n");
		exit(1);
	}

	/* Finaly init of defrag context */
	dfx.root_fhp = fhp;
	dfx.cluster_size = cluster_size >> dfx.blocksize_bits;
	dfx.ief_reloc_cluster = reloc_cluster_size >> dfx.blocksize_bits;
	dfx.iaf_cluster_size = 16;
	if (min_frag_size  >= dfx.root_st.st_blksize)
		dfx.iaf_cluster_size = min_frag_size >> dfx.blocksize_bits;

	dfx.weight_scale = scale;
	dfx.tp_weight_scale = tp_scale;
	dfx.extents_quality = quality;
	dfx.ro_fs = dry_run;
	dfx.sp_root = RB_ROOT;
	if (!dfx.ief_reloc_cluster)
		dfx.ief_reloc_cluster = dfx.cluster_size;
	if (flex_bg == -1)
		dfx.ief_reloc_grp_log = dfx.fs->super->s_log_groups_per_flex;
	else if(flex_bg)
		dfx.ief_reloc_grp_log = ul_log2(flex_bg);
	dfx.ief_compact_algo = IEF_SORT_FSTREE;
	nr_grp = (dfx.fs->group_desc_count + (1 << dfx.ief_reloc_grp_log) - 1)
		>> dfx.ief_reloc_grp_log;

	dfx.group = malloc (sizeof (struct group_info*) * nr_grp);
	CHKMEM_PROG(dfx.group, exit(1));
	memset(dfx.group, 0 , sizeof (struct group_info*) * nr_grp);

	pass1(&dfx);
	pass2(&dfx);
	pass3_prep(&dfx);
	pass3(&dfx);
	pass4(&dfx);

	gettimeofday(&time_end, 0);
	if (mem_usage)
		df_show_stats(&dfx);

	close(dfx.root_fd);
	close_device(device_name, dfx.fs);

	return 0;
}
