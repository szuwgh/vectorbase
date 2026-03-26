/**
 * lock.h — Three-tier lock system for VectorBase
 *
 * A faithful port of PostgreSQL's SpinLock / LWLock / RegularLock hierarchy.
 *
 * Tier 1: SpinLock    — hardware test-and-set with exponential backoff
 * Tier 2: LWLock      — lightweight read/write lock; single atomic word
 * Tier 3: RegularLock — full deadlock-detecting lock manager (LOCKTAG-based)
 *
 * This is a self-contained module: do NOT include vb_type.h here.
 * All thread-using code must call VbProcInit() once per thread.
 */

#ifndef LOCK_H
#define LOCK_H
#include "vb_type.h"
#include <semaphore.h>
/* =========================================================================
 * Tier 1: SpinLock
 *
 * Closely mirrors PG s_lock.h / s_lock.c.
 * =========================================================================
 */

typedef volatile unsigned char slock_t;

/* TAS — test-and-set.  Returns 0 if lock was free (now acquired), 1 if busy. */
static inline int TAS(slock_t* lock)
{
    unsigned char _res = 1;
    __asm__ __volatile__("  lock xchgb %0,%1\n" : "+q"(_res), "+m"(*lock) : : "memory", "cc");
    return (int)_res;
}

/* TAS_SPIN — relaxed read then TAS; reduces bus traffic on contended locks */
static inline int TAS_SPIN(slock_t* lock)
{
    if (*lock != 0) return 1;
    return TAS(lock);
}

/* SpinDelay tracking — adapts spins_per_delay based on observed contention */
typedef struct SpinDelayStatus
{
    int spins;
    int delays;
    int cur_delay;
    const char* file;
    int line;
    const char* func;
} SpinDelayStatus;

#define NUM_DELAYS              1000    /* abort after this many sleep rounds   */
#define MIN_SPINLOCK_DELAY      1000    /* 1 ms in microseconds                 */
#define MAX_SPINLOCK_DELAY      1000000 /* 1 s  in microseconds                 */
#define DEFAULT_SPINS_PER_DELAY 100

/* Public SpinLock API */
int s_lock(slock_t* lock, const char* file, int line, const char* func);
void perform_spin_delay(SpinDelayStatus* status);
void finish_spin_delay(SpinDelayStatus* status);

/*
 * S_LOCK — acquire spinlock; if TAS fails immediately, call s_lock() for
 * backoff.  Returns delay count (informational).
 */
#define S_LOCK(lock)   (TAS_SPIN(lock) ? s_lock((lock), __FILE__, __LINE__, __func__) : 0)

#define S_UNLOCK(lock) __atomic_clear((lock), __ATOMIC_RELEASE)

#define S_INIT_LOCK(lock) \
    do                    \
    {                     \
        *(lock) = 0;      \
    } while (0)
#define S_LOCK_FREE(lock)     (*(lock) == 0)

/* Convenience wrappers */
#define SpinLockInit(lock)    S_INIT_LOCK(lock)
#define SpinLockAcquire(lock) S_LOCK(lock)
#define SpinLockRelease(lock) S_UNLOCK(lock)
#define SpinLockFree(lock) \
    do                     \
    {                      \
    } while (0)   /* no-op: stack/static slock_t */

/* =========================================================================
 * Tier 2: LWLock
 *
 * Single _Atomic u32 state word encodes everything.  Closely mirrors
 * PG lwlock.h / lwlock.c.
 * =========================================================================
 */

/* State word bit layout (identical to PG) */
#define LW_FLAG_HAS_WAITERS ((u32)1 << 30)
#define LW_FLAG_RELEASE_OK  ((u32)1 << 29)
#define LW_FLAG_LOCKED      ((u32)1 << 28)
#define LW_VAL_EXCLUSIVE    ((u32)1 << 24)
#define LW_VAL_SHARED       ((u32)1)
#define LW_LOCK_MASK        ((u32)((1 << 25) - 1))
#define LW_SHARED_MASK      ((u32)((1 << 24) - 1))

typedef enum
{
    LW_EXCLUSIVE = 0,
    LW_SHARED = 1,
    LW_WAIT_UNTIL_FREE = 2    /* wait until not exclusively locked */
} LWLockMode;

typedef enum
{
    LW_WS_NOT_WAITING = 0,
    LW_WS_WAITING = 1,
    LW_WS_PENDING_WAKEUP = 2
} LWLockWaitState;

/*
 * LWWaiter is embedded inside VbProc; we forward-declare VbProc here
 * so LWLock can hold a VbProc* wait list.
 */
struct VbProc;  /* forward declaration */

typedef struct LWLock
{
    const char* name;
    _Atomic u32 state;        /* encode all lock state              */
    struct VbProc* head;         /* wait list head (LW_FLAG_LOCKED)    */
    struct VbProc* tail;         /* wait list tail                     */
} LWLock;

/* Maximum LWLocks a single thread may hold simultaneously */
#define MAX_SIMUL_LWLOCKS 200

typedef struct
{
    LWLock* lock;
    LWLockMode mode;
} LWLockHandle;

/* Public LWLock API */
void LWLockInit(LWLock* lock, const char* name);
void LWLockAcquire(LWLock* lock, LWLockMode mode);
bool LWLockConditionalAcquire(LWLock* lock, LWLockMode mode);
bool LWLockAcquireOrWait(LWLock* lock, LWLockMode mode);
void LWLockRelease(LWLock* lock);
void LWLockReleaseClearVar(LWLock* lock, uint64_t* var, uint64_t val);
void LWLockReleaseAll(void);
bool LWLockHeldByMe(LWLock* lock);

/* =========================================================================
 * VbProc — per-thread process descriptor (replacement for PG's PGPROC)
 * =========================================================================
 */

#define MAX_VBPROCS 256

typedef struct VbProc
{
    int procno;
    bool sem_initialized;   /* true once sem_init() has been called for this slot */
    sem_t sem;

    /* LWLock waiter linkage (doubly linked list) */
    volatile LWLockWaitState lwWaiting;
    LWLockMode lwWaitMode;
    struct VbProc* lwWaitNext;
    struct VbProc* lwWaitPrev;

    /* RegularLock waiter fields */
    struct LOCK* waitLock;
    int waitLockMode;
    volatile bool lockGranted;
    volatile bool deadlocked;

    /* Per-proc proclock list (singly linked via PROCLOCK.nextOnProc) */
    struct PROCLOCK* proclockHead;

    /* per-thread held LWLock array */
    LWLockHandle held_lwlocks[MAX_SIMUL_LWLOCKS];
    int num_held_lwlocks;
} VbProc;

extern VbProc vbproc_array[MAX_VBPROCS];
extern _Atomic int vbproc_count;          /* high-water mark: slots 0..count-1 ever allocated */
extern __thread VbProc* MyVbProc;

void VbProcInit(void);     /* call once per thread before using any lock  */
void VbProcRelease(void);  /* return slot to pool; called automatically on thread exit */

/* =========================================================================
 * Tier 3: RegularLock
 *
 * Full deadlock-detecting lock manager.  Closely mirrors PG lock.h / lock.c.
 * =========================================================================
 */

/* Lock method IDs */
#define DEFAULT_LOCKMETHOD       1
#define MAX_LOCKMETHODS          2

/* Lock mode constants (same values as PG) */
#define NoLock                   0
#define AccessShareLock          1
#define RowShareLock             2
#define RowExclusiveLock         3
#define ShareUpdateExclusiveLock 4
#define ShareLock                5
#define ShareRowExclusiveLock    6
#define ExclusiveLock            7
#define AccessExclusiveLock      8
#define MaxLockMode              8
#define MAX_LOCKMODES            (MaxLockMode + 1)

/*
 * LOCKTAG — uniquely identifies a lockable object.
 * Packed to 16 bytes, no padding (matches PG layout).
 */
typedef struct LOCKTAG
{
    u32 locktag_field1;
    u32 locktag_field2;
    u32 locktag_field3;
    uint16_t locktag_field4;
    uint8_t locktag_type;
    uint8_t locktag_lockmethodid;
} LOCKTAG;  /* 16 bytes */

/* locktag_type values */
#define LOCKTAG_RELATION           0
#define LOCKTAG_RELATION_EXTEND    1
#define LOCKTAG_PAGE               2
#define LOCKTAG_TUPLE              3
#define LOCKTAG_TRANSACTION        4
#define LOCKTAG_VIRTUALTRANSACTION 5
#define LOCKTAG_OBJECT             6
#define LOCKTAG_USERLOCK           7
#define LOCKTAG_ADVISORY           8

/* SET_LOCKTAG_* convenience macros */
#define SET_LOCKTAG_RELATION(tag, dboid, reloid)                                                \
    ((tag).locktag_field1 = (u32)(dboid), (tag).locktag_field2 = (u32)(reloid),                 \
     (tag).locktag_field3 = 0, (tag).locktag_field4 = 0, (tag).locktag_type = LOCKTAG_RELATION, \
     (tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD)

#define SET_LOCKTAG_PAGE(tag, dboid, reloid, blkno)                             \
    ((tag).locktag_field1 = (u32)(dboid), (tag).locktag_field2 = (u32)(reloid), \
     (tag).locktag_field3 = (u32)(blkno), (tag).locktag_field4 = 0,             \
     (tag).locktag_type = LOCKTAG_PAGE, (tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD)

#define SET_LOCKTAG_TUPLE(tag, dboid, reloid, blkno, offno)                         \
    ((tag).locktag_field1 = (u32)(dboid), (tag).locktag_field2 = (u32)(reloid),     \
     (tag).locktag_field3 = (u32)(blkno), (tag).locktag_field4 = (uint16_t)(offno), \
     (tag).locktag_type = LOCKTAG_TUPLE, (tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD)

#define SET_LOCKTAG_ADVISORY(tag, f1, f2, f3, f4)                             \
    ((tag).locktag_field1 = (u32)(f1), (tag).locktag_field2 = (u32)(f2),      \
     (tag).locktag_field3 = (u32)(f3), (tag).locktag_field4 = (uint16_t)(f4), \
     (tag).locktag_type = LOCKTAG_ADVISORY, (tag).locktag_lockmethodid = DEFAULT_LOCKMETHOD)

/* Conflict table index: bit (mode) set means 'mode' conflicts */
extern const u32 LockConflicts[MAX_LOCKMODES];

/* Forward declarations */
struct PROCLOCK;

/*
 * LOCK — per-lock-object state in the shared lock table.
 */
typedef struct LOCK
{
    LOCKTAG tag;                       /* identifies the locked object      */
    u32 grantMask;                 /* bitmask of currently granted modes */
    u32 waitMask;                  /* bitmask of awaited modes           */
    int requested[MAX_LOCKMODES];  /* #requests per mode (incl. granted) */
    int nRequested;
    int granted[MAX_LOCKMODES];    /* #grants per mode                   */
    int nGranted;
    /* wait queue: linked through VbProc.waitLock/waitLockMode */
    struct VbProc* waitHead;
    struct VbProc* waitTail;
    /* proclock list */
    struct PROCLOCK* proclockHead;
    /* hash chain (within partition bucket) */
    struct LOCK* next;
} LOCK;

/*
 * PROCLOCK — per-(proc, lock) state.
 */
typedef struct PROCLOCK
{
    LOCK* myLock;
    struct VbProc* myProc;
    u32 holdMask;       /* modes currently held by this proc      */
    u32 releaseMask;    /* modes to release on transaction end     */
    /* hash chain within partition */
    struct PROCLOCK* nextInPartition;
    /* per-proc list chain */
    struct PROCLOCK* nextOnProc;
    /* per-lock list chain */
    struct PROCLOCK* nextOnLock;
} PROCLOCK;

/*
 * LOCALLOCK — thread-local shadow of a lock acquisition.
 * Avoids round-tripping to the shared table for recursive acquires.
 */
typedef struct LOCALLOCK
{
    LOCKTAG lock;
    int mode;
    u32 hashcode;
    LOCK* lockPtr;
    PROCLOCK* proclockPtr;
    int64_t nLocks;       /* recursion depth                         */
    struct LOCALLOCK* next;        /* hash chain                              */
} LOCALLOCK;

/* LockAcquire return values */
typedef enum
{
    LOCKACQUIRE_NOT_AVAIL = 0,   /* deadlock / timeout                     */
    LOCKACQUIRE_OK = 1,   /* new grant                               */
    LOCKACQUIRE_ALREADY_HELD = 2   /* recursive acquire                       */
} LockAcquireResult;

/* Deadlock check result */
typedef enum
{
    DS_NO_DEADLOCK = 0,
    DS_SOFT_DEADLOCK = 1,   /* can be resolved by aborting a waiter           */
    DS_HARD_DEADLOCK = 2    /* unavoidable deadlock                           */
} DeadLockState;

/* Number of partitions in the lock hash table (must be power of 2) */
#define NUM_LOCK_PARTITIONS 16
#define LOCK_HASH_SIZE      64   /* buckets per partition                    */

/* LOCALLOCK hash table parameters */
#define LOCALLOCK_HASH_SIZE 64

/* Public RegularLock API */
void InitLocks(void);
LockAcquireResult LockAcquire(const LOCKTAG* locktag, int lockmode, bool dontWait);
bool LockRelease(const LOCKTAG* locktag, int lockmode);
void LockReleaseAll(void);
DeadLockState DeadLockCheck(VbProc* proc);

/* =========================================================================
 * Convenience: single initializer for the whole lock subsystem
 * =========================================================================
 */
void LockSubsystemInit(void);   /* call once at process start */
#endif