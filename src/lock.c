/**
 * lock.c — Three-tier lock system implementation for VectorBase
 *
 * Port of PostgreSQL SpinLock (s_lock.c), LWLock (lwlock.c) and
 * RegularLock (lock.c).  All three tiers are in one translation unit to
 * keep the module self-contained; the exported surface is in lock.h.
 */

#include "lock.h"
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>

/* Private Min/Max — avoid conflicts with vb_type.h or other headers */
#ifndef VB_LOCK_MIN
#define VB_LOCK_MIN(a, b) ((a) < (b) ? (a) : (b))
#endif
#ifndef VB_LOCK_MAX
#define VB_LOCK_MAX(a, b) ((a) > (b) ? (a) : (b))
#endif

/* =========================================================================
 * VbProc — global array + thread-local pointer
 *
 * P0 fix — slot reuse via free bitmap:
 *   vbproc_free_bits tracks released slots (bit i = 1 → slot i is free).
 *   VbProcInit() first scans the bitmap for a reusable slot with a CAS
 *   claim; only if no free slot exists does it advance vbproc_count
 *   (the high-water mark).  This prevents the array from being exhausted
 *   by repeated thread creation/destruction cycles.
 *
 * P1 fix — automatic LOCALLOCK cleanup on thread exit:
 *   A pthread cleanup key (vbproc_cleanup_key) is created once via
 *   pthread_once.  Every thread that calls VbProcInit() registers a
 *   non-NULL sentinel with pthread_setspecific; the associated destructor
 *   (vbproc_thread_cleanup → VbProcRelease) then runs automatically when
 *   the thread exits, calling LockReleaseAll() + LWLockReleaseAll() before
 *   returning the slot to the free pool.  No LOCALLOCK entries are leaked
 *   even on abnormal thread exit.
 * =========================================================================
 */

VbProc vbproc_array[MAX_VBPROCS];
_Atomic int vbproc_count = 0;            /* high-water mark */
__thread VbProc* MyVbProc = NULL;

/* Bitmap of released (reusable) slots.  bit i = 1 → vbproc_array[i] free. */
static _Atomic u64 vbproc_free_bits[MAX_VBPROCS / 64];

/* pthread key for automatic per-thread cleanup on exit */
static pthread_key_t vbproc_cleanup_key;
static pthread_once_t vbproc_key_once = PTHREAD_ONCE_INIT;

/* Forward declaration — defined below VbProcInit */
static void vbproc_thread_cleanup(void* arg);

static void make_vbproc_key(void)
{
    pthread_key_create(&vbproc_cleanup_key, vbproc_thread_cleanup);
}

/*
 * VbProcRelease — return the current thread's VbProc slot to the pool.
 *
 * Releases all held locks first, so the slot is always in a clean state
 * when reclaimed by a new thread.  Idempotent: safe to call multiple times.
 * Also called automatically by the pthread destructor on thread exit.
 */
void VbProcRelease(void)
{
    if (MyVbProc == NULL) return;   /* already released or never initialised */

    /*
     * Deregister the cleanup key first.  This prevents a double-call if
     * the user calls VbProcRelease() explicitly before thread exit AND the
     * pthread destructor fires afterwards.
     */
    pthread_setspecific(vbproc_cleanup_key, NULL);

    /* Release all regular locks (frees LOCALLOCK, LOCK, PROCLOCK entries) */
    LockReleaseAll();
    /* Release any held LW locks */
    LWLockReleaseAll();

    int slot = MyVbProc->procno;

    /* Drain any leftover semaphore posts so the next owner sees count = 0 */
    while (sem_trywait(&MyVbProc->sem) == 0);

    /* Return slot to the free pool via the bitmap */
    int word = slot / 64;
    u64 bit = (u64)1 << (slot % 64);
    atomic_fetch_or_explicit(&vbproc_free_bits[word], bit, memory_order_release);

    MyVbProc = NULL;
}

/* pthread key destructor — fires automatically when a thread exits */
static void vbproc_thread_cleanup(void* arg)
{
    (void)arg;
    VbProcRelease();
}

/*
 * VbProcInit — initialise the current thread's VbProc slot.
 *
 * Slot acquisition order:
 *   1. Scan vbproc_free_bits for a released slot and claim it with CAS.
 *   2. If none found, advance vbproc_count (high-water mark).
 *
 * Registers vbproc_thread_cleanup via a pthread key so the slot is
 * automatically returned on thread exit (P1 fix).
 */
void VbProcInit(void)
{
    if (MyVbProc != NULL) return;   /* already initialised for this thread */

    /* Ensure the cleanup pthread key is created exactly once */
    pthread_once(&vbproc_key_once, make_vbproc_key);

    int slot = -1;

    /* Step 1: scan the free bitmap for a reusable slot */
    for (int w = 0; w < MAX_VBPROCS / 64 && slot == -1; w++)
    {
        u64 bits = atomic_load_explicit(&vbproc_free_bits[w], memory_order_acquire);
        while (bits != 0)
        {
            int b = __builtin_ctzll(bits);   /* lowest set bit */
            u64 mask = (u64)1 << b;
            /*
             * CAS: claim bit b (clear it).  On failure, atomic_compare_exchange
             * writes the current value back into `bits` — we retry with the
             * refreshed bitmap word without re-loading.
             */
            if (atomic_compare_exchange_weak_explicit(&vbproc_free_bits[w], &bits, bits & ~mask,
                                                      memory_order_acquire, memory_order_relaxed))
            {
                slot = w * 64 + b;
                break;
            }
        }
    }

    /* Step 2: allocate a fresh slot from the high-water mark */
    if (slot == -1)
    {
        slot = atomic_fetch_add_explicit(&vbproc_count, 1, memory_order_relaxed);
        assert(slot < MAX_VBPROCS && "VbProc array exhausted — too many concurrent threads");
    }

    VbProc* p = &vbproc_array[slot];

    /*
     * Initialise the semaphore.
     *   Fresh slot  (sem_initialized == false): call sem_init() for the first time.
     *   Reused slot (sem_initialized == true) : drain any leftover posts from the
     *                                           previous owner; do NOT call sem_init()
     *                                           again (that would be UB on Linux).
     */
    if (!p->sem_initialized)
    {
        sem_init(&p->sem, 0 /* not process-shared */, 0 /* initial count */);
        p->sem_initialized = true;
    }
    else
    {
        while (sem_trywait(&p->sem) == 0)   /* drain to count = 0 */
            ;
    }

    p->procno = slot;
    p->lwWaiting = LW_WS_NOT_WAITING;
    p->lwWaitMode = LW_EXCLUSIVE;
    p->lwWaitNext = NULL;
    p->lwWaitPrev = NULL;
    p->waitLock = NULL;
    p->waitLockMode = NoLock;
    p->lockGranted = false;
    p->deadlocked = false;
    p->proclockHead = NULL;
    p->num_held_lwlocks = 0;

    MyVbProc = p;

    /*
     * Register the automatic cleanup destructor.
     * Any non-NULL value triggers the destructor on thread exit.
     */
    pthread_setspecific(vbproc_cleanup_key, (void*)(uintptr_t)1);
}

/* =========================================================================
 * Tier 1: SpinLock
 * =========================================================================
 */

/*
 * spins_per_delay — file-static; adapted by perform_spin_delay / finish_spin_delay.
 * Mirrors PG's local static variable in perform_spin_delay().
 */
static int spins_per_delay = DEFAULT_SPINS_PER_DELAY;

/*
 * s_lock() — slow path for SpinLock acquisition.
 *
 * Called only when TAS_SPIN fails.  Implements the exponential-backoff
 * strategy from PG: spin spins_per_delay times, then sleep with increasing
 * delay up to 1 s.  Abort after NUM_DELAYS sleep rounds.
 *
 * Returns total delay count (informational, mirrors PG).
 */
int s_lock(slock_t* lock, const char* file, int line, const char* func)
{
    SpinDelayStatus delayStatus;

    delayStatus.spins = 0;
    delayStatus.delays = 0;
    delayStatus.cur_delay = 0;
    delayStatus.file = file;
    delayStatus.line = line;
    delayStatus.func = func;

    while (TAS_SPIN(lock)) perform_spin_delay(&delayStatus);

    finish_spin_delay(&delayStatus);
    return delayStatus.delays;
}

/*
 * perform_spin_delay() — called each iteration while lock is held by someone else.
 *
 * First, spin spins_per_delay times (busy-wait).  Once spins are exhausted,
 * sleep cur_delay microseconds, doubling each time up to MAX_SPINLOCK_DELAY.
 * After NUM_DELAYS sleep rounds, print a diagnostic and abort().
 */
void perform_spin_delay(SpinDelayStatus* status)
{
    /* Busy-spin phase */
    if (status->spins < spins_per_delay)
    {
        status->spins++;
        /* CPU relax hint — avoids memory bus saturation */
        __asm__ __volatile__("pause" ::: "memory");
        return;
    }

    /* Transition to sleep phase */
    if (status->cur_delay == 0) status->cur_delay = MIN_SPINLOCK_DELAY;

    /* Check spin-timeout */
    if (status->delays >= NUM_DELAYS)
    {
        fprintf(stderr, "spinlock wait too long at %s:%d (%s)\n", status->file, status->line,
                status->func);
        abort();
    }

    /* Sleep cur_delay microseconds */
    struct timespec ts;
    ts.tv_sec = status->cur_delay / 1000000;
    ts.tv_nsec = (status->cur_delay % 1000000) * 1000;
    nanosleep(&ts, NULL);

    status->delays++;
    status->spins = 0;   /* reset spin counter for next round */

    /* Exponential backoff with a random factor (mirrors PG) */
    status->cur_delay += status->cur_delay / 2;
    /* Add small random jitter: 0..cur_delay/8 */
    status->cur_delay += (int)((unsigned)status->cur_delay * ((u32)rand() & 0xFF) / 2048);
    if (status->cur_delay > MAX_SPINLOCK_DELAY) status->cur_delay = MIN_SPINLOCK_DELAY; /* wrap */
}

/*
 * finish_spin_delay() — adapt spins_per_delay based on observed contention.
 *
 * If we had to sleep (delays > 0), reduce spins_per_delay to detect
 * contention earlier next time.  If we succeeded without sleeping,
 * cautiously increase it.
 */
void finish_spin_delay(SpinDelayStatus* status)
{
    if (status->cur_delay == 0)
    {
        /* No sleeping needed; slightly increase spin budget */
        if (spins_per_delay < 1000) spins_per_delay = VB_LOCK_MIN(spins_per_delay + 100, 1000);
    }
    else
    {
        /* We had to sleep; reduce spin budget */
        if (spins_per_delay > 10) spins_per_delay = VB_LOCK_MAX(spins_per_delay - 10, 10);
    }
}

/* =========================================================================
 * Tier 2: LWLock
 * =========================================================================
 */

/* Helpers for spin-locking the LWLock wait list via LW_FLAG_LOCKED */
static inline void lwlock_waitlist_lock(LWLock* lock)
{
    u32 old;
    for (;;)
    {
        old = atomic_fetch_or_explicit(&lock->state, LW_FLAG_LOCKED, memory_order_acquire);
        if (!(old & LW_FLAG_LOCKED)) return; /* we got it */
        /* Busy-wait with CPU relax */
        __asm__ __volatile__("pause" ::: "memory");
    }
}

static inline void lwlock_waitlist_unlock(LWLock* lock)
{
    atomic_fetch_and_explicit(&lock->state, ~LW_FLAG_LOCKED, memory_order_release);
}

/*
 * LWLockInit — initialise an LWLock (state=0, empty wait list).
 */
void LWLockInit(LWLock* lock, const char* name)
{
    lock->name = name;
    atomic_init(&lock->state, 0);
    lock->head = NULL;
    lock->tail = NULL;
}

/*
 * LWLockAttemptLock — try to grab the lock without blocking.
 *
 * Returns true  if we must wait (lock not acquired).
 * Returns false if lock was acquired.
 *
 * EXCLUSIVE: lock_free = (state & LW_LOCK_MASK) == 0
 *            acquire by adding LW_VAL_EXCLUSIVE
 * SHARED:    lock_free = (state & LW_VAL_EXCLUSIVE) == 0
 *            acquire by adding LW_VAL_SHARED
 *
 * Uses a CAS loop (not a simple swap) to avoid stomping on other flags.
 */
static bool LWLockAttemptLock(LWLock* lock, LWLockMode mode)
{
    u32 expected, desired, current;

    current = atomic_load_explicit(&lock->state, memory_order_relaxed);
    for (;;)
    {
        bool lock_free;

        if (mode == LW_EXCLUSIVE)
            lock_free = (current & LW_LOCK_MASK) == 0;
        else
            lock_free = (current & LW_VAL_EXCLUSIVE) == 0;

        if (!lock_free) return true; /* must wait */

        expected = current;
        if (mode == LW_EXCLUSIVE)
            desired = current + LW_VAL_EXCLUSIVE;
        else
            desired = current + LW_VAL_SHARED;

        if (atomic_compare_exchange_weak_explicit(&lock->state, &expected, desired,
                                                  memory_order_acquire, memory_order_relaxed))
            return false; /* acquired */

        /* CAS failed; expected now holds the current value */
        current = expected;
    }
}

/*
 * LWLockQueueSelf — add MyVbProc to the lock's wait list.
 *
 * Caller must NOT hold LW_FLAG_LOCKED; this function acquires and releases it.
 * Sets LW_FLAG_HAS_WAITERS.
 */
static void LWLockQueueSelf(LWLock* lock, LWLockMode mode)
{
    VbProc* proc = MyVbProc;
    assert(proc != NULL);
    assert(proc->lwWaiting == LW_WS_NOT_WAITING);

    proc->lwWaiting = LW_WS_WAITING;
    proc->lwWaitMode = mode;
    proc->lwWaitNext = NULL;
    proc->lwWaitPrev = NULL;

    lwlock_waitlist_lock(lock);

    /* Append to tail */
    if (lock->tail == NULL)
    {
        lock->head = proc;
        lock->tail = proc;
    }
    else
    {
        lock->tail->lwWaitNext = proc;
        proc->lwWaitPrev = lock->tail;
        lock->tail = proc;
    }

    /* Mark that there are waiters; clear LOCKED bit */
    u32 old = atomic_fetch_or_explicit(&lock->state, LW_FLAG_HAS_WAITERS, memory_order_relaxed);
    (void)old;
    lwlock_waitlist_unlock(lock);
}

/*
 * LWLockDequeueSelf — remove MyVbProc from the wait list (if still there).
 *
 * Called when LWLockAttemptLock succeeds in phase 3 before sleeping.
 *
 * There is a subtle race: LWLockWakeup may have already removed the proc from
 * the wait list and set lwWaiting=PENDING_WAKEUP — but it may not have called
 * sem_post yet.  We handle this by waiting for the waker to complete setting
 * lwWaiting=NOT_WAITING and then draining the semaphore.  This prevents a
 * spurious future sem_wait return.
 */
static void LWLockDequeueSelf(LWLock* lock)
{
    VbProc* proc = MyVbProc;

    lwlock_waitlist_lock(lock);

    if (proc->lwWaiting == LW_WS_WAITING)
    {
        /* Still in the wait list — remove ourselves */
        if (proc->lwWaitPrev != NULL)
            proc->lwWaitPrev->lwWaitNext = proc->lwWaitNext;
        else
            lock->head = proc->lwWaitNext;

        if (proc->lwWaitNext != NULL)
            proc->lwWaitNext->lwWaitPrev = proc->lwWaitPrev;
        else
            lock->tail = proc->lwWaitPrev;

        proc->lwWaitNext = NULL;
        proc->lwWaitPrev = NULL;

        /* If list is now empty, clear HAS_WAITERS */
        if (lock->head == NULL)
            atomic_fetch_and_explicit(&lock->state, ~LW_FLAG_HAS_WAITERS, memory_order_relaxed);

        lwlock_waitlist_unlock(lock);

        /* lwWaiting was WAITING; no waker will post our sem */
        proc->lwWaiting = LW_WS_NOT_WAITING;
        return;
    }

    /* lwWaiting == PENDING_WAKEUP or NOT_WAITING: waker already removed us. */
    lwlock_waitlist_unlock(lock);

    /*
     * The waker has already removed us from the list.  Either it is in
     * the process of calling sem_post (PENDING_WAKEUP) or it has already
     * completed (NOT_WAITING).
     *
     * In both cases a sem_post will arrive (or has arrived).  Spin until
     * lwWaiting becomes NOT_WAITING to ensure sem_post is complete, then
     * drain the pending count so future sem_waits don't spuriously return.
     */
    while (proc->lwWaiting != LW_WS_NOT_WAITING) __asm__ __volatile__("pause" ::: "memory");
    sem_wait(&proc->sem);
}

/*
 * LWLockWakeup — wake compatible waiters after releasing a lock.
 *
 * For a SHARED release: wake the first exclusive waiter OR all leading
 * shared waiters (whichever comes first in the queue).
 * For an EXCLUSIVE release: same logic — wake shared OR first exclusive.
 *
 * Actually we mirror PG: wake all leading SHARED waiters up to but not
 * including the first EXCLUSIVE waiter; OR wake the first EXCLUSIVE waiter
 * if the queue starts with one.
 */
static void LWLockWakeup(LWLock* lock)
{
    /* Collect the procs to wake while holding the wait-list lock */
    VbProc* wake_list = NULL; /* singly linked via lwWaitNext after removal */
    VbProc* wake_tail = NULL;
    bool want_release_ok = false;
    bool has_more_waiters = false;

    lwlock_waitlist_lock(lock);

    VbProc* cur = lock->head;
    while (cur != NULL)
    {
        VbProc* next = cur->lwWaitNext;

        if (cur->lwWaitMode == LW_EXCLUSIVE)
        {
            if (wake_list == NULL)
            {
                /* Wake this exclusive waiter and stop */
                lock->head = next;
                if (next != NULL)
                    next->lwWaitPrev = NULL;
                else
                    lock->tail = NULL;

                cur->lwWaiting = LW_WS_PENDING_WAKEUP;
                cur->lwWaitNext = NULL;
                cur->lwWaitPrev = NULL;
                wake_list = cur;
                wake_tail = cur;
                /* Check if more waiters remain */
                has_more_waiters = (lock->head != NULL);
            }
            else
            {
                /* Shared waiters were added; stop here, more waiters exist */
                has_more_waiters = true;
            }
            break;
        }
        else
        {
            /* LW_SHARED or LW_WAIT_UNTIL_FREE — wake all leading shared */
            lock->head = next;
            if (next != NULL)
                next->lwWaitPrev = NULL;
            else
                lock->tail = NULL;

            cur->lwWaiting = LW_WS_PENDING_WAKEUP;
            cur->lwWaitNext = NULL;
            cur->lwWaitPrev = NULL;

            if (wake_list == NULL)
            {
                wake_list = cur;
                wake_tail = cur;
            }
            else
            {
                wake_tail->lwWaitNext = cur;
                wake_tail = cur;
            }
            want_release_ok = true;
        }

        cur = next;
    }

    /* Adjust flags before releasing wait-list lock */
    u32 new_flags = 0;
    if (has_more_waiters) new_flags |= LW_FLAG_HAS_WAITERS;
    if (want_release_ok) new_flags |= LW_FLAG_RELEASE_OK;

    /* Atomically: clear LOCKED + HAS_WAITERS + RELEASE_OK, then OR new_flags */
    u32 clear_mask = LW_FLAG_LOCKED | LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK;
    u32 old_state, desired;
    u32 cur_state = atomic_load_explicit(&lock->state, memory_order_relaxed);
    do
    {
        old_state = cur_state;
        desired = (old_state & ~clear_mask) | new_flags;
    } while (!atomic_compare_exchange_weak_explicit(&lock->state, &cur_state, desired,
                                                    memory_order_release, memory_order_relaxed));

    /* Write barrier then post semaphores */
    atomic_thread_fence(memory_order_seq_cst);

    for (VbProc* w = wake_list; w != NULL;)
    {
        VbProc* wn = w->lwWaitNext;
        w->lwWaiting = LW_WS_NOT_WAITING;
        sem_post(&w->sem);
        w = wn;
    }
}

/*
 * LWLockAcquire — acquire LWLock in exclusive or shared mode.
 *
 * Four-phase algorithm (mirrors PG exactly):
 *   Phase 1: LWLockAttemptLock()
 *   Phase 2: LWLockQueueSelf()
 *   Phase 3: LWLockAttemptLock() again (avoid race: lock may have been
 *             released between phase 1 and phase 2)
 *   Phase 4: sem_wait() until woken by LWLockWakeup()
 *            On wake, set LW_FLAG_RELEASE_OK (we've re-acquired), loop back
 */
void LWLockAcquire(LWLock* lock, LWLockMode mode)
{
    /* Auto-initialize per-thread VbProc on first use (single-threaded callers). */
    if (MyVbProc == NULL) VbProcInit();
    VbProc* proc = MyVbProc;
    assert(proc != NULL && "VbProcInit() not called");
    assert(mode == LW_EXCLUSIVE || mode == LW_SHARED);

    for (;;)
    {
        /* Phase 1: fast path */
        if (!LWLockAttemptLock(lock, mode)) break; /* acquired */

        /* Phase 2: queue ourselves */
        LWLockQueueSelf(lock, mode);

        /* Phase 3: try again in case lock was released after phase 1 */
        if (!LWLockAttemptLock(lock, mode))
        {
            LWLockDequeueSelf(lock);
            break; /* acquired in phase 3 */
        }

        /* Phase 4: block until woken by LWLockWakeup() */
        sem_wait(&proc->sem);

        /*
         * After wakeup:
         *  - lwWaiting has been set to LW_WS_NOT_WAITING by the waker.
         *  - We have been removed from the wait list.
         *  - The lock may or may not be free now (another thread could
         *    have re-acquired it between our wakeup and now).
         * Loop back to phase 1 to try again.
         */
    }

    /* Record in held array */
    assert(proc->num_held_lwlocks < MAX_SIMUL_LWLOCKS);
    proc->held_lwlocks[proc->num_held_lwlocks].lock = lock;
    proc->held_lwlocks[proc->num_held_lwlocks].mode = mode;
    proc->num_held_lwlocks++;
}

/*
 * LWLockConditionalAcquire — try to acquire; return false immediately if busy.
 */
bool LWLockConditionalAcquire(LWLock* lock, LWLockMode mode)
{
    if (MyVbProc == NULL) VbProcInit();
    VbProc* proc = MyVbProc;
    assert(proc != NULL && "VbProcInit() not called");

    if (LWLockAttemptLock(lock, mode)) return false; /* busy */

    assert(proc->num_held_lwlocks < MAX_SIMUL_LWLOCKS);
    proc->held_lwlocks[proc->num_held_lwlocks].lock = lock;
    proc->held_lwlocks[proc->num_held_lwlocks].mode = mode;
    proc->num_held_lwlocks++;
    return true;
}

/*
 * LWLockAcquireOrWait — acquire, OR wait until the lock is free (not held).
 *
 * Used with LW_WAIT_UNTIL_FREE: if someone holds the lock, block until
 * released (but don't acquire ourselves — just continue after it's free).
 *
 * Returns true  if we acquired the lock.
 * Returns false if we just waited and the lock is now free.
 */
bool LWLockAcquireOrWait(LWLock* lock, LWLockMode mode)
{
    if (MyVbProc == NULL) VbProcInit();
    VbProc* proc = MyVbProc;
    assert(proc != NULL && "VbProcInit() not called");

    /* If lock is already free, acquire it */
    if (!LWLockAttemptLock(lock, mode))
    {
        assert(proc->num_held_lwlocks < MAX_SIMUL_LWLOCKS);
        proc->held_lwlocks[proc->num_held_lwlocks].lock = lock;
        proc->held_lwlocks[proc->num_held_lwlocks].mode = mode;
        proc->num_held_lwlocks++;
        return true;
    }

    /* Lock is held; queue and wait, but we will NOT acquire */
    LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);

    /* Check again — lock may have been released already */
    u32 state = atomic_load_explicit(&lock->state, memory_order_acquire);
    if ((state & LW_VAL_EXCLUSIVE) == 0)
    {
        LWLockDequeueSelf(lock);
        return false; /* already free, didn't acquire */
    }

    sem_wait(&proc->sem);
    /* Woke up; the lock is free but we don't hold it */
    return false;
}

/*
 * LWLockRelease — release an LWLock previously acquired by this thread.
 */
void LWLockRelease(LWLock* lock)
{
    VbProc* proc = MyVbProc;
    assert(proc != NULL);

    /* Remove from held array */
    int i;
    for (i = proc->num_held_lwlocks - 1; i >= 0; i--)
    {
        if (proc->held_lwlocks[i].lock == lock) break;
    }
    assert(i >= 0 && "LWLockRelease: lock not held");

    LWLockMode mode = proc->held_lwlocks[i].mode;

    /* Shift remaining entries down */
    for (int j = i; j < proc->num_held_lwlocks - 1; j++)
        proc->held_lwlocks[j] = proc->held_lwlocks[j + 1];
    proc->num_held_lwlocks--;

    /* Release the state word */
    u32 old_state;
    if (mode == LW_EXCLUSIVE)
        old_state = atomic_fetch_sub_explicit(&lock->state, LW_VAL_EXCLUSIVE, memory_order_release);
    else
        old_state = atomic_fetch_sub_explicit(&lock->state, LW_VAL_SHARED, memory_order_release);

    /*
     * Wake waiters if any.
     *
     * Always call LWLockWakeup when HAS_WAITERS is set — it handles
     * thundering-herd prevention internally via RELEASE_OK/LOCKED.
     */
    if (old_state & LW_FLAG_HAS_WAITERS) LWLockWakeup(lock);
}

/*
 * LWLockReleaseClearVar — release + atomically clear a variable.
 *
 * Used when an LWLock protects a 64-bit variable and the caller wants to
 * zero it as part of the release.
 */
void LWLockReleaseClearVar(LWLock* lock, u64* var, u64 val)
{
    *var = val;
    atomic_thread_fence(memory_order_release);
    LWLockRelease(lock);
}

/*
 * LWLockReleaseAll — release all LWLocks held by this thread, in reverse order.
 */
void LWLockReleaseAll(void)
{
    VbProc* proc = MyVbProc;
    if (proc == NULL) return;
    while (proc->num_held_lwlocks > 0)
        LWLockRelease(proc->held_lwlocks[proc->num_held_lwlocks - 1].lock);
}

/*
 * LWLockHeldByMe — return true if this thread holds the given lock.
 */
bool LWLockHeldByMe(LWLock* lock)
{
    VbProc* proc = MyVbProc;
    if (proc == NULL) return false;
    for (int i = 0; i < proc->num_held_lwlocks; i++)
        if (proc->held_lwlocks[i].lock == lock) return true;
    return false;
}

/* =========================================================================
 * Tier 3: RegularLock
 * =========================================================================
 */

/* Conflict table — identical values to PG */
const u32 LockConflicts[MAX_LOCKMODES] = {
    /* NoLock (0) */ 0,
    /* AccessShareLock (1) */ (1 << AccessExclusiveLock),
    /* RowShareLock (2) */ (1 << AccessExclusiveLock) | (1 << ExclusiveLock),
    /* RowExclusiveLock (3) */ (1 << AccessExclusiveLock) | (1 << ExclusiveLock) |
        (1 << ShareRowExclusiveLock) | (1 << ShareLock),
    /* ShareUpdateExclusiveLock (4)*/ (1 << AccessExclusiveLock) | (1 << ExclusiveLock) |
        (1 << ShareRowExclusiveLock) | (1 << ShareLock) | (1 << ShareUpdateExclusiveLock),
    /* ShareLock (5) */ (1 << AccessExclusiveLock) | (1 << ExclusiveLock) |
        (1 << ShareRowExclusiveLock) | (1 << RowExclusiveLock) | (1 << ShareUpdateExclusiveLock),
    /* ShareRowExclusiveLock (6) */ (1 << AccessExclusiveLock) | (1 << ExclusiveLock) |
        (1 << ShareRowExclusiveLock) | (1 << ShareLock) | (1 << RowExclusiveLock) |
        (1 << ShareUpdateExclusiveLock),
    /* ExclusiveLock (7) */ (1 << AccessExclusiveLock) | (1 << ExclusiveLock) |
        (1 << ShareRowExclusiveLock) | (1 << ShareLock) | (1 << RowExclusiveLock) |
        (1 << ShareUpdateExclusiveLock) | (1 << RowShareLock),
    /* AccessExclusiveLock (8) */ (1 << AccessExclusiveLock) | (1 << ExclusiveLock) |
        (1 << ShareRowExclusiveLock) | (1 << ShareLock) | (1 << RowExclusiveLock) |
        (1 << ShareUpdateExclusiveLock) | (1 << RowShareLock) | (1 << AccessShareLock),
};

/* -------------------------------------------------------------------------
 * Partition tables
 * -------------------------------------------------------------------------
 */

typedef struct LockPartition
{
    LWLock lock; /* partition LWLock      */
    LOCK* buckets[LOCK_HASH_SIZE]; /* LOCK hash buckets     */
    PROCLOCK* pl_buckets[LOCK_HASH_SIZE]; /* PROCLOCK hash buckets */
} LockPartition;

static LockPartition lock_partitions[NUM_LOCK_PARTITIONS];

/* -------------------------------------------------------------------------
 * LOCALLOCK hash table (per-thread)
 * -------------------------------------------------------------------------
 */
static __thread LOCALLOCK* locallock_table[LOCALLOCK_HASH_SIZE];

/* -------------------------------------------------------------------------
 * Hash helpers
 * -------------------------------------------------------------------------
 */

static u32 locktag_hash(const LOCKTAG* tag)
{
    /* FNV-1a over the 16 bytes of LOCKTAG */
    const uint8_t* p = (const uint8_t*)tag;
    u32 h = 2166136261u;
    for (int i = 0; i < (int)sizeof(LOCKTAG); i++)
    {
        h ^= p[i];
        h *= 16777619u;
    }
    return h;
}

static inline int lock_partition_id(u32 hashcode)
{
    return (int)(hashcode % NUM_LOCK_PARTITIONS);
}

static inline int lock_bucket_id(u32 hashcode)
{
    return (int)((hashcode / NUM_LOCK_PARTITIONS) % LOCK_HASH_SIZE);
}

/* PROCLOCK key hash — combine LOCK pointer and VbProc pointer */
static u32 proclock_hash(const LOCK* lock, const VbProc* proc)
{
    u64 k = ((u64)(uintptr_t)lock) ^ ((u64)(uintptr_t)proc * 2654435761u);
    return (u32)(k ^ (k >> 32));
}

static inline int proclock_bucket_id(u32 hashcode)
{
    return (int)(hashcode % LOCK_HASH_SIZE);
}

/* -------------------------------------------------------------------------
 * LOCK lookup / create (caller holds partition LWLock exclusive)
 * -------------------------------------------------------------------------
 */

static LOCK* find_or_create_lock(LockPartition* part, const LOCKTAG* tag, u32 hashcode)
{
    int bucket = lock_bucket_id(hashcode);
    LOCK* lock = part->buckets[bucket];

    while (lock != NULL)
    {
        if (memcmp(&lock->tag, tag, sizeof(LOCKTAG)) == 0) return lock;
        lock = lock->next;
    }

    /* Create new */
    lock = (LOCK*)calloc(1, sizeof(LOCK));
    assert(lock != NULL);
    lock->tag = *tag;
    lock->grantMask = 0;
    lock->waitMask = 0;
    lock->nRequested = 0;
    lock->nGranted = 0;
    lock->waitHead = NULL;
    lock->waitTail = NULL;
    lock->proclockHead = NULL;
    lock->next = part->buckets[bucket];
    part->buckets[bucket] = lock;
    return lock;
}

static void remove_lock_if_empty(LockPartition* part, LOCK* lock, u32 hashcode)
{
    if (lock->nGranted > 0 || lock->nRequested > 0 || lock->waitHead != NULL) return;

    int bucket = lock_bucket_id(hashcode);
    LOCK** pp = &part->buckets[bucket];
    while (*pp != NULL)
    {
        if (*pp == lock)
        {
            *pp = lock->next;
            free(lock);
            return;
        }
        pp = &(*pp)->next;
    }
}

/* -------------------------------------------------------------------------
 * PROCLOCK lookup / create (caller holds partition LWLock exclusive)
 * -------------------------------------------------------------------------
 */

static PROCLOCK* find_or_create_proclock(LockPartition* part, LOCK* lock, VbProc* proc,
                                         u32 lock_hashcode)
{
    u32 ph = proclock_hash(lock, proc);
    int bucket = proclock_bucket_id(ph);
    PROCLOCK* pl = part->pl_buckets[bucket];

    while (pl != NULL)
    {
        if (pl->myLock == lock && pl->myProc == proc) return pl;
        pl = pl->nextInPartition;
    }

    /* Create new */
    pl = (PROCLOCK*)calloc(1, sizeof(PROCLOCK));
    assert(pl != NULL);
    pl->myLock = lock;
    pl->myProc = proc;
    pl->holdMask = 0;
    pl->releaseMask = 0;

    /* Link into partition bucket */
    pl->nextInPartition = part->pl_buckets[bucket];
    part->pl_buckets[bucket] = pl;

    /* Link into per-lock list */
    pl->nextOnLock = lock->proclockHead;
    lock->proclockHead = pl;

    /* Link into per-proc list */
    pl->nextOnProc = proc->proclockHead;
    proc->proclockHead = pl;

    (void)lock_hashcode;
    return pl;
}

static void remove_proclock(LockPartition* part, PROCLOCK* pl)
{
    /* Remove from partition bucket */
    u32 ph = proclock_hash(pl->myLock, pl->myProc);
    int bucket = proclock_bucket_id(ph);
    PROCLOCK** pp = &part->pl_buckets[bucket];
    while (*pp != NULL)
    {
        if (*pp == pl)
        {
            *pp = pl->nextInPartition;
            break;
        }
        pp = &(*pp)->nextInPartition;
    }

    /* Remove from per-lock list */
    PROCLOCK** lp = &pl->myLock->proclockHead;
    while (*lp != NULL)
    {
        if (*lp == pl)
        {
            *lp = pl->nextOnLock;
            break;
        }
        lp = &(*lp)->nextOnLock;
    }

    /* Remove from per-proc list */
    PROCLOCK** xp = &pl->myProc->proclockHead;
    while (*xp != NULL)
    {
        if (*xp == pl)
        {
            *xp = pl->nextOnProc;
            break;
        }
        xp = &(*xp)->nextOnProc;
    }

    free(pl);
}

/* -------------------------------------------------------------------------
 * LOCALLOCK helpers (thread-local)
 * -------------------------------------------------------------------------
 */

static LOCALLOCK* find_locallock(const LOCKTAG* tag, int mode, u32 hashcode)
{
    int bucket = (int)(hashcode % LOCALLOCK_HASH_SIZE);
    LOCALLOCK* ll = locallock_table[bucket];

    while (ll != NULL)
    {
        if (memcmp(&ll->lock, tag, sizeof(LOCKTAG)) == 0 && ll->mode == mode) return ll;
        ll = ll->next;
    }
    return NULL;
}

static LOCALLOCK* create_locallock(const LOCKTAG* tag, int mode, u32 hashcode)
{
    LOCALLOCK* ll = (LOCALLOCK*)calloc(1, sizeof(LOCALLOCK));
    assert(ll != NULL);
    ll->lock = *tag;
    ll->mode = mode;
    ll->hashcode = hashcode;
    ll->lockPtr = NULL;
    ll->proclockPtr = NULL;
    ll->nLocks = 0;

    int bucket = (int)(hashcode % LOCALLOCK_HASH_SIZE);
    ll->next = locallock_table[bucket];
    locallock_table[bucket] = ll;
    return ll;
}

static void remove_locallock(LOCALLOCK* ll)
{
    int bucket = (int)(ll->hashcode % LOCALLOCK_HASH_SIZE);
    LOCALLOCK** pp = &locallock_table[bucket];
    while (*pp != NULL)
    {
        if (*pp == ll)
        {
            *pp = ll->next;
            free(ll);
            return;
        }
        pp = &(*pp)->next;
    }
}

/* -------------------------------------------------------------------------
 * GrantLock — record a new grant in LOCK and PROCLOCK
 * -------------------------------------------------------------------------
 */

static void GrantLock(LOCK* lock, PROCLOCK* proclock, int lockmode)
{
    lock->grantMask |= (1u << lockmode);
    lock->granted[lockmode]++;
    lock->nGranted++;
    proclock->holdMask |= (1u << lockmode);
}

/* -------------------------------------------------------------------------
 * LockCheckConflicts — decide if a new request can be granted immediately
 * -------------------------------------------------------------------------
 */

static bool LockCheckConflicts(LOCK* lock, int lockmode, VbProc* proc)
{
    /* Conflict if another holder holds a conflicting mode */
    u32 others_grant = lock->grantMask;

    /* If THIS proc already holds some mode, subtract its contribution
     * from the conflict check (self-compatibility). */
    PROCLOCK* pl = lock->proclockHead;
    while (pl != NULL)
    {
        if (pl->myProc == proc)
        {
            others_grant &= ~pl->holdMask;
            break;
        }
        pl = pl->nextOnLock;
    }

    if (others_grant & LockConflicts[lockmode]) return true; /* conflicts */

    /* Also conflict if there are waiters for a conflicting mode
     * (prevent starvation — mirrors PG LockCheckConflicts). */
    if (lock->waitMask & LockConflicts[lockmode]) return true;

    return false;
}

/*
 * WaitOnLock — add proc to the LOCK wait queue and block.
 *
 * IMPORTANT: Caller holds part->lock exclusive GOING IN.
 * This function releases it before sleeping, re-acquires after wakeup,
 * and returns with it held.
 *
 * Grant accounting:
 *   TryWakeupWaiters may set proc->lockGranted=true and call sem_post
 *   BEFORE releasing the partition lock.  In that case the lock is already
 *   granted; WaitOnLock must NOT call GrantLock again.
 *   If woken by sem_timedwait timeout (deadlock), the lock is NOT granted.
 */
static void WaitOnLock(LockPartition* part, LOCK* lock, PROCLOCK* proclock, int lockmode,
                       u32 hashcode)
{
    VbProc* proc = MyVbProc;

    /* Add to wait queue (partition lock held) */
    proc->waitLock = lock;
    proc->waitLockMode = lockmode;
    proc->lockGranted = false;
    proc->deadlocked = false;

    /* Mark that this mode is being waited for */
    lock->waitMask |= (1u << lockmode);

    /* Append to wait queue (FIFO) */
    proc->lwWaitNext = NULL;
    proc->lwWaitPrev = (VbProc*)lock->waitTail;
    if (lock->waitTail != NULL)
        ((VbProc*)lock->waitTail)->lwWaitNext = proc;
    else
        lock->waitHead = proc;
    lock->waitTail = proc;

    /* Release partition lock before sleeping (prevents deadlock with LWLock) */
    LWLockRelease(&part->lock);

    /*
     * Block with 10ms timeout for deadlock detection.
     * Loop on spurious wakeups and non-deadlock timeouts.
     */
    for (;;)
    {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 10000000; /* 10 ms */
        if (ts.tv_nsec >= 1000000000)
        {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }

        int rc = sem_timedwait(&proc->sem, &ts);
        if (rc == 0)
        {
            /* Woken by TryWakeupWaiters — lock already granted */
            break;
        }

        if (errno != ETIMEDOUT) continue; /* EINTR or other — retry */

        /*
         * Timeout: DeadLockCheck traverses the entire wait-for graph via DFS,
         * which may follow chains that span multiple LockPartitions.  Reading
         * another partition's LOCK/PROCLOCK without holding its lock is a data
         * race and can cause missed cycles.
         *
         * Fix: acquire ALL partition locks in strict ascending index order
         * (0 .. NUM_LOCK_PARTITIONS-1) before calling DeadLockCheck.  This
         * gives a globally consistent snapshot of the wait-for graph.
         *
         * Lock ordering: every caller that holds multiple partition locks must
         * acquire them in this same ascending order; deadlock-check timeout
         * handlers obey this rule, and normal LockAcquire holds at most one
         * partition lock at a time — so no new deadlock can be introduced.
         */
        for (int _pi = 0; _pi < NUM_LOCK_PARTITIONS; _pi++)
            LWLockAcquire(&lock_partitions[_pi].lock, LW_EXCLUSIVE);

        /*
         * Re-check lockGranted: TryWakeupWaiters may have granted us the lock
         * and called sem_post while we were acquiring the other partition locks.
         */
        if (proc->lockGranted)
        {
            sem_trywait(&proc->sem); /* drain possible pending post */
            for (int _pi = NUM_LOCK_PARTITIONS - 1; _pi >= 0; _pi--)
                LWLockRelease(&lock_partitions[_pi].lock);
            break;
        }

        DeadLockState dls = DeadLockCheck(proc);

        if (dls == DS_HARD_DEADLOCK)
        {
            proc->deadlocked = true;

            /* Remove ourselves from wait queue */
            if (proc->lwWaitPrev != NULL)
                proc->lwWaitPrev->lwWaitNext = proc->lwWaitNext;
            else
                lock->waitHead = proc->lwWaitNext;
            if (proc->lwWaitNext != NULL)
                proc->lwWaitNext->lwWaitPrev = proc->lwWaitPrev;
            else
                lock->waitTail = proc->lwWaitPrev;
            proc->lwWaitNext = NULL;
            proc->lwWaitPrev = NULL;

            /* Decrement requested counts (lock was never granted) */
            lock->requested[lockmode]--;
            lock->nRequested--;

            /* Recompute waitMask */
            {
                u32 new_wait = 0;
                VbProc* w2 = (VbProc*)lock->waitHead;
                while (w2 != NULL)
                {
                    new_wait |= (1u << w2->waitLockMode);
                    w2 = w2->lwWaitNext;
                }
                lock->waitMask = new_wait;
            }

            proc->waitLock = NULL;
            proc->waitLockMode = NoLock;

            /*
             * Caller contract: return with part->lock held.
             * Release all other partition locks first.
             */
            for (int _pi = NUM_LOCK_PARTITIONS - 1; _pi >= 0; _pi--)
            {
                if (&lock_partitions[_pi] != part) LWLockRelease(&lock_partitions[_pi].lock);
            }
            return; /* deadlocked — part->lock still held */
        }

        /* No deadlock: release all partition locks, keep waiting */
        for (int _pi = NUM_LOCK_PARTITIONS - 1; _pi >= 0; _pi--)
            LWLockRelease(&lock_partitions[_pi].lock);
    }

    proc->waitLock = NULL;
    proc->waitLockMode = NoLock;

    /* Re-acquire partition lock so caller can continue with cleanup */
    LWLockAcquire(&part->lock, LW_EXCLUSIVE);

    /*
     * Recompute waitMask from the actual wait queue.
     * This handles the case where we were the only waiter for this mode.
     * TryWakeupWaiters may have removed us; any remaining waiters for
     * other modes still set their bits.
     */
    {
        u32 new_wait = 0;
        VbProc* w = (VbProc*)lock->waitHead;
        while (w != NULL)
        {
            new_wait |= (1u << w->waitLockMode);
            w = w->lwWaitNext;
        }
        lock->waitMask = new_wait;
    }

    /* Note: lock->requested[lockmode] and lock->nRequested are left
     * incremented — they will be decremented by LockRelease. */

    (void)proclock;
    (void)hashcode;
}

/* -------------------------------------------------------------------------
 * TryWakeupWaiters — after releasing a mode, try to grant waiting requests.
 *
 * Called with the partition LWLock held exclusive.
 * Grants locks to compatible waiters and wakes them via sem_post.
 * Woken procs check proc->lockGranted after waking; if true, grant was
 * already recorded here and they must NOT call GrantLock again.
 * -------------------------------------------------------------------------
 */

static void TryWakeupWaiters(LOCK* lock)
{
    VbProc* cur = (VbProc*)lock->waitHead;

    while (cur != NULL)
    {
        VbProc* next = cur->lwWaitNext;
        int wmode = cur->waitLockMode;

        /* Check against current grant mask only (ignore waitMask — avoids
         * overly conservative starvation; this is what PG does in the simple
         * case without the full FIFO guarantee) */
        if (lock->grantMask & LockConflicts[wmode])
        {
            /* This waiter conflicts with a current holder; skip it.
             * For strong lock modes, we also stop (FIFO ordering). */
            if (wmode >= ExclusiveLock) break;
            cur = next;
            continue;
        }

        /* Find proclock for this proc */
        PROCLOCK* pl = lock->proclockHead;
        while (pl != NULL)
        {
            if (pl->myProc == cur) break;
            pl = pl->nextOnLock;
        }
        if (pl == NULL)
        {
            cur = next;
            continue;
        }

        /* Grant the lock to this waiter */
        GrantLock(lock, pl, wmode);

        /* Remove from wait queue */
        if (cur->lwWaitPrev != NULL)
            cur->lwWaitPrev->lwWaitNext = next;
        else
            lock->waitHead = next;
        if (next != NULL)
            next->lwWaitPrev = cur->lwWaitPrev;
        else
            lock->waitTail = cur->lwWaitPrev;
        cur->lwWaitNext = NULL;
        cur->lwWaitPrev = NULL;

        /* Update requested/waitMask (WaitOnLock will undo its own increment) */
        /* Signal the waiter that it has been granted */
        cur->lockGranted = true;
        sem_post(&cur->sem);

        /* For exclusive modes: only one waiter can be granted */
        if (wmode >= ShareUpdateExclusiveLock) break;

        cur = next;
    }
}

/* -------------------------------------------------------------------------
 * InitLocks — initialise the lock manager (call once at process start)
 * -------------------------------------------------------------------------
 */

void InitLocks(void)
{
    char name_buf[64];
    for (int i = 0; i < NUM_LOCK_PARTITIONS; i++)
    {
        snprintf(name_buf, sizeof(name_buf), "LockPartition%d", i);
        LWLockInit(&lock_partitions[i].lock, NULL);
        memset(lock_partitions[i].buckets, 0, sizeof(lock_partitions[i].buckets));
        memset(lock_partitions[i].pl_buckets, 0, sizeof(lock_partitions[i].pl_buckets));
    }
    /* Note: name_buf storage is local; we pass NULL name since LWLockInit
     * stores the pointer, not a copy. Acceptable for internal partition locks. */
}

/* -------------------------------------------------------------------------
 * LockAcquire — main entry point
 * -------------------------------------------------------------------------
 */

LockAcquireResult LockAcquire(const LOCKTAG* locktag, int lockmode, bool dontWait)
{
    VbProc* proc = MyVbProc;
    assert(proc != NULL && "VbProcInit() not called");
    assert(lockmode >= 1 && lockmode <= MaxLockMode);

    u32 hashcode = locktag_hash(locktag);
    int partid = lock_partition_id(hashcode);
    LockPartition* part = &lock_partitions[partid];

    /* Step 1: Check LOCALLOCK table (fast path for re-entrant acquires) */
    LOCALLOCK* ll = find_locallock(locktag, lockmode, hashcode);
    if (ll != NULL && ll->nLocks > 0)
    {
        ll->nLocks++;
        return LOCKACQUIRE_ALREADY_HELD;
    }

    /* Step 2: Acquire partition LWLock */
    LWLockAcquire(&part->lock, LW_EXCLUSIVE);

    /* Step 3: Find or create LOCK object */
    LOCK* lock = find_or_create_lock(part, locktag, hashcode);

    /* Step 4: Find or create PROCLOCK */
    PROCLOCK* proclock = find_or_create_proclock(part, lock, proc, hashcode);

    /* Step 5: Increment requested counters */
    lock->requested[lockmode]++;
    lock->nRequested++;

    /* Step 6: Check conflicts */
    bool conflict = LockCheckConflicts(lock, lockmode, proc);

    if (!conflict)
    {
        /* Grant immediately */
        GrantLock(lock, proclock, lockmode);
        LWLockRelease(&part->lock);
    }
    else
    {
        if (dontWait)
        {
            /* Undo the requested increment */
            lock->requested[lockmode]--;
            lock->nRequested--;
            /* Clean up proclock if nothing held */
            if (proclock->holdMask == 0) remove_proclock(part, proclock);
            remove_lock_if_empty(part, lock, hashcode);
            LWLockRelease(&part->lock);
            return LOCKACQUIRE_NOT_AVAIL;
        }

        /*
         * Must wait.  WaitOnLock releases the partition lock before sleeping
         * and re-acquires it after wakeup.  On return we still hold the
         * partition lock.
         */
        WaitOnLock(part, lock, proclock, lockmode, hashcode);

        if (proc->deadlocked)
        {
            /* Undo requested increment (already done in WaitOnLock timeout path) */
            if (proclock->holdMask == 0) remove_proclock(part, proclock);
            remove_lock_if_empty(part, lock, hashcode);
            LWLockRelease(&part->lock);
            return LOCKACQUIRE_NOT_AVAIL;
        }

        LWLockRelease(&part->lock);
    }

    /* Step 7: Record in LOCALLOCK */
    if (ll == NULL) ll = create_locallock(locktag, lockmode, hashcode);
    ll->lockPtr = lock;
    ll->proclockPtr = proclock;
    ll->nLocks = 1;

    return LOCKACQUIRE_OK;
}

/* -------------------------------------------------------------------------
 * LockRelease — release one level of a lock
 * -------------------------------------------------------------------------
 */

bool LockRelease(const LOCKTAG* locktag, int lockmode)
{
    VbProc* proc = MyVbProc;
    assert(proc != NULL && "VbProcInit() not called");

    u32 hashcode = locktag_hash(locktag);
    int partid = lock_partition_id(hashcode);
    LockPartition* part = &lock_partitions[partid];

    /* Step 1: Check LOCALLOCK */
    LOCALLOCK* ll = find_locallock(locktag, lockmode, hashcode);
    if (ll == NULL || ll->nLocks == 0) return false;

    ll->nLocks--;
    if (ll->nLocks > 0) return true; /* still held recursively */

    /* Step 2: Acquire partition lock */
    LWLockAcquire(&part->lock, LW_EXCLUSIVE);

    LOCK* lock = ll->lockPtr;
    PROCLOCK* proclock = ll->proclockPtr;

    /* Step 3: Update lock and proclock */
    proclock->holdMask &= ~(1u << lockmode);
    lock->granted[lockmode]--;
    lock->nGranted--;
    if (lock->granted[lockmode] == 0) lock->grantMask &= ~(1u << lockmode);
    lock->requested[lockmode]--;
    lock->nRequested--;
    if (lock->requested[lockmode] == 0)
        lock->waitMask &= ~(1u << lockmode); /* safe: decrement to 0 means no waiters */

    /* Step 4: Wake compatible waiters */
    if (lock->waitHead != NULL) TryWakeupWaiters(lock);

    /* Step 5: Clean up PROCLOCK if no longer holds anything */
    if (proclock->holdMask == 0) remove_proclock(part, proclock);

    /* Step 6: Clean up LOCK if no longer needed */
    remove_lock_if_empty(part, lock, hashcode);

    LWLockRelease(&part->lock);

    /* Step 7: Remove LOCALLOCK */
    remove_locallock(ll);

    return true;
}

/* -------------------------------------------------------------------------
 * LockReleaseAll — release all locks held by this thread
 * -------------------------------------------------------------------------
 */

void LockReleaseAll(void)
{
    VbProc* proc = MyVbProc;
    if (proc == NULL) return;

    /* Walk all PROCLOCK entries for this proc and release them */
    PROCLOCK* pl = proc->proclockHead;
    while (pl != NULL)
    {
        PROCLOCK* next = pl->nextOnProc;

        LOCK* lock = pl->myLock;
        u32 hmask = pl->holdMask;
        u32 hashcode = locktag_hash(&lock->tag);
        int partid = lock_partition_id(hashcode);
        LockPartition* part = &lock_partitions[partid];

        LWLockAcquire(&part->lock, LW_EXCLUSIVE);

        for (int mode = 1; mode <= MaxLockMode; mode++)
        {
            if (hmask & (1u << mode))
            {
                lock->granted[mode]--;
                lock->nGranted--;
                if (lock->granted[mode] == 0) lock->grantMask &= ~(1u << mode);
                lock->requested[mode]--;
                lock->nRequested--;
            }
        }
        pl->holdMask = 0;

        if (lock->waitHead != NULL) TryWakeupWaiters(lock);

        remove_proclock(part, pl);
        remove_lock_if_empty(part, lock, hashcode);

        LWLockRelease(&part->lock);

        pl = next;
    }

    /* Clear LOCALLOCK table */
    for (int i = 0; i < LOCALLOCK_HASH_SIZE; i++)
    {
        LOCALLOCK* ll = locallock_table[i];
        while (ll != NULL)
        {
            LOCALLOCK* nll = ll->next;
            free(ll);
            ll = nll;
        }
        locallock_table[i] = NULL;
    }
}

/* -------------------------------------------------------------------------
 * DeadLockCheck — simple DFS to detect wait-for cycles
 *
 * Called with the relevant partition lock held (or all locks held by caller).
 * For simplicity in this implementation, we check the full wait-for graph.
 * -------------------------------------------------------------------------
 */

/* Visited set for DFS — use a bitmask over procno (MAX_VBPROCS <= 256) */
#define VISITED_WORDS ((MAX_VBPROCS + 63) / 64)
static __thread u64 deadlock_visited[VISITED_WORDS];

static bool deadlock_dfs(VbProc* start, VbProc* cur, int depth)
{
    if (depth > MAX_VBPROCS) return false; /* sanity limit */

    LOCK* wait_lock = cur->waitLock;
    if (wait_lock == NULL) return false; /* cur is not waiting */

    /* Walk all procs that hold any conflicting mode on wait_lock */
    int wmode = cur->waitLockMode;
    PROCLOCK* pl = wait_lock->proclockHead;
    while (pl != NULL)
    {
        VbProc* holder = pl->myProc;
        if (holder == cur)
        {
            pl = pl->nextOnLock;
            continue;
        }
        if (!(pl->holdMask & LockConflicts[wmode]))
        {
            pl = pl->nextOnLock;
            continue;
        }

        /* holder conflicts with cur */
        if (holder == start) return true; /* cycle detected */

        int word = holder->procno / 64;
        u64 bit = (u64)1 << (holder->procno % 64);
        if (deadlock_visited[word] & bit)
        {
            pl = pl->nextOnLock;
            continue; /* already visited */
        }
        deadlock_visited[word] |= bit;

        if (deadlock_dfs(start, holder, depth + 1)) return true;

        pl = pl->nextOnLock;
    }
    return false;
}

DeadLockState DeadLockCheck(VbProc* proc)
{
    memset(deadlock_visited, 0, sizeof(deadlock_visited));
    int word = proc->procno / 64;
    deadlock_visited[word] |= (u64)1 << (proc->procno % 64);

    if (deadlock_dfs(proc, proc, 0)) return DS_HARD_DEADLOCK;
    return DS_NO_DEADLOCK;
}

/* -------------------------------------------------------------------------
 * LockSubsystemInit — convenience: init all subsystems at once
 * -------------------------------------------------------------------------
 */

void LockSubsystemInit(void)
{
    InitLocks();
    VbProcInit();
}
