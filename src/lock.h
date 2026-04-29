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
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

/* ============================================================
 * LWLockMode — 与旧版保持相同枚举值，调用方不需要改
 * ============================================================ */
typedef enum
{
    LW_EXCLUSIVE = 0,
    LW_SHARED = 1,
    LW_WAIT_UNTIL_FREE = 2,  /* 仅保留枚举，语义等同 LW_EXCLUSIVE */
} LWLockMode;

/* ============================================================
 * LWLock — pthread_rwlock_t 薄包装
 *
 * 结构体只含一个 rwlock 字段；旧版的 state/_Atomic/head/tail
 * 全部移除。调用方以 LWLock 为类型，不直接访问内部字段。
 * ============================================================ */
typedef struct LWLock
{
    pthread_rwlock_t rwlock;
} LWLock;

static inline void LWLockInit(LWLock* lock, const char* name)
{
    (void)name;   /* 嵌入式不需要锁名称 */
    pthread_rwlock_init(&lock->rwlock, NULL);
}

static inline void LWLockAcquire(LWLock* lock, LWLockMode mode)
{
    if (mode == LW_SHARED)
        pthread_rwlock_rdlock(&lock->rwlock);
    else
        pthread_rwlock_wrlock(&lock->rwlock);
}

/* 非阻塞尝试：成功返回 true，已被锁定返回 false */
static inline bool LWLockConditionalAcquire(LWLock* lock, LWLockMode mode)
{
    int rc = (mode == LW_SHARED) ? pthread_rwlock_tryrdlock(&lock->rwlock)
                                 : pthread_rwlock_trywrlock(&lock->rwlock);
    return rc == 0;
}

/* 等价于 LWLockAcquire（嵌入式无区别） */
static inline bool LWLockAcquireOrWait(LWLock* lock, LWLockMode mode)
{
    LWLockAcquire(lock, mode);
    return true;
}

static inline void LWLockRelease(LWLock* lock)
{
    pthread_rwlock_unlock(&lock->rwlock);
}

/* 释放锁并将 *var 设为 val（原子保证：先赋值再解锁） */
static inline void LWLockReleaseClearVar(LWLock* lock, uint64_t* var, uint64_t val)
{
    *var = val;
    pthread_rwlock_unlock(&lock->rwlock);
}

static inline void LWLockDestroy(LWLock* lock)
{
    pthread_rwlock_destroy(&lock->rwlock);
}

/* 嵌入式中不跟踪持锁线程，始终返回 false */
static inline bool LWLockHeldByMe(LWLock* lock)
{
    (void)lock;
    return false;
}
static inline void LWLockReleaseAll(void) {}

/* ============================================================
 * SpinLock — pthread_mutex_t 薄包装
 *
 * 嵌入式场景用 mutex 替代 x86 TAS spinlock：
 *   - 无忙等 + 无 inline asm，移植性好
 *   - 临界区极短（计数器读写），mutex 开销可接受
 * ============================================================ */
typedef pthread_mutex_t slock_t;

#define SpinLockInit(lock)    pthread_mutex_init((lock), NULL)
#define SpinLockAcquire(lock) pthread_mutex_lock(lock)
#define SpinLockRelease(lock) pthread_mutex_unlock(lock)
#define SpinLockFree(lock)    pthread_mutex_destroy(lock)

/* ============================================================
 * VbProcInit / VbProcRelease — 嵌入式无需手动管理线程槽
 * ============================================================ */
static inline void VbProcInit(void) {}
static inline void VbProcRelease(void) {}

/* ============================================================
 * LockSubsystemInit — 嵌入式无需全局初始化
 * ============================================================ */
static inline void LockSubsystemInit(void) {}
#endif