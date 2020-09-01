package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of *LockContext* to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * *Each resource the lock manager manages has its own queue of LockRequest objects*
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // TODO(hw4_part1): You may add helper methods here if you wish
        public Lock getLock(TransactionContext transaction) {
            long transNum = transaction.getTransNum();
            for (Lock l : locks) {
                if (l.transactionNum.equals(transNum)) {
                    return l;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(hw4_part1): You may add helper methods here if you wish
    private boolean isCompatible(TransactionContext transaction, ResourceName name, LockType lockType) {
        List<Lock> currLocks = getLocks(name);
        for (Lock e : currLocks) {
            if (e.transactionNum != transaction.getTransNum() && !LockType.compatible(e.lockType, lockType)) {
                return false;
            }
        }
        return true;
    }

    public synchronized Lock getLock(TransactionContext transaction, ResourceName name) {
        List<Lock> lockList = getLocks(transaction);
        for (Lock l : lockList) {
            if (l.name.equals(name)) {
                return l;
            }
        }
        return null;
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic faction.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean block = false;

        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, lock);
            long transactionNum = transaction.getTransNum();

            // Avoid duplicate request
            if (getLocks(transaction).contains(lock)) {
                throw new DuplicateLockRequestException("Duplicate Lock requested!");
            }

            if (!isCompatible(transaction, name, lockType)) {
                // If not compatible
                transaction.prepareBlock();
                block = true;
                entry.waitingQueue.addFirst(request);
            } else {
                // If compatible, acquire the request
                if (!transactionLocks.containsKey(transactionNum)) {
                    transactionLocks.put(transactionNum, new ArrayList<>());
                }
                transactionLocks.get(transactionNum).add(lock);
                if (!resourceEntries.containsKey(name)) {
                    resourceEntries.put(name, new ResourceEntry());
                }
                resourceEntries.get(name).locks.add(lock);
            }
        }

        if (block) {
            transaction.block();
        }

        synchronized (this) {
            // Release phase:
            // Check if the transaction has the lock on the releasing resource
            for (ResourceName n : releaseLocks) {
                Lock currLock = getLock(transaction, n);
                if (currLock == null) {
                    throw new NoLockHeldException("No such Lock held!");
                } else {
                    // Release the lock
                    transactionLocks.get(transaction.getTransNum()).remove(currLock);
                    resourceEntries.get(n).locks.remove(currLock);
                    transaction.unblock();

                    // Fetch a next lock in queue
                    Deque<LockRequest> queue = resourceEntries.get(n).waitingQueue;
                    while (!queue.isEmpty()) {
                        LockRequest re = queue.peekFirst();
                        TransactionContext tc = re.transaction;
                        Lock l = re.lock;
                        ResourceName rn = l.name;
                        LockType t = l.lockType;
                        long trans = l.transactionNum;
                        if (!isCompatible(transaction, rn, t)) {
                            break;
                        } else {
                            queue.pollFirst();
                            tc.unblock();
                            transactionLocks.put(trans, new ArrayList<>());
                            transactionLocks.get(trans).add(l);
                            if (!resourceEntries.containsKey(name)) {
                                resourceEntries.put(name, new ResourceEntry());
                            }
                            resourceEntries.get(name).locks.add(l);
                        }
                    }
                }
            }
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean block = false;

        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest request = new LockRequest(transaction, lock);
            long transactionNum = transaction.getTransNum();

            // Avoid duplicate request
            if (getLocks(transaction).contains(lock)) {
                throw new DuplicateLockRequestException("Duplicate Lock requested!");
            }

            if (!isCompatible(transaction, name, lockType) || !getResourceEntry(name).waitingQueue.isEmpty()) {
                // If not compatible
                transaction.prepareBlock();
                block = true;
                entry.waitingQueue.addLast(request);
            } else {
                // If compatible, acquire the request
                if (!transactionLocks.containsKey(transactionNum)) {
                    transactionLocks.put(transactionNum, new ArrayList<>());
                }
                transactionLocks.get(transactionNum).add(lock);
                if (!resourceEntries.containsKey(name)) {
                    resourceEntries.put(name, new ResourceEntry());
                }
                resourceEntries.get(name).locks.add(lock);
            }
        }

        if (block) {
            transaction.block();
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            Lock releaseLock = getLock(transaction, name);
            long transactionNum = transaction.getTransNum();

            if (releaseLock == null) {
                throw new NoLockHeldException("No such Lock held!");

            } else {
                // Release the lock
                transactionLocks.get(transactionNum).remove(releaseLock);
                getResourceEntry(name).locks.remove(releaseLock);
                if (transactionLocks.get(transactionNum).size() == 0) {
                    transactionLocks.remove(transactionNum);
                }
                if (getResourceEntry(name).waitingQueue.size() == 0 && getResourceEntry(name).locks.size() == 0) {
                    resourceEntries.remove(name);
                }
                transaction.unblock();

                // Fetch a next lock in queue
                Deque<LockRequest> queue = getResourceEntry(name).waitingQueue;
                while (!queue.isEmpty()) {
                    LockRequest re = queue.peekFirst();
                    TransactionContext tc = re.transaction;
                    Lock l = re.lock;
                    ResourceName rn = l.name;
                    LockType t = l.lockType;
                    long trans = l.transactionNum;
                    if (!isCompatible(tc, rn, t)) {
                        break;
                    } else {
                        queue.removeFirst();
                        tc.unblock();

                        for (Lock lock : resourceEntries.get(rn).locks) {
                            if (lock.transactionNum == trans) {
                                promote(tc, rn, t);
                                return;
                            }
                        }

                        if (!transactionLocks.containsKey(trans)) {
                            transactionLocks.put(trans, new ArrayList<>());
                        }
                        transactionLocks.get(trans).add(l);

                        if (!resourceEntries.containsKey(name)) {
                            resourceEntries.put(name, new ResourceEntry());
                        }
                        resourceEntries.get(name).locks.add(l);
                    }
                }
            }
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(hw4_part1): implement
        // You may modify any part of this method.
        boolean block = false;

        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            long transactionNum = transaction.getTransNum();
            Lock lock = new Lock(name, newLockType, transactionNum);
            LockRequest request = new LockRequest(transaction, lock);

            // Avoid duplicate request
            if (getLocks(transaction).contains(lock)) {
                throw new DuplicateLockRequestException("Duplicate Lock requested!");
            }

            Lock oldLock = getResourceEntry(name).getLock(transaction);
            if (oldLock == null) {
                throw new NoLockHeldException("No such Lock held!");
            }
            if (!LockType.substitutable(newLockType, oldLock.lockType)) {
                throw new InvalidLockException("Invalid substitution!");
            }

            if (!isCompatible(transaction, name, newLockType)) {
                // If not compatible
                transaction.prepareBlock();
                block = true;
                entry.waitingQueue.addFirst(request);
            } else {
                // If compatible, acquire the request
//                release(transaction, name);
                if (!transactionLocks.containsKey(transactionNum)) {
                    transactionLocks.put(transactionNum, new ArrayList<>());
                }

                int lockIndex = 0;
                List<Lock> locks = transactionLocks.get(transactionNum);
                for (Lock l : locks) {
                    if (l.name == name) {
                        locks.remove(l);
                        resourceEntries.get(name).locks.remove(l);
                        locks.add(lockIndex, lock);
                        break;
                    }
                    lockIndex++;
                }
//                transactionLocks.get(transactionNum).add(lock);

                if (!resourceEntries.containsKey(name)) {
                    resourceEntries.put(name, new ResourceEntry());
                }
                resourceEntries.get(name).locks.add(lock);
            }
        }

        if (block) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(hw4_part1): implement
        Lock lock = getResourceEntry(name).getLock(transaction);
        if (lock != null) {
            return lock.lockType;
        } else {
            return LockType.NL;
        }
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
