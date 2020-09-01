package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.table.Table;

import java.nio.file.LinkOption;
import java.util.ArrayList;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(hw4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        LockType currType = lockContext.getEffectiveLockType(transaction);


        LockContext parentContext = lockContext.parentContext();

        if (parentContext != null && parentContext.isTable && parentContext.autoEscalate) {
            double pageLock = parentContext.saturation(transaction);
            int pageNum = parentContext.capacity();
            if (pageNum >= 10 && pageLock >= 0.2) {
                parentContext.escalate(transaction);
//                return;
            }
        }

        // If the current transaction is null, do nothing
        if (transaction == null || lockType == LockType.NL || currType == lockType || lockContext.readonly) {
            return;
        }

        // If the call is to check the ancestors.
        if (lockType == LockType.IS || lockType == LockType.IX) {
            if (parentContext != null) {
                ensureSufficientLockHeld(parentContext, lockType);
            }
            if (LockType.substitutable(lockType, currType)) {
                if (currType == LockType.NL) {
                    lockContext.acquire(transaction, lockType);
                } else {
                    lockContext.promote(transaction, lockType);
//                    List<ResourceName> list = new ArrayList<>();
//                    list.add(lockContext.getResourceName());
//                    lockContext.lockman.acquireAndRelease(transaction, lockContext.getResourceName(), lockType, list);
                }
            }
            if (lockType == LockType.IX && currType == LockType.S) {
                lockContext.promote(transaction, LockType.SIX);
            }
            return;
        }

        // If the call is an initial call.
        if (parentContext != null) {
            if (lockType == LockType.S) {
                ensureSufficientLockHeld(parentContext, LockType.IS);
            } else if (lockType == LockType.X) {
                ensureSufficientLockHeld(parentContext, LockType.IX);
            }
        }

        if (parentContext != null && !LockType.canBeParentLock(parentContext.getExplicitLockType(transaction), lockType)) {
            return;
        }


        if (currType == LockType.NL || lockContext.getExplicitLockType(transaction) == LockType.NL) {
            lockContext.acquire(transaction, lockType);
            return;

        } else if (currType == LockType.X) {
            return;

        } else if (LockType.substitutable(lockType, currType)) {
            lockContext.promote(transaction, lockType);

        } else if ((currType == LockType.IX && lockType == LockType.S)) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        currType = lockContext.getEffectiveLockType(transaction);

        if (currType != LockType.NL) {
            lockContext.escalate(transaction);
        }
    }

    // TODO(hw4_part2): add helper methods as you see fit
}
