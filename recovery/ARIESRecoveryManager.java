package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.LRUEvictionPolicy;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(hw5): implement

        TransactionTableEntry transactionEntry = this.transactionTable.get(transNum);

        long lastLSN = transactionEntry.lastLSN;
        LogRecord logRecord = new CommitTransactionLogRecord(transNum, lastLSN);
        long lsn = this.logManager.appendToLog(logRecord);
        transactionEntry.lastLSN = lsn;

        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);

        this.logManager.flushToLSN(logRecord.LSN);
        return lsn;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(hw5): implement
        long lastLSN;
        TransactionTableEntry transactionEntry = this.transactionTable.get(transNum);

        lastLSN = transactionEntry.lastLSN;
        LogRecord logRecord = new AbortTransactionLogRecord(transNum, lastLSN);
        long lsn = this.logManager.appendToLog(logRecord);
        transactionEntry.lastLSN = lsn;

        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);

        return lsn;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(hw5): implement
        long lastLSN;
        TransactionTableEntry transactionEntry = this.transactionTable.get(transNum);

        lastLSN = transactionEntry.lastLSN;
        LogRecord logRecord = new EndTransactionLogRecord(transNum, lastLSN);
        long lsn = this.logManager.appendToLog(logRecord);
        transactionEntry.lastLSN = lsn;

        if (logManager.fetchLogRecord(lastLSN).type == LogType.ABORT_TRANSACTION) {
            // get the CLR of the record if it is undoable.
//            while (lastLSN != 0) {
//                LogRecord lastRecord = logManager.fetchLogRecord(lastLSN);
//                if (lastRecord.isRedoable()) {
//                    LogRecord clr = logRecord.undo(lastLSN).getFirst();
//                    clr.redo(this.diskSpaceManager, this.bufferManager);
//                }
//                lastLSN = lastRecord.getPrevLSN().isPresent() ? lastRecord.getPrevLSN().get() : 0;
//            }
            rollback(lastLSN, 0);
        }

        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        this.transactionTable.remove(transNum);

        return lsn;
    }

    // TODO(hw5): add any helper methods needed
    public void rollback(long lsn, long targetLSN) {
        long lastLSN = lsn;

        while (lastLSN != targetLSN) {
            LogRecord lastRecord = logManager.fetchLogRecord(lastLSN);
            if (lastRecord.isRedoable()) {
                LogRecord clr = lastRecord.undo(lastLSN).getFirst();
                logManager.appendToLog(clr);
                clr.redo(this.diskSpaceManager, this.bufferManager);
            }
//            if (lastRecord.getUndoNextLSN().isPresent()) {
//                lastLSN = lastRecord.getUndoNextLSN().get();
//            } else {
//                lastLSN = lastRecord.getPrevLSN().isPresent() ? lastRecord.getPrevLSN().get() : 0;
//            }
            lastLSN = lastRecord.getPrevLSN().isPresent() ? lastRecord.getPrevLSN().get() : 0;
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(hw5): implement
        long lastLSN = this.transactionTable.get(transNum).lastLSN;
        TransactionTableEntry transactionEntry = this.transactionTable.get(transNum);

        if (before.length >= BufferManager.EFFECTIVE_PAGE_SIZE / 2) {
            LogRecord undoRecord = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, new byte[]{});
            LogRecord redoRecord = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, new byte[]{}, after);

            this.logManager.appendToLog(undoRecord);
            this.logManager.appendToLog(redoRecord);
            transactionEntry.lastLSN = redoRecord.LSN;
            transactionEntry.touchedPages.add(pageNum);
            if (!dirtyPageTable.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, undoRecord.LSN);
            }

            return redoRecord.LSN;

        } else {
            LogRecord logRecord = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, after);

            // Update the Xact Table and DPT
            this.logManager.appendToLog(logRecord);
            transactionEntry.lastLSN = logRecord.LSN;
            transactionEntry.touchedPages.add(pageNum);
            if (!dirtyPageTable.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, logRecord.LSN);
            }

            return logRecord.LSN;
        }
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        dirtyPageTable.remove(pageNum);

        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(hw5): implement
        rollback(transactionEntry.lastLSN, LSN);

    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(hw5): generate end checkpoint record(s) for DPT and transaction table

        // Check the DPT
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            long transNum = entry.getKey();
            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size() + 1, txnTable.size(), touchedPages.size(), numTouchedPages);
            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
            }

            dpt.put(transNum, entry.getValue());
        }

        // Check the Txn Table
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            boolean fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                    dpt.size(), txnTable.size() + 1, touchedPages.size(), numTouchedPages);
            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                txnTable.clear();
            }

            Pair<Transaction.Status, Long> pair = new Pair<>(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN);
            txnTable.put(transNum, pair);
        }

        // Check the touchedPage map
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(hw5): add any helper methods needed

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(hw5): implement
        // Analysis phase
        restartAnalysis();

        // Redo phase
        restartRedo();

        // Remove pages that are not actually dirty from the DPT
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()) {
            long pageNum = entry.getKey();
            long recLSN = entry.getValue();

            Page page = bufferManager.fetchPage(getPageLockContext(pageNum), pageNum, false);

            try {
                class Foo implements BiConsumer<Long, Boolean> {
                    @Override
                    public void accept(Long aLong, Boolean aBoolean) {
                        if (aLong == pageNum && !aBoolean) {
                            dirtyPageTable.remove(aLong);
                        }
                    }
                }
                BiConsumer<Long, Boolean> f = new Foo();
                bufferManager.iterPageNums(f);
                long pageLSN = page.getPageLSN();
                if (recLSN < pageLSN) {
                    dirtyPageTable.remove(pageNum);
                }
            } finally {
                page.unpin();
            }

        }

        // Return a Runnable that performs the undo phase and checkpoints, instead of performing those actions immediately
        return () -> {
            restartUndo();
            checkpoint();
        };
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(hw5): implement
        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        while (iter.hasNext()) {
            LogRecord logRecord = iter.next();
            long transNum = logRecord.getTransNum().isPresent()? logRecord.getTransNum().get() : 0;

            // If the log record is for a change in transaction status
            if (logRecord.type == LogType.COMMIT_TRANSACTION || logRecord.type == LogType.ABORT_TRANSACTION ||
                    logRecord.type == LogType.END_TRANSACTION) {
                // Update the Txn Table
                    // If the transaction is not in the transaction table, it should be added to the table
                if (!transactionTable.containsKey(transNum)) {
                    Transaction newTxn = newTransaction.apply(transNum);
                    transactionTable.put(transNum, new TransactionTableEntry(newTxn));
                }
                    // The lastLSN of the transaction should be updated
                TransactionTableEntry entry = transactionTable.get(transNum);
                Transaction transaction = entry.transaction;
                entry.lastLSN = logRecord.getLSN();

                // Set the status of the Txn
                if (logRecord.type == LogType.COMMIT_TRANSACTION) {
                    transaction.setStatus(Transaction.Status.COMMITTING);

                } else if (logRecord.type == LogType.ABORT_TRANSACTION) {
                    transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);

                } else if (logRecord.type == LogType.END_TRANSACTION) {
                    transaction.cleanup();
                    transactionTable.remove(transNum);
                    transaction.setStatus(Transaction.Status.COMPLETE);
                }
            }

            // If the log record is a begin_checkpoint record
            else if (logRecord.type == LogType.BEGIN_CHECKPOINT) {
                // updateTransactionCounter
                updateTransactionCounter.accept(logRecord.getMaxTransactionNum().get());
            }

            // If the log record is an end_checkpoint record
            // the tables stored in the record should be combined with the tables currently in memory
            else if (logRecord.type == LogType.END_CHECKPOINT) {

                Map<Long, Long> dpt = logRecord.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> txnTable = logRecord.getTransactionTable();
                Map<Long, List<Long>> touchedPages = logRecord.getTransactionTouchedPages();

                // recLSN of a page in the checkpoint always be used

                // Copy all entries of checkpoint DPT (replace existing entries if any)
                for (Map.Entry<Long, Long> entry : dpt.entrySet()) {
                    long txnKey = entry.getKey();
                    long lsnVal = entry.getValue();
                    if (dirtyPageTable.containsKey(txnKey)) {
                        dirtyPageTable.replace(txnKey, lsnVal);
                    } else {
                        dirtyPageTable.put(txnKey, lsnVal);
                    }
                }

                // Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's,
                // add to transaction table if not already present.
                // <transNum, Pair<status, lastLSN>>
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : txnTable.entrySet()) {
                    Long txnNum = entry.getKey();
                    Transaction.Status status = entry.getValue().getFirst();
                    long lastLSN = entry.getValue().getSecond();

                    if (!transactionTable.containsKey(txnNum)) {
                        Transaction newTxn = newTransaction.apply(txnNum);
                        transactionTable.put(txnNum, new TransactionTableEntry(newTxn));
                    }

                    Transaction txn = transactionTable.get(txnNum).transaction;
                    txn.setStatus(status);

                    TransactionTableEntry e = transactionTable.get(txnNum);
                    e.lastLSN = Math.max(lastLSN, transactionTable.get(txnNum).lastLSN);
                }

                // Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
                // transaction table if the transaction has not finished yet, and acquire X locks.
                for (Map.Entry<Long, List<Long>> entry : touchedPages.entrySet()) {
                    long txnNum = entry.getKey();
                    if (transactionTable.containsKey(transNum)) {
                        TransactionTableEntry e = transactionTable.get(txnNum);
                        Transaction txn = e.transaction;
                        e.touchedPages.addAll(entry.getValue());
                        for (Long pageNum : entry.getValue()) {
                            acquireTransactionLock(txn, getPageLockContext(pageNum), LockType.X);
                        }
                    }
                }
            }

            // If the log record is for a transaction operation
            else {
                // Update the Txn Table
                    // If the transaction is not in the transaction table, it should be added to the table
                if (!transactionTable.containsKey(transNum)) {
                    Transaction newTxn = newTransaction.apply(transNum);
                    transactionTable.put(transNum, new TransactionTableEntry(newTxn));
                }
                    // The lastLSN of the transaction should be updated
                TransactionTableEntry entry = transactionTable.get(transNum);
                Transaction transaction = entry.transaction;
                entry.lastLSN = logRecord.getLSN();

                    // If the log record is about a page, the page needs to be added to the touchedPages set in the
                    // transaction table entry and the transaction needs to request an X lock on it.
                if (isPageRelated(logRecord.type)) {
                    long pageNum = logRecord.getPageNum().get();
                    entry.touchedPages.add(pageNum);

                    LockContext lc = getPageLockContext(pageNum);
                    acquireTransactionLock(transaction, lc, LockType.X);

                    // The dirty page table needs to be updated accordingly when one of these page-related log records are encountered.
                    // UpdatePage/UndoUpdatePage
                    if (logRecord.type == LogType.UPDATE_PAGE || logRecord.type == LogType.UNDO_UPDATE_PAGE) {
                        dirtyPageTable.put(pageNum, logRecord.LSN);
                    } else {
                        dirtyPageTable.remove(pageNum);
                    }
                }
            }


        }

        // cleanup and end transactions that are in the COMMITING state, and
        // move all transactions in the RUNNING state to RECOVERY_ABORTING.
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            TransactionTableEntry e = entry.getValue();
            Transaction txn = entry.getValue().transaction;
            long txnNum = entry.getKey();
            if (txn.getStatus() == Transaction.Status.COMMITTING) {
                txn.cleanup();
                txn.setStatus(Transaction.Status.COMPLETE);
                LogRecord endRecord = new EndTransactionLogRecord(txnNum, e.lastLSN);
                logManager.appendToLog(endRecord);
                transactionTable.remove(entry.getKey());
            } else if (txn.getStatus() == Transaction.Status.RUNNING) {
                LogRecord abortRecord = new AbortTransactionLogRecord(txnNum, e.lastLSN);
                this.logManager.appendToLog(abortRecord);
                txn.setStatus(Transaction.Status.RECOVERY_ABORTING);
                e.lastLSN = abortRecord.LSN;
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(hw5): implement'
        long startPoint = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> iter = logManager.scanFrom(startPoint);

        while (iter.hasNext()) {
            LogRecord logRecord = iter.next();
            if (logRecord.isRedoable()) {
                if (isPageRelated(logRecord.type)) {
                    long pageNum = logRecord.getPageNum().get();
                    Page page = bufferManager.fetchPage(getPageLockContext(pageNum), pageNum, false);
                    try {
                        if (dirtyPageTable.containsKey(pageNum) && logRecord.LSN >= dirtyPageTable.get(pageNum)
                                && page.getPageLSN() < logRecord.LSN) {
                            logRecord.redo(diskSpaceManager, bufferManager);
                        }
                    } finally {
                        page.unpin();
                    }
                } else if (isPartitionRelated(logRecord.type)) {
                    logRecord.redo(diskSpaceManager, bufferManager);
                }
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(hw5): implement
        // First, a priority queue is created sorted on lastLSN of all aborting transactions.
        PriorityQueue<Pair<Long, TransactionTableEntry>> pq = new PriorityQueue<>(
                (p1, p2) -> Long.compare(p2.getFirst(), p1.getFirst()));

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            TransactionTableEntry e = entry.getValue();
            if (e.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                Pair<Long, TransactionTableEntry> p = new Pair(e.lastLSN, e);
                pq.add(p);
            }
        }

        // Then, always working on the largest LSN in the priority queue until we are done,
        //    * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
        //    * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
        //    *   (or prevLSN if none) of the record; and
        //    * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
        while (!pq.isEmpty()) {
            Pair<Long, TransactionTableEntry> pair = pq.poll();
            TransactionTableEntry entry = pair.getSecond();
            long transNum = entry.transaction.getTransNum();
            long lastLSN = pair.getFirst();
//            long lsn = transactionTable.get(transNum).lastLSN;

//            rollback(lsn, 0);
            LogRecord logRecord = logManager.fetchLogRecord(lastLSN);
            long prevLSN = transactionTable.get(transNum).lastLSN;
            if (logRecord.isUndoable()) {
                Pair<LogRecord, Boolean> undo = logRecord.undo(prevLSN);
                LogRecord clr = undo.getFirst();
                logManager.appendToLog(clr);
//                    if (undo.getSecond()) {
//                        logManager.flushToLSN(clr.LSN);
//                    }
                // Update the DPT if necessary
                if (clr.getPageNum().isPresent() && !dirtyPageTable.containsKey(clr.getPageNum().get())) {
                    dirtyPageTable.put(clr.getPageNum().get(), clr.LSN);
                }
//                if (logRecord.getPageNum().isPresent() && dirtyPageTable.containsKey(logRecord.getPageNum().get())
//                        && lastLSN == dirtyPageTable.get(logRecord.getPageNum().get())) {
//                    dirtyPageTable.remove(logRecord.getPageNum().get());
//                }
                entry.lastLSN = clr.LSN;
                clr.redo(this.diskSpaceManager, this.bufferManager);
                logRecord= clr;
            }

            if (logRecord.getUndoNextLSN().isPresent()) {
                lastLSN = logRecord.getUndoNextLSN().get();
            } else {
                lastLSN = logRecord.getPrevLSN().isPresent()? logRecord.getPrevLSN().get() : 0;
            }
            if (lastLSN != 0) {
                Pair<Long, TransactionTableEntry> p = new Pair<>(lastLSN, entry);
                pq.add(p);
            }

//            while (lsn != 0) {
//                // if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
//                if (logRecord.isUndoable()) {
//                    LogRecord clr = logRecord.undo(lastLSN).getFirst();
//                    logManager.appendToLog(clr);
//                    entry.lastLSN = clr.LSN;
//                    clr.redo(diskSpaceManager, bufferManager);
//                    if (lastLSN == dirtyPageTable.get(transNum)) {
//                        dirtyPageTable.remove(transNum);
//                    }
//                    logRecord = clr;
//                }
//
//                // replace the entry in the set should be replaced with a new one, using the undoNextLSN
//                //    *   (or prevLSN if none) of the record; and
//                if (logRecord.getUndoNextLSN().isPresent()) {
//                    lsn = logRecord.getUndoNextLSN().get();
//                } else {
//                    lsn = logRecord.getPrevLSN().get();
//                }
//            }

            // end the transaction if the LSN from the previous step is 0, removing it from the set and the transaction table.
            if (lastLSN == 0) {
                LogRecord endRecord = new EndTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN);
                logManager.appendToLog(endRecord);
                entry.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transNum);
            }
        }
    }

    // TODO(hw5): add any helper methods needed
    boolean isPageRelated(LogType type) {
        return type == LogType.UPDATE_PAGE || type == LogType.UNDO_UPDATE_PAGE ||
                type == LogType.ALLOC_PAGE || type == LogType.FREE_PAGE ||
                type == LogType.UNDO_ALLOC_PAGE || type == LogType.UNDO_FREE_PAGE;
    }

    boolean isPartitionRelated(LogType type) {
        return type == LogType.ALLOC_PART || type == LogType.FREE_PART ||
                type == LogType.UNDO_ALLOC_PART || type == LogType.UNDO_FREE_PART;
    }

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
