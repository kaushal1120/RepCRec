from lock import Lock
from lock_type import LockType


# Handle locks on data items for a given site.
class LockTable:
    def __init__(self):
        # lock table is hash table for all (variable, lock) pairs
        self.lock_table = dict()

    # Utility function to check if a variable is locked
    def is_locked(self, variable):
        if variable in self.lock_table and len(self.lock_table[variable].lock_owners) > 0:
            return True
        return False

    # Utility to check if transaction is read locked
    def is_read_locked(self, variable):
        if self.lock_table[variable].lock_type == LockType.RLOCK:
            return True
        return False

    # Fetch owner/s of a lock on a variable
    def get_lock_owners(self, variable):
        return self.lock_table[variable].lock_owners

    # Obtain lock of lock_type for transaction_id on variable
    def lock(self, transaction_id, variable, lock_type):
        if variable not in self.lock_table:
            lock = Lock(lock_type)
        else:
            lock = self.lock_table[variable]
        if transaction_id in lock.lock_owners and lock_type == lock.lock_type:
            return
        if lock.lock_type == LockType.RLOCK or (lock.lock_type == LockType.WLOCK and len(lock.lock_owners) == 0):
            lock.lock_owners.append(transaction_id)
        self.lock_table[variable] = lock

    # Obtain a shared read lock for transaction_id on variable
    def share_lock(self, transaction_id, variable):
        if self.lock_table[variable].lock_type == LockType.RLOCK:
            if transaction_id not in self.lock_table[variable].lock_owners:
                self.lock_table[variable].lock_owners.append(transaction_id)

    # Promote a read lock to write lock if possible.
    def promote_lock(self, variable, transaction_id):
        if self.lock_table[variable]:
            if self.lock_table[variable].lock_type == LockType.RLOCK:
                if len(self.lock_table[variable].lock_owners) == 1:
                    if transaction_id in self.lock_table[variable].lock_owners:
                        self.lock_table[variable].lock_type = LockType.WLOCK

    # Release all locks held by transaction_id
    def release_locks_by_transaction(self, transaction_id):
        for variable in self.lock_table.copy():
            new_owner_list = []
            for trans_id in self.lock_table[variable].lock_owners:
                if trans_id != transaction_id:
                    new_owner_list.append(trans_id)
            self.lock_table[variable].lock_owners = new_owner_list
            # Remove lock for table if no longer useful
            if len(self.lock_table[variable].lock_owners) == 0 and len(self.lock_table[variable].lock_queue) == 0:
                del self.lock_table[variable]

    # Add tuple of (transaction_id, lock_type) to waiting queue for the lock on variable
    def lock_enqueue(self, transaction_id, variable, lock_type):
        lock_already_enqueued = False
        for lock in self.lock_table[variable].lock_queue:
            if lock[0] == transaction_id and (lock_type == LockType.RLOCK or lock[1] == lock_type):
                lock_already_enqueued = True
                break
        if not lock_already_enqueued:
            self.lock_table[variable].lock_queue.append((transaction_id, lock_type))

    # Assumed this function is called only when the a transaction commits or aborts to dequeue locks
    # that may have been waiting for a lock before the transaction holding that lock was committed or
    # aborted
    def dequeue_waiting_locks(self):
        for variable in self.lock_table:
            if len(self.lock_table[variable].lock_queue) > 0:
                # Something to dequeue
                if len(self.lock_table[variable].lock_owners) == 0:
                    # If lock_owners is empty enable first lock waiting in queue
                    self.lock_table[variable].lock_type = self.lock_table[variable].lock_queue[0][1]
                    self.lock_table[variable].lock_owners.append(
                        self.lock_table[variable].lock_queue[0][0])
                    self.lock_table[variable].lock_queue.pop(0)
                if self.lock_table[variable].lock_type == LockType.RLOCK:
                    # If dequeued lock is a read lock, dequeue all read locks until lock_queue is empty or write lock is encountered
                    while len(self.lock_table[variable].lock_queue) > 0 and self.lock_table[variable].lock_queue[0][1] == LockType.RLOCK:
                        self.lock_table[variable].lock_owners.append(
                            self.lock_table[variable].lock_queue[0][0])
                        self.lock_table[variable].lock_queue.pop(0)

                    if len(self.lock_table[variable].lock_owners) == 1:
                        if len(self.lock_table[variable].lock_queue) > 0:
                            if self.lock_table[variable].lock_queue[0][1] == LockType.WLOCK:
                                if self.lock_table[variable].lock_queue[0][0] == self.lock_table[variable].lock_owners[0]:
                                    # If dequeued lock is a read lock and next lock in queue is write lock for same transaction, we can promote it
                                    self.promote_lock(
                                        variable, self.lock_table[variable].lock_owners[0])
                                    self.lock_table[variable].lock_queue.pop(0)

    # Check if theres transaction waiting for a write lock on variable
    def has_waiting_write_lock(self, variable, transaction_id=None):
        for lock in self.lock_table[variable].lock_queue:
            if lock[1] == LockType.WLOCK and transaction_id != lock[0]:
                return True
        return False

    # Unlocks for transaction_id
    def unlock_tid_queue(self, transaction_id):
        for variable in self.lock_table.keys():
            new_queue = []
            for lock in self.lock_table[variable].lock_queue:
                if lock[0] != transaction_id:
                    new_queue.append(lock)
            self.lock_table[variable].lock_queue = new_queue

    # Generates a dependency for graph for deadlock detection
    def get_dep_graph(self):
        waits_for_graph = dict()

        # Generate lock graph for the locks in queue with each other.
        for variable in self.lock_table.keys():
            lock = self.lock_table[variable]
            for i in range(len(lock.lock_queue)):
                xlock = lock.lock_queue[i]
                for ylock in lock.lock_queue[0:i]:
                    if xlock[1] == LockType.WLOCK or ylock[1] == LockType.WLOCK:
                        waits_for_graph.setdefault(xlock[0], set()).add(ylock[0])

        # Generate lock graph for the lock_owners with the locks in queue.
        for variable in self.lock_table.keys():
            lock = self.lock_table[variable]
            for conflicting_lock in lock.lock_queue:
                if lock.lock_type == LockType.WLOCK or conflicting_lock[1] == LockType.WLOCK:
                    if conflicting_lock[1] == LockType.WLOCK:
                        for shared_owner in lock.lock_owners:
                            if shared_owner != conflicting_lock[0]:
                                waits_for_graph.setdefault(
                                    conflicting_lock[0], set()).add(shared_owner)
                    else:
                        waits_for_graph.setdefault(
                            conflicting_lock[0], set()).add(lock.lock_owners[0])

        return waits_for_graph

    # Clear the current lock table
    def clear(self):
        self.lock_table.clear()
