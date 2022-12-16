from lock import Lock
from lock_type import LockType
from locktable import LockTable
from misc import *
from op import *
from transaction import Transaction, TransType


# Represents a site
class SiteManager:
    def __init__(self, site_id):

        # maintains a dictionary for locks on all variables
        self.lock_table = LockTable()
        # list of tuples of (up_timestamp, down_timestamp) for site
        self.up_times = []
        # start timestamp of current up_time
        self.start_time = 0
        # traks wheather can commit at commit time
        self.can_commit = True
        # site id
        self.site_id = site_id
        # site is up or down
        self.up = True
        # temporary workspace before committing. values are list of tuples of (value, transaction_id)
        self.tempData = dict()
        # committed data stored in the site. values are list of tuples of (committed_value, commit_time)
        self.data = dict()

        self._initialize_database()

    # initialize variables and data
    def _initialize_database(self):
        current_timestamp = 0
        for i in range(0, 20):
            if (i + 1) % 2 != 0:  # not even
                # not replicated
                # variable = 'xn' + str(i)
                variable = 'x' + str(i+1)
                if (i + 1) % 10 + 1 == self.site_id:
                    self.data[variable] = [((i + 1) * 10, current_timestamp)]
            else:
                # replicated
                # variable = 'xr' + str(i)
                variable = 'x' + str(i + 1)
                self.data[variable] = [((i + 1) * 10, current_timestamp)]

    # Fail this site
    def fail(self, timestamp):
        self.up = False
        self.up_times.append((self.start_time, timestamp))
        self.lock_table.clear()

    # Site recovers
    def recover(self, timestamp):
        # No variable from this site is now readable until written to
        self.up = True
        self.start_time = timestamp

    # For deadlock detection per site
    def get_dep_graph(self):
        return self.lock_table.get_dep_graph()

    # Abort given transaction
    def abort(self, transaction_id):
        self.lock_table.unlock_tid_queue(transaction_id)
        self.lock_table.release_locks_by_transaction(transaction_id)
        self.lock_table.dequeue_waiting_locks()

    # Perform commit at commit_time
    def commit(self, transaction_id, commit_time, transaction=None):

        # if transaction is not None:
        #     if transaction.id_trans == transaction_id:

        self.lock_table.release_locks_by_transaction(transaction_id)

        for variable in self.tempData.copy():
            if self.tempData[variable][1] == transaction_id:
                self.data[variable].append((self.tempData[variable][0], commit_time))
                del self.tempData[variable]

        self.lock_table.dequeue_waiting_locks()

    # Calls appropriate read function based on whether transaction is RO or RW
    def read(self, variable, transaction):
        if transaction.transactionType == TransType.READ_ONLY:
            return self.ro_read(variable, transaction.id_trans, transaction.startTime)
        return self.rw_read(variable, transaction.id_trans)

    # Read method for read-only transactions.
    def ro_read(self, variable, transaction_id, timestamp):
        committed_values = self.data[variable]
        for val_tuple in reversed(committed_values):
            if val_tuple[1] > timestamp:
                continue
            # val_tuple has last committed value before the transaction began
            if is_replicated_variable(variable):
                # variable is replicated
                if val_tuple[1] < timestamp and val_tuple[1] >= self.start_time:
                    # For current running span of site
                    # if last commit was between site start_time and start of RO transaction
                    return (val_tuple[0],0)
                # If not in current running span, check in previous up times
                for up_time in reversed(self.up_times):
                    if val_tuple[1] < timestamp and val_tuple[1] >= up_time[0] and timestamp < up_time[1]:
                        return (val_tuple[0],0)
                # This site does not have any usable committed value
                return (None,0)
            else:
                # variable is not replicated
                return (val_tuple[0],0)

        # No committed value before RO transaction began
        return (None,0)

    # Read method for read-write transactions.
    def rw_read(self, variable, transaction_id):
        committed_values = self.data[variable]

        # res = self.data[variable][-1][0]
        # if is_replicated_variable(variable):
        #     return None
        # elif self.lock_table.has_waiting_write_lock(variable):
        #     return None
        # if not self.lock_table.is_locked(variable) and self.lock_table.lock(transaction_id, variable, LockType.RLOCK):

        # Site is up but its last committed value was before recovery
        read_val = None
        to_enqueue = False
        if is_replicated_variable(variable) and self.data[variable][-1][1] < self.start_time:
            return (read_val, to_enqueue)
        else:
            if self.lock_table.is_locked(variable):
                if self.lock_table.is_read_locked(variable):
                    if transaction_id in self.lock_table.get_lock_owners(variable):
                        read_val = self.data[variable][-1][0]
                    else:
                        # read lock is held by some other transaction
                        if self.lock_table.has_waiting_write_lock(variable):
                            # lock has transactions waiting in queue to write
                            to_enqueue = True
                        else:
                            self.lock_table.share_lock(transaction_id, variable)
                            read_val = self.data[variable][-1][0]
                else:
                    if transaction_id in self.lock_table.get_lock_owners(variable):
                        read_val = self.tempData[variable][0]
                    else:
                        to_enqueue = True
                if to_enqueue:
                    #print(f'Transaction {transaction_id} is waiting for a read lock on {variable} at site {self.site_id}')
                    self.lock_table.lock_enqueue(transaction_id, variable, LockType.RLOCK)
            else:
                # If there's no lock on the variable, set a read lock before reading and return read value.
                self.lock_table.lock(transaction_id, variable, LockType.RLOCK)
                read_val = self.data[variable][-1][0]
        return (read_val,to_enqueue)

    # To check whether a transaction can get the write lock of the variable.
    def test_write_lock(self, transaction_id, variable):
        to_enqueue = False
        return_val = False
        if self.lock_table.is_locked(variable):
            if self.lock_table.is_read_locked(variable):
                if len(self.lock_table.get_lock_owners(variable)) == 1:
                    if transaction_id in self.lock_table.get_lock_owners(variable):
                        if self.lock_table.has_waiting_write_lock(variable, transaction_id):
                            to_enqueue = True
                        else:
                            return_val = True
                    else:
                        to_enqueue = True
                else:
                    to_enqueue = True
            else:
                if transaction_id in self.lock_table.get_lock_owners(variable):
                    return_val = True
                else:
                    to_enqueue = True
            if to_enqueue:
                #print(f'Transaction {transaction_id} is waiting for a write lock on {variable} at site {self.site_id}')
                self.lock_table.lock_enqueue(transaction_id, variable, LockType.WLOCK)
        else:
            # No lock on the variable, can set a write lock
            return_val = True

        # print('writelock', return_val)
        return return_val

    def write(self, transaction_id, variable, value):
        # self.dump()
        # Assume transaction manager has ensured wlocks can be obtained on all writeable sites
        if self.lock_table.is_locked(variable):
            # Current variable is locked
            if self.lock_table.is_read_locked(variable):
                self.lock_table.promote_lock(variable, transaction_id)
        else:
            # Current variable is not locked
            self.lock_table.lock(transaction_id, variable, LockType.WLOCK)
        self.tempData[variable] = (value, transaction_id)

    # dump the store
    def dump(self):
        o_str = f'site_id {self.site_id} up: {self.up}'
        strs = [f'{k}:{v[-1][0]}' for k, v in self.data.items()]
        o_str = f'{o_str} — {", ".join(strs)}'
        print(o_str)
