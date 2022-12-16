from collections import defaultdict, deque

from misc import *
from op import *
from parse import parse
from site_manager import SiteManager
from transaction import Transaction, TransState, TransType


class TransactionManager:
    def __init__(self):
        # init vars
        self.sites = []
        self.ticks = 0
        self.commands = deque()
        self.str_to_op = {
            'dump': self.dump,
            'fail': self.fail,
            'recover': self.recover,
            'begin': self.begin,
            'beginRO': self.beginRO,
            'end': self.end,
            'W': self.queue_write,
            'R': self.queue_read,
        }
        self.str_to_cmd = {
            WRITE: self.write,
            READ: self.read,
        }
        self.activeTransactions = dict()
        self.sites = [SiteManager(i+1) for i in range(0, 10)]

    def getNextOperation(self, line):
        # parse and execute a line
        #print(f'time: {self.ticks}')
        command = parse(line)
        if command is None:
            self._tick()
            return

        #print(f'NEW OP {command}')

        # maybe do not need to do deadlock every tick
        # TODO: add deadlock
        dl = self.isDeadlocked()
        # print('deadlock: ', dl)

        if dl:
            for cmd in list(self.commands):
                print(f'TRY EXEC OP [{cmd.op},{cmd.id_trans},{cmd.id_val}]')
                f_cmd = self.str_to_cmd[cmd.op]
                done = f_cmd(cmd)
                if done:
                    self._rmcommand(cmd)

        # do operation
        operation_str = command[0]
        # get
        op = self.str_to_op[operation_str]
        # call
        print(f'NEW OP {command}')
        op(command)

        for cmd in list(self.commands):
            print(f'TRY EXEC OP [\'{cmd.op}\', \'{cmd.id_trans}\', \'{cmd.id_val}]\'')
            f_cmd = self.str_to_cmd[cmd.op]
            done = f_cmd(cmd)
            if done:
                self._rmcommand(cmd)

        self._tick()

    def end(self, arguments):
        # end trans, with a commit to db or fatal
        id_trans = arguments[1]
        if self.activeTransactions.get(id_trans).transactionState == TransState.ABORTED:
            self.abort(id_trans)
            #print(f'{id_trans} aborted')
            return

        self.commit(id_trans)
        print(f'{id_trans} commited')
        return

    def begin(self, arguments, readonly=False):
        # start the transaction
        id_trans = arguments[1]
        all_trans = self.activeTransactions
        #print(f'start trans {id_trans} read_only: {readonly}')
        transaction = Transaction(id_trans, self.ticks, (TransType.READ_ONLY if readonly else TransType.READ_WRITE), TransState.RUNNING)
        all_trans[id_trans] = transaction

    def beginRO(self, arguments, readonly=True):
        # start the transaction
        id_trans = arguments[1]
        all_trans = self.activeTransactions
        #print(f'start trans {id_trans} read_only: {readonly}')
        transaction = Transaction(id_trans, self.ticks, (TransType.READ_ONLY if readonly else TransType.READ_WRITE), TransState.RUNNING)
        all_trans[id_trans] = transaction

    def queue_read(self, command):
        # push a read command into the command queue

        id_trans = command[1]
        id_val = command[2]

        read_o = Operation(READ, id_trans, id_val)
        self.commands.append(read_o)

    def read(self, cmd):
        # do the read operation
        id_trans, id_val = cmd.id_trans, cmd.id_val
        trans = self.activeTransactions.get(id_trans)

        if trans.transactionType == TransType.READ_WRITE:
            for site in self.sites:
                in_site = id_val in site.data
                if in_site and site.up:
                    result_value = site.read(id_val, trans)
                    if result_value[0] is not None:
                        trans.sitesAccessed.append(site.site_id)
                        print(f'trans {id_trans} read {id_val} from {site.site_id} val {result_value[0]}')
                        print(f'{id_trans}: {result_value[0]}')
                        return True
                    else:
                        if result_value[1] is True:
                            print(f'Couldn\'t obtain read lock for trans {id_trans} on variable {id_val}')
                            break;
        else:
            some_site_down = False
            for site in self.sites:
                in_site = id_val in site.data
                if in_site:
                    if not site.up:
                        print(f'Transaction {id_trans} waiting for site {site.site_id} to recover for readonly')
                        some_site_down = True
                        continue
                    result_value = site.read(id_val, trans)
                    if result_value[0] is not None:
                        trans.sitesAccessed.append(site.site_id)
                        print(f'trans {id_trans} read {id_val} from {site.site_id} val {result_value[0]}')
                        print(f'{id_trans}: {result_value[0]}')
                        return True
            # No readable value and all sites were up. Abort.
            if not some_site_down:
                self.abort(id_trans)
        return False

    def queue_write(self, command):
        # push a write command into the command queue
        id_trans = command[1]
        id_val = command[2]
        value = command[3]
        write_o = Operation(WRITE, id_trans, id_val, value)
        self.commands.append(write_o)

    def write(self, cmd):
        # do the write operation
        id_trans, id_val, value = cmd.id_trans, cmd.id_val, cmd.value

        sites_write = []

        for site in self.sites:
            in_site = id_val in site.data
            if in_site and site.up:

                write_lock = site.test_write_lock(id_trans, id_val)
                if not write_lock:
                    print(f'Couldn\'t obtain write lock for trans {id_trans} on variable {id_val}')
                    return False
                sites_write.append(int(site.site_id))

        # no writes
        if len(sites_write) == 0:
            print('no sites to write', sites_write)
            return False

        trans = self.activeTransactions[id_trans]
        for site_id in sites_write:
            self.sites[site_id - 1].write(id_trans, id_val, value)
            trans.sitesAccessed.append(site_id)

        print(f'trans {id_trans} with {cmd} writes to {sites_write}')
        return True

    def isDeadlocked(self):
        res = False
        # peform deadlock dection
        blocking = build_deps(self.sites)
        deadlock = detect_dl(self.activeTransactions, blocking)
        if deadlock is not None:
            print(f'deadlock dected, aborting: {deadlock}')
            self.abort(deadlock)
            res = True

        return res

    def abort(self, id_trans):
        # perform abort
        [site.abort(id_trans) for site in self.sites]

        # remove trans from active
        # print('abort',  self.activeTransactions)
        new_active_trans = {}
        for key, value in self.activeTransactions.items():
            if key != id_trans:
                new_active_trans[key] = value
        self.activeTransactions = new_active_trans
        for op in list(self.commands):
            if op.id_trans == id_trans:
                self._rmcommand(op)

        print(f'{id_trans} aborted')

    def fail(self, arguments):
        site_id = int(arguments[1])

        # kill trans for site
        for trans in self.activeTransactions.values():
            if site_id in trans.sitesAccessed and trans.transactionType == TransType.READ_WRITE:
                trans.transactionState = TransState.ABORTED

        # cause a site to fail
        #print(f'fail on {site_id}')
        ts = self.ticks
        self.sites[site_id - 1].fail(ts)

    def recover(self, arguments):
        # recover site
        site_id = int(arguments[1])
        #print(f'recover site {site_id}')
        ts = self.ticks
        self.sites[site_id - 1].recover(ts)

    def commit(self, id_trans):
        # perofrm the commit for id_trans
        time = self.ticks
        #print(f'{id_trans} commits, time: {time}')
        [site.commit(id_trans, time) for site in self.sites]
        self.activeTransactions.pop(id_trans)

    def dump(self, *args):
        # dump state of simulatiom
        print('dumps:')
        [site.dump() for site in self.sites]

    def _rmcommand(self, c):
        # removes command
        self.commands.remove(c)

    def _tick(self):
        self.ticks += 1
        # print(self.ticks)


def detect_dl(t_dict, dep_graph):
    # check for cycle and evict youngest

    dep_trans = []
    for k in dep_graph.keys():
        dep_trans.append(k)

    cycle_trans = []  # keep track of trans with cycle

    for transaction in dep_trans:
        if node_in_cycle(transaction, dep_graph):
            cycle_trans.append(transaction)

    min_time = -1
    min_id = None

    # print('cycles', cycle_trans)
    # find min
    for t_id in cycle_trans:
        trans = t_dict[t_id]
        if trans.startTime > min_time:
            min_time = trans.startTime
            min_id = t_id

    # print('victim', min_id)
    return min_id


def build_deps(sites):
    # build dependcies
    up_sites = get_up_sites(sites)
    dep_g = defaultdict(set)

    site_graphs = [s.get_dep_graph() for s in up_sites]

    for g in site_graphs:
        for key in g.keys():
            value = g[key]
            dep_g[key].update(value)
            # if key in dep_g:
            #     dep_g[key].append(value)
            # else:
            #     dep_g[key] = [value]

    # # convert to set for unique
    # for key in dep_g.keys():
    #     print(dep_g[key])
    #     # dep_g[key] = set(dep_g[key])
    return dep_g
