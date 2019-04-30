from threading import Thread, Condition
from mpi4py import MPI
import random
from enum import Enum
import time

# SEE
MIN_SEE_SIZE = 5
MAX_SEE_SIZE = 6

# GROUP
MIN_GROUP_SIZE = 1
MAX_GROUP_SIZE = 4

# SLEEP
MIN_WAIT_TIME = 2
MAX_WAIT_TIME = 5

class Resource(Enum):
    NONE=0
    SEE=1
    VEHICLE=2
    ENGINEER=3

class Message(Enum):
    REQ=1
    RES_OK=2
    RES_REQ=3
    RELEASE=4
    FREE=5

class Guide():
    def __init__(self, m, comm, rank, size):
        # local data
        self.m = m
        self.max_m = m

        # requesting resource data
        self.req_res_cond = Condition()
        self.req_res = Resource.NONE
        self.x = None

        # mpi related
        self.comm = comm
        self.rank = rank
        self.size = size

        # time related
        self.lamport_cond = Condition()
        self.lamport = 0
        self.req_lamport = None

        self.responses = {}
        self.req_before = []

        self.proc_cond = Condition()

        self.running = True
        self.run()

    def run(self):
        # create thread listening for messages
        Thread(target=self.listen).start()
        
        while self.running:
            # wait until group gather
            self.log('wait for group to gather')
            self.rand_sleep()

            # request access to see, broadcast request
            self.log('requesting access to see')
            self.request_access_to_see()

            # wait until can enter section
            self.log('wait until can enter section')
            with self.proc_cond:
                self.proc_cond.wait()

            # change section state
            self.log('in section')
            self.see_section()

            # release section
            self.log('section released - traveling')

            # travel
            self.rand_sleep()

            # free see
            self.log('finished traveling - releasing see')
            self.free_see()

    def free_see(self):
        msg = {'id': self.rank, 'type': Message.FREE}
        
        with self.req_res_cond:
            msg['resource'] = Resource.SEE
            msg['amount'] = self.x
            self.m += self.x
            self.x = None

        self.broadcast_msg(msg)

    def see_section(self):
        msg = {'id': self.rank, 'type': Message.RELEASE}

        with self.req_res_cond:
            self.req_res = Resource.NONE
            self.req_lamport = None
            self.responses = {}
            self.m -= self.x
            msg['resource'] = Resource.SEE
            msg['amount'] = self.x

        self.broadcast_msg(msg)

    def request_access_to_see(self):
        msg = {'id': self.rank, 'type': Message.REQ}

        with self.req_res_cond:
            self.req_res = Resource.SEE
            msg['resource'] = Resource.SEE
            self.x = random.randint(MIN_WAIT_TIME, MAX_GROUP_SIZE)
            msg['amount'] = self.x

            with self.lamport_cond:
                self.lamport += 1
                self.req_lamport = self.lamport
                msg['req_lamport'] = self.req_lamport
        
        self.broadcast_msg(msg)

    def broadcast_msg(self, msg):
        with self.lamport_cond:
            self.lamport += 1
            msg['lamport'] = self.lamport

            for tid in range(self.size):
                if tid != self.rank:
                    self.comm.send(msg, dest=tid)
    
    def rand_sleep(self):
        sleep_time = random.randint(MIN_WAIT_TIME, MAX_WAIT_TIME)
        time.sleep(sleep_time)

    def send(self, msg, to):
        with self.lamport_cond:
            self.lamport += 1
            msg['lamport'] = self.lamport

        self.comm.send(msg, dest=to)

    def listen(self):
        while self.running:
            msg = self.receive()

            if msg['type'] == Message.REQ:
                # self.log('received request - {}'.format(msg))
                
                with self.req_res_cond:
                    if self.req_res != msg['resource']:
                        resp = {'id': self.rank, 'type': Message.RES_OK}
                        self.send(resp, msg['id'])
                    else:
                        resp = {'id': self.rank, 'type': Message.RES_REQ, 'amount': self.x, 'req_lamport': self.req_lamport}
                        self.send(resp, msg['id'])

            elif msg['type'] == Message.RES_OK or msg['type'] == Message.RES_REQ:
                self.responses[msg['id']] = msg
                # self.log('received response - {}'.format(msg))
                
                # check if can enter section
                self.can_enter_section()

            elif msg['type'] == Message.RELEASE:
                # self.log('received release - {}'.format(msg))
                
                with self.req_res_cond:
                    if msg['resource'] == Resource.SEE:
                        self.m -= msg['amount']

                    if self.req_res == msg['resource']:
                        self.responses[msg['id']] = msg
                        self.can_enter_section()
        
                # if res == vehicle, check req_before

            elif msg['type'] == Message.FREE:
                # self.log('received free - {}'.format(msg))
                
                with self.req_res_cond:
                    if msg['resource'] == Resource.SEE:
                        self.m += msg['amount']

                # check if can enter section
                if msg['resource'] == self.req_res:
                    self.can_enter_section()

    def can_enter_section(self):
        # return when didn't received response from others
        if len(self.responses.values()) < self.size-1:
            return

        # return when at least one of the messages has lamport before our request's lamport
        with self.req_res_cond:
            for res in self.responses.values():
                if res['lamport'] < self.req_lamport:
                    return

            res_sum = 0
            for res in self.responses.values():
                if 'req_lamport' in res:
                    if res['req_lamport'] < self.req_lamport or (res['req_lamport'] == self.req_lamport and res['id'] < self.rank):
                        self.req_before.append(res)
                        res_sum += res['amount']

            if res_sum + self.x <= self.m:
                with self.proc_cond:
                    self.proc_cond.notify()

    def receive(self):
        msg = self.comm.recv()

        with self.lamport_cond:
            self.lamport = max(self.lamport, msg['lamport']) + 1

        return msg

    def log(self, msg):
        print('id: {}, t: {:5}, req_t: {:4}, m: {:3}, x: {:4} - {}'.format(self.rank, self.lamport, self.req_lamport, self.m, self.x, msg))

def init_state():
    m = 0

    if rank == 0:
        m = random.randint(MIN_SEE_SIZE,MAX_SEE_SIZE)
        data = {'m': m}
    else:
        data = None

    data = comm.bcast(data,root=0)

    if rank != 0:
        m = data['m']

    return (m)

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    m = init_state()
    g = Guide(m, comm, rank, size)