from threading import Thread, Condition, Semaphore
from mpi4py import MPI
import random
from enum import Enum
import time
import sys

# SEA
MIN_SEA_SIZE = 8
MAX_SEA_SIZE = 10

# GROUP
MIN_GROUP_SIZE = 3
MAX_GROUP_SIZE = 5

# VEHICLE
MIN_VEHICLE_NUM = 2
MAX_VEHICLE_NUM = 2
MIN_VEHICLE_DURABILITY = 5
MAX_VEHICLE_DURABILITY = 10

# ENGINEER
MIN_ENGINEER_NUM = 1
MAX_ENGINEER_NUM = 2

# SLEEP
MIN_WAIT_TIME = 2
MAX_WAIT_TIME = 5

class Resource(Enum):
    NONE=0
    SEA=1
    VEHICLE=2
    ENGINEER=3

class Message(Enum):
    REQ=1
    RES_OK=2
    RES_REQ=3
    RELEASE=4
    FREE=5

class Guide():
    def __init__(self, m, p, t, comm, rank, size):
        # local data
        self.m = m
        self.max_m = m
        self.p = p
        self.t = t
        self.max_t = t

        # requesting resource data
        self.req_res_cond = Condition()
        self.req_res = Resource.NONE
        self.x = None
        self.taken_vehicle = None
        # self.taken_t = None

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
        self.taken_but_not_free = [] # vehicles that has been taken before we obtained free message

        self.traveler_semaphore = Semaphore()
        self.messenger_semaphore = Semaphore()

        self.running = True
        self.run()

    def run(self):
        # create thread listening for messages
        Thread(target=self.listen).start()
        self.traveler_semaphore.acquire()
        self.messenger_semaphore.acquire()
        
        while self.running:
            # wait until group gather
            self.log('wait for group to gather')
            self.rand_sleep()

            # request access to sea, broadcast request
            self.log('requesting access to sea')
            self.request_access_to_sea()

            # wait until can enter section
            self.log('wait until can enter sea section')
            self.traveler_semaphore.acquire()
            
            # change section state
            self.log('in sea section')
            self.sea_section()
            self.messenger_semaphore.release()
            
            # release section
            self.log('sea section released')

            # requesting access for vehicle
            self.log('requesting access for vehicle')
            self.request_access_to_vehicle()

            # wait until can enter vehicle section
            self.log('wait until can enter vehicle section')
            self.traveler_semaphore.acquire()
            
            # change vehicle section state
            self.log('in vehicle section')
            self.vehicle_section()
            self.messenger_semaphore.release()
            
            # release vehicle section
            self.log('vehicle section released')
            
            # travel
            self.log('traveling')
            self.rand_sleep(damage_vehicle=True)

            # free sea
            self.log('finished traveling - releasing sea')
            self.free_sea()

            # check if vehicle is a wreck
            if (self.taken_vehicle['durability'] == 0):
                # request access for engineer
                self.log('requesting access to engineer')
                self.request_access_to_engineer()

                # wait until can enter section
                self.log('wait until can enter engineer section')
                self.traveler_semaphore.acquire()
            
                # change section state
                self.log('in engineer section')
                self.engineer_section()
                self.messenger_semaphore.release()

                # release section
                self.log('engineer section released - repairing vehicle')

                # repair vehicle
                self.rand_sleep(repair_vehicle=True)

                # free engineer
                self.log('releasing engineer')
                self.free_engineer()
            
            # free vehicle
            self.log('releasing vehicle')
            self.free_vehicle()

    def free_engineer(self):
        msg = {'id': self.rank, 'type': Message.FREE}
        
        with self.req_res_cond:
            msg['resource'] = Resource.ENGINEER
            self.t += 1
            
            self.broadcast_msg(msg)

    def engineer_section(self):
        msg = {'id': self.rank, 'type': Message.RELEASE}

        with self.req_res_cond:
            self.req_res = Resource.NONE
            self.req_lamport = None
            self.responses = {}
            self.t -= 1
            msg['resource'] = Resource.ENGINEER
            
            self.broadcast_msg(msg)

    def request_access_to_engineer(self):
        msg = {'id': self.rank, 'type': Message.REQ}

        with self.req_res_cond:
            self.req_res = Resource.ENGINEER
            msg['resource'] = Resource.ENGINEER
    
            self.broadcast_msg(msg)

    ################################################

    def free_vehicle(self):
        msg = {'id': self.rank, 'type': Message.FREE}

        with self.req_res_cond:
            msg['resource'] = Resource.VEHICLE
            msg['vehicle'] = self.taken_vehicle
            self.p.append(self.taken_vehicle)
            self.taken_vehicle = None

            self.broadcast_msg(msg)

    def vehicle_section(self):
        msg = {'id': self.rank, 'type': Message.RELEASE}

        with self.req_res_cond:
            self.req_res = Resource.NONE
            self.req_lamport = None
            self.responses = {}
            self.taken_vehicle = self.p.pop(0)
            msg['vehicle'] = self.taken_vehicle['vid']
            msg['resource'] = Resource.VEHICLE
            
            self.broadcast_msg(msg)
        
    def request_access_to_vehicle(self):
        msg = {'id': self.rank, 'type': Message.REQ}

        with self.req_res_cond:
            self.req_res = Resource.VEHICLE
            msg['resource'] = Resource.VEHICLE
    
            self.broadcast_msg(msg)

    ################################################

    def free_sea(self):
        msg = {'id': self.rank, 'type': Message.FREE}
        
        with self.req_res_cond:
            msg['resource'] = Resource.SEA
            msg['amount'] = self.x
            self.m += self.x
            self.x = None

            self.broadcast_msg(msg)

    def sea_section(self):
        msg = {'id': self.rank, 'type': Message.RELEASE}

        with self.req_res_cond:
            self.req_res = Resource.NONE
            self.req_lamport = None
            self.responses = {}
            self.m -= self.x
            msg['resource'] = Resource.SEA
            msg['amount'] = self.x

            self.broadcast_msg(msg)

    def request_access_to_sea(self):
        msg = {'id': self.rank, 'type': Message.REQ}

        with self.req_res_cond:
            self.req_res = Resource.SEA
            msg['resource'] = Resource.SEA
            self.x = random.randint(MIN_WAIT_TIME, MAX_GROUP_SIZE)
            msg['amount'] = self.x
    
            self.broadcast_msg(msg)

    ################################################

    def broadcast_msg(self, msg):
        with self.lamport_cond:
            self.lamport += 1
            msg['lamport'] = self.lamport
            self.req_lamport = self.lamport
            msg['req_lamport'] = self.req_lamport
        
            for tid in range(self.size):
                if tid != self.rank:
                    self.comm.send(msg, dest=tid)
    
    def rand_sleep(self, damage_vehicle = False, repair_vehicle = False):
        sleep_time = random.randint(MIN_WAIT_TIME, MAX_WAIT_TIME)
        time.sleep(sleep_time)

        if damage_vehicle:
            self.taken_vehicle['durability'] = random.randint(0, self.taken_vehicle['durability'])
            
        if repair_vehicle:
            self.taken_vehicle['durability'] = self.taken_vehicle['max_durability']

    def send(self, msg, to):
        with self.lamport_cond:
            self.lamport += 1
            msg['lamport'] = self.lamport

        self.comm.send(msg, dest=to)

    ################################################

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
                        resp = {'id': self.rank, 'type': Message.RES_REQ, 'req_lamport': self.req_lamport} # 'amount': self.x
                        if self.req_res == Resource.SEA:
                            resp['amount'] = self.x
                        self.send(resp, msg['id'])

            elif msg['type'] == Message.RES_OK or msg['type'] == Message.RES_REQ:
                self.responses[msg['id']] = msg
                # self.log('received response - {}'.format(msg))
                
                # check if can enter section
                self.can_enter_section()

            elif msg['type'] == Message.RELEASE:
                # self.log('received release - {}'.format(msg))
                
                with self.req_res_cond:
                    if msg['resource'] == Resource.SEA:
                        self.m -= msg['amount']
                    elif msg['resource'] == Resource.VEHICLE:
                        filtered = list(filter(lambda v: v['vid'] != msg['vehicle'], self.p))
                        if len(filtered) == len(self.p):
                            print("ERROR - {}, {}, {}, {}".format(self.rank, self.p, filtered, msg['vehicle']))
                            self.taken_but_not_free.append(msg['vehicle'])
                            # sys.exit()
                        self.p = filtered
                    elif msg['resource'] == Resource.ENGINEER:
                        self.t -= 1

                if self.req_res == msg['resource']:
                    self.responses[msg['id']] = msg
                    self.can_enter_section()
        
            elif msg['type'] == Message.FREE:
                # self.log('received free - {}'.format(msg))
                
                with self.req_res_cond:
                    if msg['resource'] == Resource.SEA:
                        self.m += msg['amount']
                    elif msg['resource'] == Resource.VEHICLE:
                        # TODO: taken_but_not_free
                        if msg['vehicle']['vid'] in self.taken_but_not_free:
                            self.taken_but_not_free.remove(msg['vehicle']['vid'])
                        else: 
                            self.p.append(msg['vehicle'])
                    elif msg['resource'] == Resource.ENGINEER:
                        self.t += 1

                # check if can enter section
                if msg['resource'] == self.req_res:
                    self.can_enter_section()

    def can_enter_section(self):
        print(self.rank, self.responses)
        # return when didn't received response from others
        if len(self.responses.values()) < self.size-1:
            return

        # return when at least one of the messages has lamport before our request's lamport
        with self.req_res_cond:
            for res in self.responses.values():
                if res['lamport'] < self.req_lamport:
                    return

            res_sum = 0
            self.req_before = []
            for res in self.responses.values():
                if 'req_lamport' in res:
                    if res['req_lamport'] < self.req_lamport or (res['req_lamport'] == self.req_lamport and res['id'] < self.rank):
                        self.req_before.append(res)
                        res_sum += res['amount'] if self.req_res == Resource.SEA else 1

            if self.req_res == Resource.SEA:
                if res_sum + self.x > self.m:
                    return
                
            elif self.req_res == Resource.VEHICLE:
                # return if anyone choosing vehicle before us or there is no available vehicles
                if len(self.req_before) != 0 or len(self.p) == 0:
                    return
                
            elif self.req_res == Resource.ENGINEER:
                if res_sum + 1 > self.t:
                    return
                
        self.traveler_semaphore.release()
        self.messenger_semaphore.acquire()

    def receive(self):
        msg = self.comm.recv()

        with self.lamport_cond:
            self.lamport = max(self.lamport, msg['lamport']) + 1

        return msg

    def log(self, msg):
        colors = ['\033[91m','\033[92m','\033[93m','\033[94m','\033[95m','\033[96m']
        CEND = '\033[0m'
        # print(colors[self.rank] + 'id: {}, t: {:5}, req_t: {:4}, m: {:3}, x: {:4} - {}'.format(self.rank, self.lamport, self.req_lamport, self.m, self.x, msg) + CEND)
        parse_vehicle = lambda v: '{}:{}/{}'.format(v['vid'],v['durability'],v['max_durability'])
        vehicles = list(map(parse_vehicle, self.p))
        my_vehicle = list(map(parse_vehicle, [self.taken_vehicle])) if self.taken_vehicle else '-'
        print(colors[self.rank] + 'id: {}, l: {:5}, req_l: {:4}, m: {:3}, x: {:4}, p: {:30}, my_p: {:15}, t: {:2} - {}'.format(self.rank, self.lamport, self.req_lamport, self.m, self.x, vehicles, my_vehicle, self.t, msg) + CEND) 
        # check if 2 vehicles
        ct = list(map(lambda p: p['vid'], self.p))
        for c1 in ct:
            count = 0
            for c2 in ct:
                if c1 == c2:
                    count += 1
            if count >= 2:
                print("STOP P - {}".format(self.rank))
                sys.exit()
        # if self.m < 0:
        #     print("STOP M - {}".format(self.rank))
        #     sys.exit()
        # if self.t < 0:
        #     print("STOP T - {}".format(self.rank))
        #     sys.exit()

def init_state():
    m = 0
    p = []
    t = 0

    if rank == 0:
        m = random.randint(MIN_SEA_SIZE,MAX_SEA_SIZE)
        p_num = random.randint(MIN_VEHICLE_NUM, MAX_VEHICLE_NUM)
        for vehicle_id in (range(p_num)):
            durability = random.randint(MIN_VEHICLE_DURABILITY, MAX_VEHICLE_DURABILITY)
            p.append({'vid': vehicle_id, 'durability': durability, 'max_durability': durability})
        t = random.randint(MIN_ENGINEER_NUM, MAX_ENGINEER_NUM)
        data = {'m': m, 'p': p, 't': t}
    else:
        data = None

    data = comm.bcast(data,root=0)

    if rank != 0:
        m = data['m']
        p = data['p']
        t = data['t']  

    return (m, p, t)

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()
    m, p, t = init_state()
    g = Guide(m, p, t, comm, rank, size)