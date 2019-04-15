# tag = 1 - request/response
# {id: int, lamport: int, resource_type: int, requested_size?: int, vehice_id?: int} 
# 1 - see, 2 - vehicle, 3 - engineer
# tag = 2 - release
# {id: int, lamport: int, resource_type: int, size: int} 
# +take/-release

# SEE
MIN_SEE_SIZE = 5
MAX_SEE_SIZE = 15
MIN_GROUP_SIZE = 1
MAX_GROUP_SIZE = 4
# VEHICLE
MIN_VEHICLE_NUM = 2
MAX_VEHICLE_NUM = 3
MIN_VEHICLE_DURABILITY = 5
MAX_VEHICLE_DURABILITY = 10
# ENGINEER
MIN_ENGINEER_NUM = 1
MAX_ENGINEER_NUM = 2

import threading
from mpi4py import MPI
import random

# local data
m = 0 # size of see
p = [] # array of vehicles
t = 0 # number of engineers
lamport = 0
requests = []

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def initialize_state():
    if rank == 0:
        m = random.randint(MIN_SEE_SIZE,MAX_SEE_SIZE)
        p_num = random.randint(MIN_VEHICLE_NUM, MAX_VEHICLE_NUM)
        for vehicle_id in (range(p_num)):
            p.append({'vehicle_id': vehicle_id, 'durability': random.randint(MIN_VEHICLE_DURABILITY, MAX_VEHICLE_DURABILITY)})
        t = random.randint(MIN_ENGINEER_NUM, MAX_ENGINEER_NUM)
        data = {'m': m, 'p': p, 't': t}
    else:
        data = None
    data = comm.bcast(data, root=0)
    if rank != 0:
        m = data['m']
        p = data['p']
        t = data['t']  

# each process creates thread for listening
# prepare locks for shared data
# start routine

if __name__ == "__main__":
    initialize_state()

