import thread
import time

class Guide:
    def __init__(self, m, p, t, comm, rank, size):
        self.m = m
        self.p = p
        self.t = t
        self.lamport = 0
        self.request = []
        self.comm = comm
        self.rank = rank
        self.size = size
        self.m_lock = thread.allocate_lock()
        self.p_lock = thread.allocate_lock()
        self.t_lock = thread.allocate_lock()
        self.thr_id = None
        
    def listen(self):
        # test content
        print(self.rank, ' listening')
        tid = self.comm.recv(source=self.rank, tag=1)
        print(self.rank, tid)

    def run(self):
        # start thread for listening messages from others
        self.thr_id = thread.start_new_thread(self.listen, ())
        # test content
        print(self.rank, ' routine')
        self.comm.send(self.rank, dest=self.rank, tag=1)
        time.sleep(2)
