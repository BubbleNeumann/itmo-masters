from mpi4py import MPI
import sys
import numpy as np
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

N = 10  # iters
initial_elements = 1
max_elements = 50000
increment = 1000  # per step

if rank == 0:
    current_elements = initial_elements
    while current_elements <= max_elements:
        data = [1] * current_elements

        data_size = sys.getsizeof(data)

        start_time = time.time()
        for _ in range(N):
            comm.send(data, dest=1, tag=0)
            received_data = comm.recv(source=1, tag=1)
        end_time = time.time()

        elapsed_time = end_time - start_time
        bandwidth = (2 * N * data_size) / elapsed_time / (1024 * 1024)

        print(f"object_size ({data_size} bytes): {bandwidth:.2f} MB/s")

        current_elements += increment

elif rank == 1:
    while True:
        data = comm.recv(source=0, tag=0)
        comm.send(data, dest=0, tag=1)
