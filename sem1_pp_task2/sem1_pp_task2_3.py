from mpi4py import MPI
import numpy as np
import time

# Get my rank
rank = MPI.COMM_WORLD.Get_rank()
comm = MPI.COMM_WORLD

if rank == 0:
    object1 = [1, 2, 3, 4, 5]
    object2 = {'name': 'Worker', 'id': 1}
    object3 = np.array([1.1, 2.2, 3.3, 4.4, 5.5])

    list_of_objects = [object1, object2, object3]

    comm.send(list_of_objects[0], dest=1, tag=0)
    comm.send(list_of_objects[1], dest=2, tag=1)
    comm.send(list_of_objects[2], dest=3, tag=2)

    for i in range(1, 4):
        start_time = comm.recv(source=i, tag=10)
        end_time = comm.recv(source=i, tag=11)
        elapsed_time = (end_time - start_time) * 1000
        print(f"Time for Worker {i} to send and host to receive: {elapsed_time:.2f} ms")

elif rank == 1:
    message = comm.recv(source=0, tag=0)
    print(f"Worker 1 received: {message}")

    start_time = time.time()
    comm.send(start_time, dest=0, tag=10)
    end_time = time.time()
    comm.send(end_time, dest=0, tag=11)

elif rank == 2:
    message = comm.recv(source=0, tag=1)
    print(f"Worker 2 received: {message}")

    start_time = time.time()
    comm.send(start_time, dest=0, tag=10)
    end_time = time.time()
    comm.send(end_time, dest=0, tag=11)

elif rank == 3:
    message = comm.recv(source=0, tag=2)
    print(f"Worker 3 received: {message}")

    start_time = time.time()
    comm.send(start_time, dest=0, tag=10)
    end_time = time.time()
    comm.send(end_time, dest=0, tag=11)
