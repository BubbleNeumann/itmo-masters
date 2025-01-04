from mpi4py import MPI
import numpy as np

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

elif rank == 1:
    message = comm.recv(source=0, tag=0)
    print(f"Worker 1 received: {message}")

elif rank == 2:
    message = comm.recv(source=0, tag=1)
    print(f"Worker 2 received: {message}")

elif rank == 3:
    message = comm.recv(source=0, tag=2)
    print(f"Worker 3 received: {message}")
    