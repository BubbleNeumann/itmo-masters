from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if rank == 0:
    print("host: starting")
    for i in range(1, size):
        worker_rank = comm.recv(source=i, tag=0)
        print(f"host: received message from worker {worker_rank}")
