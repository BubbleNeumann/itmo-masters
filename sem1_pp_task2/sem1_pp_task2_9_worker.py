from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank != 0:
    print(f"msg from {rank}")
    comm.send(rank, dest=0, tag=0)
