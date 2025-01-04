from mpi4py import MPI

# Get my rank
rank = MPI.COMM_WORLD.Get_rank()

if rank == 0:
    message = "Hello, world!"
    req = MPI.COMM_WORLD.isend(message, dest=1, tag=0)  # send to rank 1
    req.wait()

if rank == 1:
    req = MPI.COMM_WORLD.irecv(source=0, tag=0)  # rec from rank 0
    message = req.wait()
    print(message)
