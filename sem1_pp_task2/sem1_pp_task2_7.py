from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

N = 10

if size < N:
    if rank == 0:
        print("err")
    exit()

if rank == 0:
    initial_message = "init"
    comm.send(initial_message, dest=1, tag=0)

    received_message = comm.recv(source=N - 1, tag=0)
    print(f"host received the message back: {received_message}")
    print("DONE")

else:
    message = comm.recv(source=(rank - 1) % N, tag=0)
    print(f"{rank} received message: {message}")

    comm.send(message, dest=(rank + 1) % N, tag=0)
