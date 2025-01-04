from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    message = "msg"
    # non-blocking send
    req = comm.isend(message, dest=1, tag=0)
    print("host: WAITING")
    req.wait()

elif rank == 1:
    print("worker: sleep for 25 seconds before receiving the message...")
    time.sleep(25)
    # blocking receive
    received_message = comm.recv(source=0, tag=0)
    print(f"worker: rece message: {received_message}")
