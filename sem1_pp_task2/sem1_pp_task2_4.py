from mpi4py import MPI
import time


def sleep_for(time_duration=10):
    time.sleep(time_duration)


# Get my rank
rank = MPI.COMM_WORLD.Get_rank()
comm = MPI.COMM_WORLD

if rank == 0:
    print("host: going to sleep...")
    sleep_for(5)
    print("host: waking up and receiving the message...")
    message = comm.recv(source=1, tag=0)
    print(f"host: received message: {message}")

elif rank == 1:
    message = "Hello, world from Worker!"
    print("worker: sending message to host")
    comm.send(message, dest=0, tag=0)
    for i in range(3):
        print(f"worker: running... iter {i+1}")
        time.sleep(1)
