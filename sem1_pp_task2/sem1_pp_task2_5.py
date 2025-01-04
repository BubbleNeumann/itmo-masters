from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

vector_size = 100000

if rank == 0:
    vector1 = np.ones(vector_size, dtype=np.float64)
    vector2 = np.full(vector_size, 2, dtype=np.float64)
    chunks1 = np.array_split(vector1, size)
    chunks2 = np.array_split(vector2, size)
else:
    chunks1 = None
    chunks2 = None

chunk1 = comm.scatter(chunks1, root=0)
chunk2 = comm.scatter(chunks2, root=0)

partial_dot_product = np.dot(chunk1, chunk2)

partial_results = comm.gather(partial_dot_product, root=0)

if rank == 0:
    final_dot_product = sum(partial_results)
    print(f"res: {final_dot_product}")
