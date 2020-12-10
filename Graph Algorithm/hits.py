import numpy as np

def hits(M, num_iterations: int = 100):

    N = M.shape[1]
    h = np.ones(N)
    a = np.ones(N)

    for i in range(num_iterations):
        # authority update 
        a =  h @ M
        a = a/ np.linalg.norm(a,1)
        # hub update
        h =  M @ a 
        h = h/ np.linalg.norm(h,1)

    return a ,h 

M = np.array([[0, 1, 0, 1, 1],
              [0, 0, 0, 0, 0],
              [1, 0, 0, 0, 0],
              [0, 0, 0, 0, 0],
              [0, 0, 0, 0, 0]])
a,h = hits(M, 50)
print(a,h)


