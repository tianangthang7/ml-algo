import numpy as np

def generate_data(w,b,num_example):
    """
    Generate Y = WX + b + noise

    Args:
        w ([type]): [The weight]
        b ([type]): [The bias]
        num_example ([type]): [Number of example]
    """
    W = np.array([2,-3.4])
    b = 4.2 
    num_example = 1000
    X = np.random.normal(0,1, (num_example,W.shape[0]))
    y = np.matmul(X,W) +b
    y = y + np.random.normal(0,0.01,y.shape)
    return X,y

