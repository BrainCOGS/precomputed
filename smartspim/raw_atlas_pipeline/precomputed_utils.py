from collections import defaultdict
import numpy as np

def determine_factors(chunk_size):
    # Determines how to calculate downsampling factors given an initial chunk size
    # The factors need to be defined in such a way that the chunks end up being 
    # maximally isotropic. 

    # Algorithm:
    # 1. Start by finding the largest number, N and its index in chunk_size, N_i. Downsample factor for N is D_N. Need to determine if 1 or 2.
    # 2. Then find the next largest number, M and its index in chunk_size, M_i. Downsample factor for N is D_M. Need to determine if 1 or 2.
    # 3. Now find the smallest number, P and its index in chunk_size, P_i. Downsample factor for P is D_P = 1
    # 4. Calculate X = round(N/M), Y = round(N/P).
    # 5. If X > 1, then let N = N/2. D_N = 2, D_M = 1. 
    #    Else, N and M are already within a factor of 2 and are isotropic. D_N=2, D_M=2. 
    #    If Y <= 1, N and P are already isotropic. Therefore chunk is isotropic -> D = [2,2,2]. Break out of while loop
    assert all([x>0 for x in chunk_size])
    factor_level_list = [[1,1,1]] # level 0 has [1,1,1] by default
    while True:
        D_N,D_M,D_P = 2,2,1 # initialize
        argsort = np.argsort(chunk_size)
        N_i = argsort[2]
        M_i = argsort[1]
        P_i = argsort[0]
        N = chunk_size[argsort[2]]
        M = chunk_size[argsort[1]]
        P = chunk_size[argsort[0]]
        X = round(N/M)
        Y = round(N/P)
        if X > 1:
            D_N, D_M = 2, 1
        elif Y <= 1: 
            D_P = 2
        factors = [0,0,0]
        factors[N_i] = D_N
        factors[M_i] = D_M
        factors[P_i] = D_P
        chunk_size = [0,0,0]
        chunk_size[N_i] = int(N/D_N)
        chunk_size[M_i] = int(M/D_M)
        chunk_size[P_i] = int(P/D_P)
        factor_level_list.append(factors)
        if factors == [2,2,2]:
            break
    # append a whole bunch more [2,2,2] levels
    for _ in range(20):
        factor_level_list.append([2,2,2])
    return factor_level_list

def calculate_chunks(downsample, mip):
    """
    Chunks default to 64,64,64 so we want different chunks at 
    different resolutions
    """
    d = defaultdict(dict)
    d['full'][-1] = [1024,1024,1]
    d['full'][0] = [128,128,64]
    d['full'][1] = [128,128,64]
    d['full'][2] = [128,128,64]
    d['full'][3] = [128,128,64]
    d['full'][4] = [128,128,64]
    d['full'][5] = [64,64,64]
    d['full'][6] = [64,64,64]
    d['full'][7] = [64,64,64]
    d['full'][8] = [64,64,64]
    d['full'][9] = [64,64,64]

    try:
        result = d[downsample][mip]
    except:
        result = [64,64,64]
    return result

def calculate_factors(downsample, mip):
    """
    Scales get calculated by default by 2x2x1 downsampling
    """
    d = defaultdict(dict)
    d['full'][0] = [2,2,1]
    d['full'][1] = [2,2,2]
    d['full'][2] = [2,2,2]
    d['full'][3] = [2,2,2]
    d['full'][4] = [2,2,2]
    d['full'][5] = [2,2,2]
    d['full'][6] = [2,2,2]
    d['full'][7] = [2,2,2]
    d['full'][8] = [2,2,2]
    d['full'][9] = [2,2,2]

    try:
        result = d[downsample][mip]
    except:
        result = [2,2,1]
    return result
