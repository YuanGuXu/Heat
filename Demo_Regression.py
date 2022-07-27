import pandas as pd

from SCN import SCN
import numpy as np
import matplotlib.pyplot as plt
import scipy.io

from Heat_supply import *
X,Y=get_data_scn()


# Parameter Setting
L_max = 250                    # maximum hidden node number
tol = 0.001                    # training tolerance
T_max = 100                    # maximun candidate nodes number
Lambdas = [0.5, 1, 5, 10, 30, 50, 100, 150, 200, 250]  # scope sequence
r = [0.9, 0.99, 0.999, 0.9999, 0.99999, 0.999999]  # 1-r contraction sequence
nB = 1       # batch size
verbose = 10

# Model Initialization
M = SCN(L_max, T_max, tol, Lambdas, r, nB, verbose)

# load data

T1 = Y[:2500]
X1 = X[:2500]
T2 = Y[2500:]
X2 =X[2500:]

per = M.regression(np.array(X1), np.array(T1))

plt.subplot(3, 1, 1)
plt.plot(range(0, per.shape[1]), per.reshape(-1, 1).tolist(), 'r.-')
plt.axis(ymin=0, ymax=0.20)
plt.legend(['Training RMSE'])

O1 = M.getOutput(X1)
print(O1)
plt.subplot(3, 1, 2)
plt.plot(X1, T1, 'r.-')
plt.plot(X1, O1, 'b.-')

plt.legend(['Train Target', 'Model Output'])


O2 = M.getOutput(X2)
print(O2)
plt.subplot(3, 1, 3)
plt.plot(X2, T2, 'r.-')
plt.plot(X2, O2, 'b.-')

plt.legend(['Test Target', 'Model Output'])

plt.show()
