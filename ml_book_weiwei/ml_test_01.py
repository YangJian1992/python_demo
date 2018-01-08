import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd



def sofmax(s):
    return np.exp(s) / np.sum(np.exp(s), axis=0)
x = np.arange(-3.0, 6.0, 0.1)
scores = np.vstack([x, np.ones_like(x), 0.2*np.ones_like(x)])
print(sofmax(scores)[0].shape)
plt.plot(x, sofmax(scores).T, linewidth=2)
y = np.poly1d([1, 0, 0])
plt.show()