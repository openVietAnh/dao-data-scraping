import numpy as np
import matplotlib.pyplot as plt

data = np.loadtxt("gnosis.eth_coalitions.csv", delimiter=",")

plt.figure(figsize=(16, 16))
plt.imshow(data, cmap="gray", interpolation="nearest")
plt.colorbar(label="Intensity")
plt.axis("off")
plt.show()
