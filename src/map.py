import matplotlib.pyplot as plt

class HeatMap:
    def __init__(self, data):
        self.data = data

    def plot_heatmap(self):
        plt.imshow(self.data, cmap='hot', interpolation='nearest')
        plt.colorbar()
        plt.show()

if __name__ == "__main__":
    # Example usage
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    heatmap = HeatMap(data)
    heatmap.plot_heatmap()