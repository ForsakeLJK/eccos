import matplotlib.pyplot as plt
import numpy as np
import csv

def read_csv(filename):
    strategies = []
    latencies = []
    with open(filename, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            strategies.append(row[0])
            latencies.append(float(row[1]))
    return strategies, latencies

def plot_latency(strategies, latencies):
    plt.figure(figsize=(10, 5))
    x = np.arange(len(strategies))
    plt.bar(x, latencies, color='blue')
    plt.xticks(x, strategies, rotation=30, ha='right')
    plt.xlabel("Scheduling Strategy")
    plt.ylabel("Average Latency (ms)")
    plt.title("Overall Throughput of Scheduling Strategies (Average Latency)")
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Show values on top of bars
    for i, v in enumerate(latencies):
        plt.text(i, v + 20, f"{v:.2f}", ha='center', fontsize=10)

    plt.show()

def main():
    filename = 'logs/local-run/throughput_exp.csv'
    strategies, latencies = read_csv(filename)
    plot_latency(strategies, latencies)

if __name__ == "__main__":
    main()