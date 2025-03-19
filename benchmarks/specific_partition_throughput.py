import csv
import numpy as np
import matplotlib.pyplot as plt
import argparse
from pathlib import Path

def plot_partition_messages(file_path):
    messages_by_partition = []
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            if len(row) >= 3:
                try:
                    value = float(row[2])
                    messages_by_partition.append(value)
                except ValueError:
                    pass
    
    plt.figure(figsize=(10, 6))
    x_pos = np.arange(len(messages_by_partition))
    plt.bar(x_pos, messages_by_partition, width=0.6, color='#1f77b4')
    for i, value in enumerate(messages_by_partition):
        plt.text(i, value + max(messages_by_partition) * 0.02, 
                 f'{int(value)}', 
                 ha='center', va='bottom', fontsize=10)
    plt.title('Partition throughouts', fontsize=14)
    plt.xlabel('Partitions', fontsize=12)
    plt.ylabel('Number of decided messages', fontsize=12)
    plt.xticks(x_pos, [f'Partition {i+1}' for i in range(len(messages_by_partition))])
    plt.grid(True, linestyle='--', alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig('specific_partition_throughput.png', dpi=300)
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='specific_partition_throughput')
    parser.add_argument('file')
    args = parser.parse_args()
    plot_partition_messages(args.file)

if __name__ == "__main__":
    main()