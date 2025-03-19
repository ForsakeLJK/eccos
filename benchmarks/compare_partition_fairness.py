import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def analyze_csv_distribution(file_path):
    try:
        third_column_data = []
        with open(file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                if len(row) >= 3:
                    try:
                        value = float(row[2])
                        third_column_data.append(value)
                    except ValueError:
                        pass
        data_array = np.array(third_column_data)
        stats = {
            "file_name": Path(file_path).name,
            "data_num": len(data_array),
            "avg": np.mean(data_array),
            "median": np.median(data_array),
            "std": np.std(data_array),
            "min": np.min(data_array),
            "max": np.max(data_array),
            "data": data_array.tolist()
        }
        return stats
    except Exception as e:
        return {"error": f"{str(e)}"}

def compare_multiple_csv_distributions(file_stats):
    print(f"{'Filename':<20} {'Stddev':<10}")
    print("-" * 30)
    sorted_stats = sorted(file_stats, key=lambda x: x.get("std", float('inf')))
    for idx, stats in enumerate(sorted_stats):
        std = stats.get("std", float('inf'))
        print(f"{stats['file_name']:<20} {std:<10.2f}")

def plot_distributions(file_stats):
    plt.figure(figsize=(15, 7))
    plt.subplot(1, 2, 1)
    n_files = len(file_stats)
    grouped_data = {}
    for i, stats in enumerate(file_stats):
        if "data" in stats:
            data_subset = stats["data"][:len(stats["data"])]
            for j, value in enumerate(data_subset):
                if j not in grouped_data:
                    grouped_data[j] = []
                while len(grouped_data[j]) < i:
                    grouped_data[j].append(None)
                grouped_data[j].append(value)
    for j in grouped_data:
        while len(grouped_data[j]) < n_files:
            grouped_data[j].append(None)
    bar_width = 0.8 / n_files
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    for j in range(len(grouped_data)):
        x = np.arange(len(grouped_data[j]))
        for i in range(n_files):
            if j in grouped_data and i < len(grouped_data[j]) and grouped_data[j][i] is not None:
                value = grouped_data[j][i]
                position = j + i * bar_width
                bar = plt.bar(position, value, bar_width, color=colors[i % len(colors)], label=file_stats[i]["file_name"] if j == 0 else "")
                plt.text(position, value + max(grouped_data[j]) * 0.02,
                         f'{value:.0f}',
                         ha='center', va='bottom', fontsize=9)
    group_positions = [j + (n_files-1) * bar_width / 2 for j in range(len(grouped_data))]
    plt.xticks(group_positions, [f"Parition {j+1}" for j in range(len(grouped_data))])
    plt.title('Partition Messages')
    plt.ylabel('Messages')
    plt.grid(True, linestyle='--', alpha=0.3, axis='y')
    plt.legend()

    plt.subplot(1, 2, 2)
    x_pos = np.arange(len(file_stats))
    width = 0.6
    std_values = [stats.get("std", 0) for stats in file_stats]
    bar_colors = [colors[i % len(colors)] for i in range(len(file_stats))]
    bars = plt.bar(x_pos, std_values, width, color=bar_colors, alpha=0.8)
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height + max(std_values) * 0.02,
                 f'{height:.2f}',
                 ha='center', va='bottom', fontsize=10)
    plt.title('Standard Deviation Comparison')
    plt.ylabel('Standard Deviation')
    plt.xticks(x_pos, [stats["file_name"] for stats in file_stats], rotation=45, ha='right')
    plt.grid(True, linestyle='--', alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig('partition_fairness.png', dpi=300)
    plt.show()

def main():
    # parser = argparse.ArgumentParser(description='analyze_distributions')
    # parser.add_argument('files', nargs='+',)
    # parser.add_argument('--no-plot', action='store_true')
    # args = parser.parse_args()
    no_plot = False
    file1_path = "logs/local-run/server-1.csv"
    file2_path = "logs/local-run/server-1-fcfs-11111.csv"
    files = [file1_path, file2_path]
    all_file_stats = []
    for file_path in files:
        stats = analyze_csv_distribution(file_path)
        if "error" in stats:
            print(f"error: {stats['error']}")
            continue
        all_file_stats.append(stats)
        # print(f"data_num: {stats['data_num']}")
        # print(f"avg: {stats['avg']:.2f}")
        # print(f"median: {stats['median']:.2f}")
        # print(f"std: {stats['std']:.2f}")
        # print(f"range: {stats['min']} - {stats['max']}")
    if len(all_file_stats) >= 2:
        compare_multiple_csv_distributions(all_file_stats)
        if not no_plot:
            try:
                plot_distributions(all_file_stats)
            except Exception as e:
                print(f"{str(e)}")

if __name__ == "__main__":
    main()