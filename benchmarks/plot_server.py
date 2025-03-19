import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def main(file_path):
    # Load the CSV data without headers
    df = pd.read_csv(file_path, header=None)

    # Rename columns for easier access
    df.columns = ['key_range_left', 'key_range_right', 'decided_entries_count']

    # Calculate standard deviation for fairness
    std_dev = np.std(df['decided_entries_count'])

    sum = np.sum(df['decided_entries_count'])

    # Print the results
    print("Decided Entries Distribution:")
    print(df[['decided_entries_count']])
    print(f"Standard Deviation of Decided Entries: {std_dev:.2f}")
    print(f"Sum of Decided Entries: {sum:.2f}")

    # Plotting the results
    partitions = np.arange(len(df))  # Create an array of partition indices
    plt.bar(partitions, df['decided_entries_count'])
    plt.xlabel('Partitions')
    plt.ylabel('Decided Entries Count')
    plt.title('Decided Entries Distribution Across Partitions')
    plt.xticks(partitions)  # Set x-ticks to partition indices
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    # Specify the file path and total decided entries
    file_path = 'logs/local-run/server-1.csv'  # Change this to your actual file path
    main(file_path)
