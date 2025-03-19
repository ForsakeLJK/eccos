import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys


def read_data(file_path):
    """Read the CSV data and return as a DataFrame."""
    try:
        # Reading CSV with no header
        df = pd.read_csv(file_path, header=None, names=['responses', 'avg_latency', 'total_requests'])
        return df
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        sys.exit(1)


def create_distribution(df, bin_size=50):
    """Create a distribution of response counts in bins of specified size."""
    # Find the maximum response count to determine the number of bins
    max_responses = df['responses'].max()
    # Create bins from 0 to max_responses with step of bin_size
    bins = list(range(0, int(max_responses) + bin_size, bin_size))

    # Count how many rows fall into each bin
    hist, bin_edges = np.histogram(df['responses'], bins=bins)

    # Create a DataFrame for the distribution
    dist_df = pd.DataFrame({
        'bin_start': bin_edges[:-1],
        'bin_end': bin_edges[1:],
        'count': hist
    })

    # Add a column for bin label (for display)
    dist_df['bin_label'] = dist_df.apply(lambda row: f"{int(row['bin_start'])}-{int(row['bin_end'])}", axis=1)

    return dist_df


def create_unified_bins(dist1, dist2):
    """Create a unified set of bins for both distributions."""
    unsorted_bins = list(set(dist1['bin_label'].tolist() + dist2['bin_label'].tolist()))
    # print(unsorted_bins)
    all_bins = sorted(unsorted_bins, key=lambda x: int(x.split('-')[0]))
    # print(all_bins)

    # Create a unified dataframe with all bins
    unified_df = pd.DataFrame({'bin_label': all_bins})

    # Merge the count data from both distributions
    unified_df = unified_df.merge(dist1[['bin_label', 'count']], on='bin_label', how='left')
    unified_df = unified_df.merge(dist2[['bin_label', 'count']], on='bin_label', how='left', suffixes=('_1', '_2'))

    # Fill NaN values with 0
    unified_df = unified_df.fillna(0)

    return unified_df


def plot_side_by_side_bars(unified_df, file1_name, file2_name):
    """Plot distributions with bars side by side for direct comparison."""
    fig, ax = plt.subplots(figsize=(16, 8))

    # Set width of bars
    bar_width = 0.35

    # Set position of bars on x axis
    x = np.arange(len(unified_df['bin_label']))

    # Create bars
    bar1 = ax.bar(x - bar_width / 2, unified_df['count_1'], bar_width, color='blue', alpha=0.7, label=file1_name)
    bar2 = ax.bar(x + bar_width / 2, unified_df['count_2'], bar_width, color='green', alpha=0.7, label=file2_name)

    # Add labels and title
    ax.set_xlabel('Response Count Range', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Comparison of Response Distributions', fontsize=14)

    # Add x-tick labels
    ax.set_xticks(x)
    ax.set_xticklabels(unified_df['bin_label'], rotation=45)

    # Add a legend
    ax.legend()

    # Add annotations for each bar
    for i, v in enumerate(unified_df['count_1']):
        if v > 0:
            ax.text(i - bar_width / 2, v + 0.5, str(int(v)), ha='center', va='bottom', fontsize=8)

    for i, v in enumerate(unified_df['count_2']):
        if v > 0:
            ax.text(i + bar_width / 2, v + 0.5, str(int(v)), ha='center', va='bottom', fontsize=8)

    # Add summary statistics
    df1_stats = f"{file1_name} - Total: {int(unified_df['count_1'].sum())}"
    df2_stats = f"{file2_name} - Total: {int(unified_df['count_2'].sum())}"

    ax.text(0.02, 0.95, df1_stats, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='blue', alpha=0.1))
    ax.text(0.02, 0.90, df2_stats, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='green', alpha=0.1))

    plt.tight_layout()
    plt.savefig('response_distribution_comparison.png', dpi=300)
    plt.show()


def main():
    file1_path = "logs/local-run/client-throughput-rr.csv"
    file2_path = "logs/local-run/client-throughput-lifo.csv"

    # Read data from both files
    df1 = read_data(file1_path)
    df2 = read_data(file2_path)

    # Create distributions
    dist1 = create_distribution(df1, bin_size=100)
    dist2 = create_distribution(df2, bin_size=100)

    # Create unified bins for side-by-side comparison
    unified_df = create_unified_bins(dist1, dist2)

    # Plot the distributions with side-by-side bars
    plot_side_by_side_bars(unified_df, file1_path, file2_path)

    # Print additional statistics
    print(f"\nFile: {file1_path}")
    print(f"Total responses: {df1['responses'].sum()}")
    print(f"Average latency: {df1['avg_latency'].mean():.2f} ms")
    print(f"Total requests: {df1['total_requests'].iloc[0]}")

    print(f"\nFile: {file2_path}")
    print(f"Total responses: {df2['responses'].sum()}")
    print(f"Average latency: {df2['avg_latency'].mean():.2f} ms")
    print(f"Total requests: {df2['total_requests'].iloc[0]}")


if __name__ == "__main__":
    main()
