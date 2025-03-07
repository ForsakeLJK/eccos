import pandas as pd
import matplotlib.pyplot as plt

def process_data(file_path):
    # Load data from the CSV file
    df = pd.read_csv(file_path)

    # Assuming you need to calculate 'response_received', 'average_latency', and 'total_requests_sent'
    # Adjust this part based on how these values are derived in your actual dataset
    df['response_received'] = df.iloc[:, 0]  # Placeholder logic, replace with actual column
    df['average_latency'] = df.iloc[:, 1]    # Placeholder logic, replace with actual column
    df['total_requests_sent'] = df.iloc[:, 2] # Placeholder logic, replace with actual column

    # Calculate bin edges from 0 to the largest value of 'response_received', in steps of 'bin_size'
    bin_size = 50
    max_response = df['response_received'].max()

    # Create bins from 0 to the max value, inclusive
    bins = list(range(0, int(max_response) + bin_size, bin_size))

    # Bin the 'response_received' values
    df['response_bin'] = pd.cut(df['response_received'], bins=bins, right=False)

    # Count the occurrences of each bin
    bin_counts = df['response_bin'].value_counts().sort_index()

    # Plotting the distribution of response counts
    plt.figure(figsize=(10, 6))
    bin_counts.plot(kind='bar', width=0.8, color='skyblue')
    plt.title('Distribution of Response Count (Binned by 50)')
    plt.xlabel('Response Count (Binned)')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main():
    # Define the path to your CSV file
    file_path = 'logs/local-run/client-throughput.csv'  # Replace with your actual file path

    # Process and plot the data
    process_data(file_path)

if __name__ == "__main__":
    main()
