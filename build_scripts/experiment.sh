#!/bin/zsh
# Run the scripts
#./run-local-cluster.sh
#./run-local-client.sh

# Set the number of iterations (X times)
X=13  # Change this value as needed

# Loop X times
i=0
while [ $i -lt $X ]
do
    echo "Iteration $((i+1)) of $X"

    # Run the scripts
    ./run-local-cluster.sh
    ./run-local-client.sh

    # Wait for 40 seconds before the next iteration
    if [ $((i+1)) -lt $X ]; then
        sleep 15s
    fi

    ((i++))
done

echo "All $X iterations completed!"
