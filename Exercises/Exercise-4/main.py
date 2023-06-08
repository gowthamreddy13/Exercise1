import boto3


def main():
    # your code here
    pass
import os
import json
import pandas as pd

# Define the data directory path
data_dir = 'Exercises/Exercise-4/data'

# Get all JSON files in the data directory
json_files = []
for root, dirs, files in os.walk(data_dir):
    for file in files:
        if file.endswith('.json'):
            json_files.append(os.path.join(root, file))

# Process each JSON file
for json_file in json_files:
    # Load the JSON file
    with open(json_file, 'r') as file:
        json_data = json.load(file)

    # Flatten the JSON data
    flattened_data = {}

    def flatten_json(data, prefix=''):
        if isinstance(data, dict):
            for key, value in data.items():
                flatten_json(value, prefix + key + '_')
        elif isinstance(data, list):
            for index, item in enumerate(data):
                flatten_json(item, prefix + str(index) + '_')
        else:
            flattened_data[prefix[:-1]] = data

    flatten_json(json_data)

    # Convert flattened data to a DataFrame
    df = pd.DataFrame([flattened_data])

    # Get the output CSV file path
    csv_file = os.path.splitext(json_file)[0] + '.csv'

    # Write the DataFrame to the CSV file
    df.to_csv(csv_file, index=False)

if __name__ == "__main__":
    main()
