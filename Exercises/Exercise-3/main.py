import boto3


def main():
    # your code here
    pass
import boto3
import gzip

# Define the S3 bucket and key
bucket_name = 'commoncrawl'
key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

# Create a Boto3 client for S3
s3_client = boto3.client('s3')

# Step 1: Download the .gz file
s3_client.download_file(bucket_name, key, 'wet.paths.gz')

# Step 2: Extract the file and retrieve the URI
with gzip.open('wet.paths.gz', 'rt') as file:
    uri = file.readline().strip()

# Step 3: Download the URI file
uri_bucket = uri.split('/')[2]
uri_key = '/'.join(uri.split('/')[3:])
s3_client.download_file(uri_bucket, uri_key, 'downloaded_file.txt')

# Step 4: Print each line of the file
with open('downloaded_file.txt', 'r') as file:
    for line in file:
        print(line.strip())


if __name__ == "__main__":
    main()
