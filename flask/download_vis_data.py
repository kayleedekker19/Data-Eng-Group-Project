from google.cloud import storage

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")

# Example usage:
bucket_name = 'data_eng_group_bucket'
source_blob_name = 'weather/data/dataflow/1100da42-fa6d-4abc-840f-2126fc1a2e10.npz'
destination_file_name = '/Users/kayleedekker/PycharmProjects/DataEngineeringProject/flask/file_1.npz'

download_blob(bucket_name, source_blob_name, destination_file_name)

