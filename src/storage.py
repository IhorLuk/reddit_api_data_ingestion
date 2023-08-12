import os
from google.cloud import storage

class GoogleStorageLoader():
    def __init__(self) -> None:
        """Start Google Cloud clint - could be used for uploading to storage
        """
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../content/key-bucket.json"
        self.client = storage.Client()

    def upload_to_bucket(self, bucket_name, source_file, destination):
        """uploads file to the bucket

        Args:
            bucket_name (str): _description_
            source_file (str): _description_
            destination (str): _description_
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination)
        blob.upload_from_filename(source_file)