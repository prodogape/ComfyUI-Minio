import os
from minio import Minio
import mimetypes
from datetime import timedelta

class MinioHandler:
    def __init__(self):
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY")
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT")
        self.client = self.initialize_minio_client()

    def initialize_minio_client(self):
        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )  # Set secure to True if using HTTPS
        return client

    def is_minio_connected(
        self,
        bucket_name,
    ):
        try:
            self.client.bucket_exists(bucket_name)
            return True
        except Exception as e:
            return False

    def put_file(self, bucket_name, file_name, file_stream, length: int, content_type="application/octet-stream"):
        file_stream.seek(0)
        return self.client.put_object(
            bucket_name=bucket_name, object_name=file_name, data=file_stream, length=length, content_type=content_type
        )

    def put_image_by_stream(self, bucket_name, file_name, file_stream):
        file_extension = os.path.splitext(file_name)[-1]
        file_stream_size = file_stream.getbuffer().nbytes
        file_extension = file_extension.split(".")[-1]
        content_type = mimetypes.types_map.get(
            "." + file_extension.lower(), "application/octet-stream"
        )
        return self.put_file(
            bucket_name=bucket_name,
            file_name=file_name,
            file_stream=file_stream,
            length=file_stream_size,
            content_type=content_type,
        )

    def get_all_files_in_bucket(self, bucket_name):
        files = []
        objects = self.client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            files.append(obj.object_name)
        return files

    def get_file_by_name(self, bucket_name, file_name):
        return self.client.get_object(bucket_name=bucket_name, object_name=file_name)

    def get_file_url_by_name(self, bucket_name, file_name, expires_hours=1):
        expires = timedelta(hours=expires_hours)
        return self.client.presigned_get_object(bucket_name=bucket_name, object_name=file_name, expires=expires)

# Example usage
if __name__ == "__main__":
    minio_handler = MinioHandler()
    bucket_name = "your_bucket_name"
    if minio_handler.is_minio_connected(bucket_name):
        file_path = "path/to/your/image.png"
        object_name = "image.png"
        mime_type = "image/png"

        minio_handler.upload_image_to_bucket(bucket_name, file_path, object_name, mime_type)
    else:
        print("Failed to connect to Minio")
