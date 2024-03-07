import os
from minio import Minio

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
            secure=False,
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

    def upload_image_to_bucket(self, bucket_name, file_path, object_name, mime_type):
        try:
            self.client.fput_object(
                bucket_name, object_name, file_path, content_type=mime_type
            )
            print(f"File {object_name} uploaded successfully to bucket {bucket_name}")
        except Exception as err:
            print(err)

    def upload_image_to_minio(self, bucket_name, image, object_name):
        try:
            i = 255.0 * image.cpu().numpy()
            img = Image.fromarray(np.clip(i, 0, 255).astype(np.uint8))
            metadata = None
            buffer = BytesIO()
            img.save(
                buffer, "png", pnginfo=metadata, compress_level=self.compress_level
            )
            buffer.seek(0)

            # Upload the image to Minio
            self.client.put_object(
                bucket_name, object_name, buffer, len(buffer.getvalue()), "image/png"
            )

            return True
        except Exception as e:
            print(f"Error uploading image to Minio: {e}")
            return False

    def get_all_files_in_bucket(self, bucket_name):
        files = []
        objects = self.client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            files.append(obj.object_name)
        return files

    def get_file_by_name(self, bucket_name, file_name):
        return self.client.get_object(bucket_name=bucket_name, object_name=file_name)

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
