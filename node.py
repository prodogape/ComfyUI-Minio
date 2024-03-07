import folder_paths
from .core.minio_prodogape import MinioHandler

import os
import json
import time
import uuid
import torch
import datetime
import requests
import numpy as np
from PIL import Image
from PIL.PngImagePlugin import PngInfo
from base64 import b64encode
from io import BytesIO
import logging

logger = logging.getLogger("ComfyUI-Minio")

minio_config = "minio_config.json"
minio_dir_name = "minio"

class AnyType(str):
    """A special type that can be connected to any other types. Credit to pythongosssss"""
    def __ne__(self, __value: object) -> bool:
        return False

any_type = AnyType("*")


def Load_minio_config():
    folder = os.path.join(folder_paths.models_dir, minio_dir_name)
    minio_config_path = os.path.join(folder, minio_config)
    if os.path.exists(minio_config_path):
        with open(minio_config_path, 'r') as file:
            config_data = json.load(file)
        save_config_to_env(config_data)
        return config_data
    else:
        config_data = {
            "MINIO_HOST": os.environ.get("MINIO_HOST"),
            "MINIO_PORT": os.environ.get("MINIO_PORT"),
            "MINIO_ENDPOINT": os.environ.get("MINIO_ENDPOINT"),
            "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY"),
            "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY"),
            "COMFYINPUT_BUCKET": os.environ.get("COMFYINPUT_BUCKET"),
            "COMFYOUTPUT_BUCKET": os.environ.get("COMFYOUTPUT_BUCKET"),
            "MINIO_SECURE": os.environ.get("MINIO_SECURE"),
        }

        if all(value is not None for value in config_data.values()):
            save_config_to_local(config_data)
            return config_data
        else:
            return None

def save_config_to_env(config_data):
    if config_data:
        os.environ["MINIO_HOST"] = config_data["MINIO_HOST"]
        os.environ["MINIO_PORT"] = config_data["MINIO_PORT"]
        os.environ["MINIO_ENDPOINT"] = config_data["MINIO_ENDPOINT"]
        os.environ["MINIO_ACCESS_KEY"] = config_data["MINIO_ACCESS_KEY"]
        os.environ["MINIO_SECRET_KEY"] = config_data["MINIO_SECRET_KEY"]
        os.environ["COMFYINPUT_BUCKET"] = config_data["COMFYINPUT_BUCKET"]
        os.environ["COMFYOUTPUT_BUCKET"] = config_data["COMFYOUTPUT_BUCKET"]
        os.environ["MINIO_SECURE"] = str(config_data["MINIO_SECURE"])

def save_config_to_local(config_data): 
    if config_data:
        folder = os.path.join(folder_paths.models_dir, minio_dir_name)
        if not os.path.exists(folder):
            os.makedirs(folder)
        minio_config_path = os.path.join(folder, minio_config)
        with open(minio_config_path, "w") as config_file:
            json.dump(config_data, config_file, indent=4)

class SetMinioConfig:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "minio_host": (
                    "STRING",
                    {
                        "default": "http://localhost",
                    },
                ),
                "minio_port": (
                    "STRING",
                    {
                        "default": "9000",
                    },
                ),
                "minio_access_key": (
                    "STRING",
                    {
                        "default": "",
                    },
                ),
                "minio_secret_key": (
                    "STRING",
                    {
                        "default": "",
                    },
                ),
                "ComfyUI_input_bucket": (
                    "STRING",
                    {
                        "default": "comfyinput",
                    },
                ),
                "ComfyUI_output_bucket": (
                    "STRING",
                    {
                        "default": "comfyoutput",
                    },
                ),
                "minio_secure": ("BOOLEAN", {"default": False}),
            },
        }

    CATEGORY = "ComfyUI-Minio"
    FUNCTION = "main"
    RETURN_TYPES = (any_type,'JSON')
    RETURN_NAMES = ("init_info",'minio_config',)

    def main(
        self,
        minio_host,
        minio_port,
        minio_access_key,
        minio_secret_key,
        ComfyUI_input_bucket,
        ComfyUI_output_bucket,
        minio_secure
    ):

        os.environ["MINIO_HOST"] = minio_host
        os.environ["MINIO_PORT"] = minio_port
        os.environ["MINIO_ENDPOINT"] = f"{minio_host}:{minio_port}"
        os.environ["MINIO_ACCESS_KEY"] = minio_access_key
        os.environ["MINIO_SECRET_KEY"] = minio_secret_key
        os.environ["COMFYINPUT_BUCKET"] = ComfyUI_input_bucket
        os.environ["COMFYOUTPUT_BUCKET"] = ComfyUI_output_bucket
        os.environ["MINIO_SECURE"] = str(minio_secure)

        minio_client = MinioHandler()
        text = ''
        if minio_client.is_minio_connected(ComfyUI_input_bucket):
            text = "Minio initialize successful!"
        else:
            text = "Minio unable to connect, please check if your Minio is configured correctly!"

        config_data = {
            "MINIO_HOST": minio_host,
            "MINIO_PORT": minio_port,
            "MINIO_ENDPOINT": f"{minio_host}:{minio_port}",
            "MINIO_ACCESS_KEY": minio_access_key,
            "MINIO_SECRET_KEY": minio_secret_key,
            "COMFYINPUT_BUCKET": ComfyUI_input_bucket,
            "COMFYOUTPUT_BUCKET": ComfyUI_output_bucket,
            "MINIO_SECURE": minio_secure,
        }
        save_config_to_local(config_data)

        return (text,config_data)

class LoadImageFromMinio:

    @classmethod
    def INPUT_TYPES(cls):
        files = []
        config_data = Load_minio_config()
        if config_data is not None:
            COMFYINPUT_BUCKET = os.environ.get("COMFYINPUT_BUCKET")
            minio_client = MinioHandler()
            if minio_client.is_minio_connected(COMFYINPUT_BUCKET):
                files = minio_client.get_all_files_in_bucket(COMFYINPUT_BUCKET)
        return {
            "required": {
                "image": (sorted(files),),
            },
        }

    CATEGORY = "ComfyUI-Minio"
    FUNCTION = "main"
    RETURN_TYPES = ("IMAGE", "MASK")

    def main(self, image):
        config_data = Load_minio_config()
        if config_data is not None:
            minio_client = MinioHandler()
            if minio_client.is_minio_connected(config_data["COMFYINPUT_BUCKET"]):
                start_time = time.time()
                image_file = minio_client.get_file_by_name(
                    config_data["COMFYINPUT_BUCKET"], image
                )
                print(f"Minio get file time: {time.time()-start_time}s")

                i = Image.open(image_file)
                image = i.convert("RGB")
                image = np.array(image).astype(np.float32) / 255.0
                image = torch.from_numpy(image)[None,]
                if "A" in i.getbands():
                    mask = np.array(i.getchannel("A")).astype(np.float32) / 255.0
                    mask = 1.0 - torch.from_numpy(mask)
                else:
                    mask = torch.zeros((64, 64), dtype=torch.float32, device="cpu")
                return (image, mask)
            else:
                raise Exception("Failed to connect to Minio")
        else:
            raise Exception("please check if your Minio is configured correctly")

class SaveImageToMinio:

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "images": ("IMAGE",),
                "filename_prefix": (
                    "STRING",
                    {
                        "default": "ComfyUI",
                    },
                ),
            },
        }

    CATEGORY = "ComfyUI-Minio"
    FUNCTION = "main"
    RETURN_TYPES = ('JSON',)

    def main(self, images, filename_prefix):
        config_data = Load_minio_config()
        print("config_data", config_data)
        if config_data is not None:
            minio_client = MinioHandler()
            if minio_client.is_minio_connected(config_data['COMFYOUTPUT_BUCKET']):
                object_name = f"{datetime.datetime.now().strftime('%Y%m%d')}-{uuid.uuid1()}-{filename_prefix}.png"
                results = []
                for image in images:
                    i = 255. * image.cpu().numpy()
                    img = Image.fromarray(np.clip(i, 0, 255).astype(np.uint8))
                    metadata = None
                    buffer = BytesIO()
                    img.save(buffer, "png", pnginfo=metadata, compress_level=4)
                    minio_client.put_image_by_stream(
                        bucket_name=config_data["COMFYOUTPUT_BUCKET"],
                        file_name=object_name,
                        file_stream=buffer,
                    )
                    url = f"{config_data['MINIO_ENDPOINT']}/{config_data['COMFYOUTPUT_BUCKET']}/{object_name}"
                    results.append({"filename": object_name, "type": "output", "url": url})

                print('results',results)
                results_json = json.dumps(results)
                return results_json
            else:
                raise Exception("Failed to connect to Minio")
        else:
            raise Exception("please check if your Minio is configured correctly")
