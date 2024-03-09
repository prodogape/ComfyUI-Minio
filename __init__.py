from .node import *

NODE_CLASS_MAPPINGS = {
    "Set Minio Config": SetMinioConfig,
    "Load Image From Minio": LoadImageFromMinio,
    "Save Image To Minio": SaveImageToMinio,
}
NODE_DISPLAY_NAME_MAPPINGS = {
    "Set Minio Config": "Set Minio Config",
    "Load Image From Minio": "Load Image From Minio",
    "Save Image To Minio": "Save Image To Minio",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
