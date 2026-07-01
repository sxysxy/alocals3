__version__ = "0.6.1"

__all__ = ["LocalS3Client", "LocalS3ClientAsync", "__version__"]


def __getattr__(name: str):
    if name == "LocalS3Client":
        from .client import LocalS3Client

        return LocalS3Client
    if name == "LocalS3ClientAsync":
        from .client import LocalS3ClientAsync

        return LocalS3ClientAsync
    raise AttributeError(f"module 'alocals3' has no attribute {name!r}")
