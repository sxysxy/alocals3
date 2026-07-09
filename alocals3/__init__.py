__version__ = "0.7.1"

__all__ = ["ALocalS3Client", "ALocalS3ClientAsync", "__version__"]


def __getattr__(name: str):
    if name == "ALocalS3Client":
        from .client import ALocalS3Client

        return ALocalS3Client
    if name == "ALocalS3ClientAsync":
        from .client import ALocalS3ClientAsync

        return ALocalS3ClientAsync
    raise AttributeError(f"module 'alocals3' has no attribute {name!r}")
