import os


def expand(path: str) -> str:
    """Expand '~' and environment variables in a path string.

    Returns an absolute, user-expanded path.
    """
    if not isinstance(path, str):
        return path
    return os.path.expanduser(os.path.expandvars(path))

