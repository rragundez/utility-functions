import os


def check_abs_path(path):
    """Raise exception if path is not absolute.

    Raises:
        TypeError: If path is not absolute.
    """
    if not os.path.isabs(path):
        raise TypeError("Path '{}' is not absolute.")
