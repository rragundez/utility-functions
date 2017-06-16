import os
import pickle


def object_to_local(obj, *, storage_path,
                    permission_code=755, overwrite=True):
    """Store a python object to a local pickle file.

    Args:
        obj: Python object.
        storage_path (str): Local full path of the file to write to.
        permission_code (int/str): Permission to set on the pickle file.
            Defaults to 755
        overwrite (bool): Overwrite if file already exists.
            Defaults to True

    Raises:
        IOError: If file exists and overwrite is set to False.
    """
    if overwrite or not os.path.isfile(storage_path):
        os.makedirs(os.path.dirname(storage_path), exist_ok=True)
        with open(storage_path, 'wb') as f:
            pickle.dump(obj, f)
        os.chmod(storage_path, int(str(permission_code), 8))
    else:
        raise FileExistsError("Local file '{}' already exists. "
                              "Argument overwrite set to {}."
                              .format(storage_path, overwrite))


def object_from_local(storage_path):
    """Retrieve a python object from a local pickle file.

    Args:
        storage_path (str): Local full path of the file to read from.

    Returns:
        The python object that was loaded from the pickle file.

    Raises:
        NameError: If the object's class is not defined in the namespace.
    """
    try:
        with open(storage_path, 'rb') as f:
            obj = pickle.load(f)
    except AttributeError as _:
        raise NameError("Pickle file object class not found in the namespace.")
    return obj
