import os
import shutil
import logging
import hashlib

def move_data(src_data_files, dest_path, overwrite=False):
    """Move list of data file to destination path.

    Parameters
    ----------
    src_data_files: list or str
        A list of data path in string.

    dest_path: str
        A path.

    overwrite: boolean
        Whether or not to overwrite the file if the data file is already exists.

    Returns
    -------
    data_files: list/str
        List of path for data files after move.

    Raises
    ------
    IOError: If the moving operation can't proceed.

    """
    if len(src_data_files) == 0:
        return src_data_files

    if isinstance(src_data_files, str):
        src_data_files = [src_data_files]

    dest_path = os.path.realpath(dest_path)
    if os.path.isfile(dest_path):
        if len(src_data_files) == 1:
            if overwrite:
                shutil.move(src_data_files[0], dest_path)
            return dest_path
        else:
            raise IOError("Need to move multiple files, but destination is an existed file")
    else:
        if not os.path.exists(dest_path) and len(src_data_files) == 1:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.move(src_data_files[0], dest_path)
            return dest_path

        os.makedirs(dest_path, exist_ok=True)
        dest_data_files = set(os.listdir(dest_path))
        failed_data_files = list()
        succeed_data_files = list()
        for data_file in src_data_files:
            new_data_file = os.path.join(dest_path, os.path.basename(data_file))
            if os.path.exists(new_data_file) and not overwrite \
                    and md5(data_file) != md5(new_data_file):
                failed_data_files.append(data_file)
                continue
            succeed_data_files.append(shutil.move(data_file, new_data_file))

        if len(failed_data_files) > 0:
            logging.warning("Failed data files: %s" % (
                                ", ".join(map(os.path.basename, failed_data_files))))
            raise IOError("Failed to move %d data files" % (len(failed_data_files)))
        if len(succeed_data_files) == 1:
            return succeed_data_files[0]
        return succeed_data_files


def md5(fname):
    """Calculate md5 value of a file.

    Parameters
    ----------
    fname: string
        The name of the file.

    Returns
    -------
    md5_value: string
        The md5 hexdigest of the file content.

    """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as fin:
        for chunk in iter(lambda: fin.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

