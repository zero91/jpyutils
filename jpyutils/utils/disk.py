import os
import shutil
import logging

def move_data(src_data_files, dest_path, overwrite=False):
    """Move list of data file to destination path.

    Parameters
    ----------
    src_data_files: list
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

    if os.path.isfile(dest_path):
        if len(src_data_files) == 1:
            if overwrite:
                shutil.move(src_data_files[0], dest_path)
            return dest_path
        else:
            raise IOError("Need to move multiple files, but destination is an existed file")
    else:
        if not os.path.exists(dest_path) and len(src_data_files) == 1:
            shutil.move(src_data_files[0], dest_path)
            return dest_path

        os.makedirs(dest_path, exist_ok=True)
        dest_data_files = set(os.listdir(dest_path))
        failed_data_files = list()
        succeed_data_files = list()
        for data_file in src_data_files:
            if os.path.basename(data_file) in dest_data_files and not overwrite:
                failed_data_files.append(data_file)
                continue
            new_data_file = os.path.join(dest_path, os.path.basename(data_file))
            succeed_data_files.append(shutil.move(data_file, new_data_file))

        if len(failed_data_files) > 0:
            logging.info("Failed data files: %s" % (
                    ", ".join(map(os.path.basename, failed_data_files))))
            raise IOError("Need to move multiple files, but destination file is exists")
        return succeed_data_files

