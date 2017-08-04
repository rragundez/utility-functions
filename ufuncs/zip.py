def unzip(zipfile_fullpath, *,
          output_directory=None, head_str=None, delete_zip=False):
    """Unzip a .zip file to a location.

    Args:
        zipfile_fullpath (str): Fullpath of file to unzip.
        output_directory (str): Path of directory to extract .zip to.
        head_str (str/None): String to beggining of extracted filenames.
        delete_zip (bool): Indicate if after extraction original .zip
            file should be deleted.
    """
    if output_directory:
        os.makedirs(output_directory, exist_ok=True)
    else:
        output_directory = os.path.dirname(zipfile_fullpath)

    logging.debug("Zip file {}".format(zipfile_fullpath))
    with ZipFile(zipfile_fullpath, 'r') as zf:
        for filename in zf.namelist():
            output_fullpath = os.path.join(
                output_directory,
                '{0}_{1}'.format(head_str, filename) if head_str else filename)
            logging.debug("Unzipped file {}".format(output_fullpath))
            with open(output_fullpath, 'wb') as of:
                shutil.copyfileobj(zf.open(filename, 'r'), of)
    if delete_zip:
        logging.debug("Removed file {}".format(zipfile_fullpath))
        os.remove(zipfile_fullpath)
