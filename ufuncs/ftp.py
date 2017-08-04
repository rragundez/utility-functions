def ftp_get(*, host, username, password,
            ftp_full_path, local_full_path, return_mdtm=False):
    """Function to mimic get command in ftp.

    Args:
        host (str): FTP server host name/ip
        username (str): Username in the FTP server
        password (str): Password for `username` on the FTP server
        ftp_full_path (str): Fullpath of the file to get.
        file_full_path (str): Local file fullpath where the remote file will
            be written.
        return_mdtm (bool): If return last modified datetime of file.

    Returns:
        None/str: If `return_mdtm` is False returns None, a timestamp
                string otherwise.
    """
    os.makedirs(os.path.dirname(local_full_path), exist_ok=True)

    logging.debug(("FTP credentials host={h} username={u}"
                   .format(h=host, u=username)))
    with FTP(host) as ftp:
        ftp.login(user=username, passwd=password)
        with open(local_full_path, 'wb') as of:
            logging.debug("FTP file {}".format(ftp_full_path))
            logging.debug("Local file {}".format(local_full_path))
            ftp.retrbinary('RETR ' + ftp_full_path, of.write)
        if return_mdtm:
            mdtm = ftp.sendcmd('MDTM ' + ftp_full_path).split()[1]
            logging.debug("FTP file modification date {}".format(mdtm))
            return mdtm
