import logging

def gpg_decrypt_file(filepath, *, passphrase, recipient):
    """Decrypt file using gpg.

    Args:
        filepath (str): Path to file.
        passphrase (str): The passphrase to unlock private key.
        recipient (str): Recipient used during encryption.
    """
    logging.info("Decrypting file {}".format(filepath))
    cmd = ("echo {} | gpg --batch --passphrase-fd 0 -u {} -d {}"
           .format(passphrase, recipient, filepath))
    output, error = run_command(cmd,
                                get_output=True, shell=True, raise_exc=False)
    if not output:
        raise Exception(error)
    return output


def gpg_encrypt_file(filepath, recipient):
    """Encrypt file using gpg.

    Args:
        filepath (str): Path to file.
        recipient (str): The publickey to use according to the recipient.
    """
    logging.info("Encrypting file {} for recipient {}"
                 .format(filepath, recipient))
    cmd = ("gpg --yes --recipient {r} --output {f}.gpg --encrypt {f}"
           .format(r=recipient, f=filepath))
    run_command(cmd)
