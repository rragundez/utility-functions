import subprocess

def run_command(command, *, get_output=False, shell=False, raise_exc=True):
    """Execute a shell command.

    Args:
        command (str): Shell command to run.
        get_output (bool): Indicate if command output should be returned.
            The returned object are bytes. Default False.
        shell (bool): Whether to use the shell as the program to execute.

    Returns:
        None/bytes: If get_output is True then returns output from
            executing the command.
    """
    logging.info("Executing shell command {}".format(command))
    command = command if shell else command.split()
    p = subprocess.Popen(command, shell=shell,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    output, error = p.communicate()
    if error and raise_exc:
        raise Exception(error.decode('utf-8'))
    if get_output and not raise_exc:
        return output, error.decode('utf-8')
    if get_output and raise_exc:
        return output
