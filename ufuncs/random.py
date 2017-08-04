
def generate_random_string(length, *, start_with=None,
                           chars=string.ascii_lowercase + string.digits):
    """Generate random string of numbers and lowercase letters.

    Args:
        length (int): Number of characters to be generated randomly.
        start_with (str): Optional string to start the output with.
        chars: Types of characters to choose from.

    Returns:
        str: Randomly generated string with optional starting string.
    """
    if start_with:
        start_with = '_'.join([start_with, '_'])
    else:
        start_with = ''

    rand_str = ''.join(random.choice(chars) for _ in range(length))
    return ''.join([start_with, rand_str])
