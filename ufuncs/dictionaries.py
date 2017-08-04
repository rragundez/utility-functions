def get_values_from_key(a_dict, *, key, recursive=True, only_dict=False):
    """Lookup key in multi-level dictionary.

    This function will transverse a multi-level dictionary and extract
    to a list all the values corresponding to key `key`.
    `max_level` argument indicates how deep to look in the dictionary.
    If no key is found an KeyError exception will be raised.

    Example:
        input_dict = {'A': 'valueA',
                      'B': {'BA': 'valueBA',
                            'BB': {'BBA': 'valueBBA'}},
                      'C': {'CA': {'$value': 'valueCA'}},
                      'D': {'A': {'$value': 'valueCA'}}
                      }
        >>> from profilefit.utils.utils import get_values_from_key
        >>> get_values_from_key(input_dict, key='A')
        ['valueA', {'$value': 'valueCA'}]
        >>> get_values_from_key(input_dict, key='A', recursive=False)
        ['valueA']
        >>> get_values_from_key(input_dict, key='CA')
        [{'$value': 'valueCA'}]
        >>> get_values_from_key(input_dict, key='CA', recursive=False)
        KeyError: "Key 'CA' not found. Search not recursive."
        >>> get_values_from_key(input_dict, key='Z')
        KeyError: "Key 'z' not found."

    Args:
        a_dict (dict): Multi-level dictionary to transverse.
        key (hashable): Key to look for in the dictionary.
        recursive (bol): If to search multiple levels into the dictionary.
        only_dict (bol): Only return values that are dictionaries.

    Returns:
        list: List of values found corresponding to key `key`.
    """
    check_type(var=a_dict, var_type=dict, var_name='a_dict')

    def helper(b_dict, key):
        nonlocal values
        for k, v in b_dict.items():
            if isinstance(v, dict) and key != k:
                if recursive:
                    helper(v, key)
            elif key == k:
                if only_dict:
                    if isinstance(v, dict):
                        values.append(v)
                else:
                    values.append(v)

    values = []
    helper(a_dict, key)
    if not values:
        msg = "Key '{k}' not found,".format(k=key)
        if recursive:
            msg = ' '.join([msg, 'in recursive search.'])
        else:
            msg = ' '.join([msg, 'in NOT recursive search.'])
        raise KeyError(msg)
    return values
