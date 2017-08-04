from lxml import etree
from lxml.builder import ElementMaker

import pandas as pd


def xml_to_dict(xml_tree):
    """Convert xml information into a dictionary.

    Get XML tags and text as keys and values in a dictionary. If the
    tag repeats the different texts/values will be concatenated using
    a smicolon (;). The tags are accumulated whilte transversing the
    tree by concatenating them by a '_'
    """
    all_info = {}

    def helper(el, tag=''):
        nonlocal all_info
        for e in el.getchildren():
            new_tag = re.sub('{.*}', '', e.tag).lower()
            new_tag = '_'.join([tag, new_tag]) if tag else new_tag
            text = e.text
            if text is not None:
                new_tag = clean_tag(new_tag)
                if new_tag in all_info:
                    all_info[new_tag] = ';'.join([all_info[new_tag], text])
                else:
                    all_info[new_tag] = text
            else:
                helper(e, new_tag)
    helper(xml_tree)
    return all_info


def _char_converter(string):
    """Converts special xml characters.

    The table below describes the behaviour of the function.

    | Symbol  | Character   |
    | --------|-------------|
    | _x0020_ | white Space |
    | _x0026_ | &           |
    | _x0027_ | '           |
    | _x0028_ | (           |
    | _x0029_ | )           |
    | _x0040_ | @           |
    | _x002F_ | /           |
    | _x003C_ | <           |
    | _x003E_ | >           |

    Args:
        string (str): A string.

    Returns:
        str: Parsed string.
    """
    string = (string.lower()
                    .replace('_x0020_', ' ')
                    .replace('_x0026_', "&")
                    .replace('_x0027_', "'")
                    .replace('_x0028_', "(")
                    .replace('_x0029_', ")")
                    .replace('_x0040_', "@")
                    .replace('_x002f_', '/')
                    .replace('_x003c_', '<')
                    .replace('_x003e_', '>')
                    .replace('_x002c_', ',')
                    .replace('_x0032_', '2')
                    .strip())
    return string


def xml2df(xml_string_or_path):
    """Convert xml file to a pandas dataframe.

    Args:
        xml_string_or_path (str): XML file path or its contents.

    Returns:
        pandas.DataFrame: Dataframe with the information in the xml
    """
    parser = etree.XMLParser(remove_blank_text=True,
                             ns_clean=True, encoding='utf-8')

    if xml_string_or_path.endswith('.xml'):
        tree = etree.parse(xml_string_or_path, parser)
    else:
        tree = etree.ElementTree(etree.fromstring(xml_string_or_path, parser))
    root = tree.getroot()
    pd_series = []
    for i, child in enumerate(root):
        columns = []
        values = []
        for subchild in child:
            columns.append(_char_converter(subchild.tag))
            text = subchild.text
            values.append(_char_converter(text) if text else text)
        pd_series.append(pd.Series(data=values, index=columns))
    df = pd.concat(pd_series, axis=1).T
    return df.dropna(how='all', axis=0).dropna(how='all', axis=1)
