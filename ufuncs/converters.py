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
