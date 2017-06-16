from collection import Iterable

def enumerate_outer(collection, is_outer=True, index=-1):
    for item in collection:
        if is_outer:
            index += 1
        if isinstance(item, Iterable) and not isinstance(item, (str, bytes, bytearray)):
            yield from enumerate_outer(item, False, index)
        else:
            yield index, item
