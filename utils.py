from itertools import zip_longest, tee
from operator import itemgetter


def analyze_outputs(*files):
    diff_cols = set()
    for file in files:
        diff_cols.symmetric_difference_update(set(file.headers.keys()))
    d_cols_msg = 'Different column names was found: {0}.'.format(', '.join(diff_cols))
    d_lines_no_msg = 'Invalid lines number in provided files found!\n'
    check_lines = any(len(f) != len(files[0]) for f in files)
    if check_lines:
        for f in files:
            d_lines_no_msg += '{0} has {1} lines \n'.format(f.filename, len(f.data))

    d_head_msg = 'Invalid headers number in provided files found!\n'
    check_headers = any(len(f.headers) != len(files[0].headers) for f in files)
    if check_lines:
        for f in files:
            d_head_msg += '{0} has {1} headers \n'.format(f.filename, len(f.headers))

    validations = {
        d_head_msg: check_headers,
        d_lines_no_msg: check_lines,
        d_cols_msg: diff_cols
    }

    validation_errors = '\n'.join(msg for msg, v in validations.items() if v)
    return validation_errors


def index(dataList, indexKeys, sortKeys=None, reverseSort=False, uniqueValues=False):
    indexedData = {}

    if isinstance(indexKeys, itemgetter):
        MakeKey = indexKeys
    else:
        MakeKey = itemgetter(*indexKeys)

    # Build the index
    for rec in dataList:
        key = MakeKey(rec)
        if key in indexedData:
            if uniqueValues and rec in indexedData[key]:
                continue
            indexedData[key].append(rec)
        else:
            indexedData[key] = [rec]
            # Create list or append data

    # Sort the values by the sort key if provided
    if sortKeys:
        if isinstance(sortKeys, itemgetter):
            MakeKey = sortKeys
        else:
            MakeKey = itemgetter(*sortKeys)

        for v in indexedData.values():
            v.sort(key=MakeKey, reverse=reverseSort)

    return indexedData


def align_cols(l1, l2, fill='null'):
    ov = []
    alias = 'null as {col}'
    for l, r in zip_longest(l1, l2, fillvalue=fill):
        if l == r:
            ov.append((l, r))
        else:
            if l != fill:
                ov.append((l, alias.format(fill=fill, col=l)))
            if r != fill:
                ov.append((alias.format(fill=fill, col=r), r))
    return (tuple(map(itemgetter(x), tee(ov)[x])) for x in range(2))
