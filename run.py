import argparse
import sqlite3

import utils
from csv_parser import CsvReader
from sql_diff import Sqldiff


def compare(**kwargs):
    # pprint(kwargs)
    # temporary hardocoded
    if not kwargs:
        kwargs = {'memory_mode': True, 'filename1': 'samples/left.csv', 'filename2': 'samples/right.csv',
                  'tbl1_name': 'left',
                  'tbl2_name': 'right'}

    f1 = kwargs.get('filename1')
    f2 = kwargs.get('filename2')
    tbl_name1 = kwargs.get('tbl1_name')
    tbl_name2 = kwargs.get('tbl2_name')
    file1 = CsvReader(f1)
    file2 = CsvReader(f2)
    with sqlite3.connect(":memory:") as con:
        print(utils.analyze_outputs(file1, file2))
        file1.serialize(con, tbl_name1)
        file2.serialize(con, tbl_name2)

        obj = Sqldiff(**kwargs, sqlite_conn=con)
        diff_lines = obj.get_diff_lines()
        diff_cols = obj.get_diff_columns(diff_lines)

    # prepare data for visualization
    df2, headers = {}, {}
    for line_id, diff_data in diff_lines.items():
        if not headers:
            headers = diff_data[0].keys() if diff_data else {}
        new_data = []
        for diff_dateline in diff_data:
            styled_data = []
            # print(diff_dateline)
            for colname, val in diff_dateline.items():
                styled_data.append({
                    'name': colname,
                    'val': val,
                    'class': 'danger' if colname in diff_cols.get(line_id, [{}])[0].keys() else ''
                })
            new_data.append(styled_data)
        df2[str(line_id)] = new_data

    return dict(data=df2, headers=headers)


if __name__ == '__main__':
    import os
    import time
    import sys
    import jinja2
    import webbrowser

    parser = argparse.ArgumentParser(description='CSV diff tool. Provide path to files')

    parser.add_argument('-f', '--files', required=True, type=str, default=[], nargs='+',
                        help='space delimited files list')
    parser.add_argument('-t', '--tables', required=False, type=str, default=[], nargs='*',
                        help='space delimited files list')
    parser.add_argument('-m', '--mode', type=str, default='memory',
                        help='Source mode to compare files (memory[default] or database)')
    parser.add_argument('-i', '--ignored', type=str, default=[], nargs='*',
                        help='space delimited list of ignored files')
    parser.add_argument('-p', '--path', type=str,
                        help='Path to database containing folder. For DB mode only')
    args = parser.parse_args()

    options = {
        'memory_mode': args.mode == 'memory'
    }
    for n, fname in enumerate(args.files, 1):
        if not os.path.isfile(fname):
            print('{0} is not valid filename. Exit'.format(fname))
            sys.exit(1)
        options['filename{}'.format(n)] = fname
    for n, t_name in enumerate(args.tables or args.files, 1):
        options['tbl{}_name'.format(n)] = '{f}'.format(f=os.path.basename(t_name).split('.')[0])
    # print(options)

    # test launch
    from app import TEMPLATES_DIR
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATES_DIR))
    template = env.get_template('diff_table.jinja2')
    data = compare(**options)
    html_diff = template.render(**data)

    call_dir = os.path.abspath(os.curdir)
    html_f_name = 'diff_{tbl1_name}_{tbl2_name}.html'.format(**options)
    outfile_full_path = os.path.join(call_dir, html_f_name)

    if os.path.exists(outfile_full_path):
        html_f_name = '.'.join((html_f_name.split('.')[0] + str(int(time.time())), 'html'))
    with open(html_f_name, 'w') as outfile:
        outfile.write(html_diff)

    webbrowser.open('file://' + outfile_full_path)