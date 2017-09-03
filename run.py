import sqlite3

import jinja2

import utils
from csv_parser import CsvReader
from sql_diff import Sqldiff


def compare(**kwargs):
    kwargs = {
        'memory_mode': True,
        'filename1': 'samples/left.csv',
        'filename2': 'samples/right.csv',
        # 'path_origin': DB1_FOLDER_PATH,
        # 'path_cmp': DB2_FOLDER_PATH,
        # 'path_origin': DB1_PATH,
        # 'path_cmp': DB2_PATH,
        'ignored_files': ('sp_log.db3', 'sp_ncpdp_pharma.db3'),
        'tbl1_name': 'left_table',
        'tbl2_name': 'right_table',
    }
    f1 = kwargs.get('filename1')
    f2 = kwargs.get('filename2')

    file1 = CsvReader(f1)
    file2 = CsvReader(f2)
    with sqlite3.connect(":memory:") as con:
        print(utils.analyze_outputs(file1, file2))
        file1.serialize(con, 'left_table')
        file2.serialize(con, 'right_table')

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
            print(diff_dateline)
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
    options = {
        'memory_mode': True,
        'filename1': 'samples/left.csv',
        'filename2': 'samples/right.csv',
        # 'path_origin': DB1_FOLDER_PATH,
        # 'path_cmp': DB2_FOLDER_PATH,
        # 'path_origin': DB1_PATH,
        # 'path_cmp': DB2_PATH,
        'ignored_files': ('sp_log.db3', 'sp_ncpdp_pharma.db3'),
        'tbl1_name': 'left_table',
        'tbl2_name': 'right_table',
    }
    env = jinja2.Environment(loader=jinja2.FileSystemLoader('/home/padalko/dev/csv_diff/templates'))
    template = env.get_template('diff_table.jinja2')
    data = compare(**options)
    print(template.render(**data))
