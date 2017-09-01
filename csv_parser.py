import csv
from collections import OrderedDict
from datetime import datetime
from itertools import islice
from decimal import Decimal, DecimalException


class CSVReadError(Exception):
    """generic exception"""
    pass


class CsvReader(object):
    """Reader for ONLY ONE file"""
    TYPE_DEF_FUNCTIONS = (
        ('date', lambda x: datetime.strptime(x, "%Y-%m-%d")),
        ('datetime', lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")),
        ('float', lambda x: float(x.replace(',', '.'))),
        # ('decimal', lambda x: Decimal(str(x))),
        ('str', lambda x: str(x) if isinstance(x, str) else x)
    )

    def __init__(self, filename, **kwargs):
        self.data = []
        self.filename = filename
        self.lines = ''
        self.headers = OrderedDict()
        self.convert_types = kwargs.get('convert_types', True)
        self.DEF_FORMAT = kwargs.get('default_format', 'str')

    def _read_headers(self):
        for h in islice(self.lines, 1).__next__():
            self.headers[h] = self.DEF_FORMAT

    def _parse(self, line, parse_header_types=False):
        parsed = []
        header_keys = list(self.headers.keys())

        for col_idx, col_val in enumerate(line):
            for type_name, func in self.TYPE_DEF_FUNCTIONS:
                try:
                    new_val = func(col_val)
                except (ValueError, DecimalException):
                    continue
                else:
                    if parse_header_types:
                        self.headers[header_keys[col_idx]] = type_name
                    parsed.append(new_val)
                    break
            else:
                raise CSVReadError('Something went wrong during types recognition')
        return parsed

    def extract_line_data(self, line_data):
        """maps lines values according to header types"""
        if self.convert_types:
            line_data = self._parse(line_data)
        od = OrderedDict()
        od.update(zip(self.headers.keys(), line_data))
        return od

    def recognize_types(self, line):
        """Parse Header Type depending the first line values"""
        self._parse(line, parse_header_types=True)

    def read(self):
        """provided file"""
        try:
            with open(self.filename) as infile:
                self.lines = csv.reader(infile, delimiter=',')
                self._read_headers()
                for idx, line in enumerate(self.lines, 1):
                    if idx == 1:
                        # perform types recognition here
                        self.recognize_types(line)
                    line = self.extract_line_data(line)
                    # transform_data
                    line['file_name'] = self.filename
                    line['line_no'] = idx
                    self.data.append(line)
        except Exception as e:
            raise CSVReadError('Error in parsing file: {fil}, please check file format: {err!r}'.format(fil=self.filename,
                                                                                                        err=e))

    def serialize(self, conn, tbl_name):
        """creates load to SQL"""
        self._create_table(conn, tbl_name)
        self._load_table(conn, tbl_name)

    def _create_table(self, conn, tbl_name):
        cursor = conn.cursor()
        col_types = ', '.join(' '.join((col, typ)) for col, typ in self.headers.items())
        create_STMT = """CREATE TABLE {tbl_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {col_types})""".format(
            tbl_name=tbl_name, col_types=col_types)
        cursor.execute(create_STMT)
        conn.commit()

    def _load_table(self, conn, tbl_name):
        cursor = conn.cursor()
        insert_STMT = """
        INSERT INTO {tbl_name} ({cols})
             VALUES ({values})
         """.format(tbl_name=tbl_name, cols=', '.join(self.headers.keys()),
                    values=', '.join(':{0}'.format(x) for x in self.headers.keys()))
        # print(insert_STMT)
        cursor.executemany(insert_STMT, self.data)
        conn.commit()
        print(cursor.execute("select * from {}".format(tbl_name)).fetchall())


if __name__ == '__main__':
    args = {
        'filename1': 'samples/left.csv',
        'filename2': 'samples/right.csv',
    }
    f1 = args.get('filename1')
    f2 = args.get('filename2')

    f1_ = CsvReader(f1)
    f2_ = CsvReader(f2)
    f1_.read()
    f2_.read()

    def analyze_outputs(*files):
            # headers1 = kwargs.get('headers1')
            # headers2 = kwargs.get('headers2')
            # lines1 = kwargs.get('data1')
            # lines2 = kwargs.get('data2')
            diff_cols = set()
            for file in files:
                diff_cols.symmetric_difference_update(set(file.headers.keys()))
            # d_head_msg = 'Found different headers in provided files! {f1q} in {f1} against {f2q} in {f2}.'.format(
            #     f1q=len(head1),
            #     f2q=len(head2),
            #     f1=f1,
            #     f2=f2,
            #     diff_cols=', '.join(diff_cols))
            # d_lines_no_msg = 'Invalid lines number! {1} in "{0}" against {3} in "{2}"'.format(f1, len(lines1), f2, len(lines2))
            d_cols_msg = 'Different column names was found: {0}.'.format(', '.join(diff_cols))
            #
            # # #validations
            validations = {
                # d_head_msg: len(headers1) != len(headers2),
                # d_lines_no_msg: len(lines1) != len(lines2),
                d_cols_msg: diff_cols
            }
            validation_errors = '\n'.join(msg for msg, v in validations.items() if v)
            print(validation_errors)

    import sqlite3
    with sqlite3.connect(":memory:") as con:
        f1_.serialize(con, 'left_table')
        f2_.serialize(con, 'right_table')

        analyze_outputs(f1_, f2_)
    #
    #     options = {
    #         'memory_mode': True,
    #         # 'path_origin': DB1_FOLDER_PATH,
    #         # 'path_cmp': DB2_FOLDER_PATH,
    #         # 'path_origin': DB1_PATH,
    #         # 'path_cmp': DB2_PATH,
    #         'ignored_files': ('sp_log.db3', 'sp_ncpdp_pharma.db3'),
    #         'tbl1_name': 'left_table',
    #         'tbl2_name': 'right_table',
    #         'sqlite_conn': con
    #     }
    #
    #     obj = Sqldiff(**options)
    #     diff = obj.make_diff()
    #
    #
    #     def cmp(data):
    #         d = defaultdict(list)
    #         od = OrderedDict
    #         for row_idx, vals in data.items():
    #             if len(vals) == 1:
    #                 vals.append(dict.fromkeys(vals[0].keys()))
    #             row1, row2 = vals
    #             d[row_idx].append(od(sorted(row1.items() - row2.items(), key=itemgetter(0))))
    #             d[row_idx].append(od(sorted(row2.items() - row1.items(), key=itemgetter(0))))
    #         return d
    #
    #
    #     rest = cmp(diff)
    #     for line_no, data in rest.items():
    #         print(line_no)
    #         print(data[0].keys())
    #         print(data[1].keys())