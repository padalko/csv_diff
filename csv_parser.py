import csv
from collections import OrderedDict
from datetime import datetime
from decimal import DecimalException
from itertools import islice


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
    create_table_qry = """
    CREATE TABLE {tbl_name} 
    (line_id INTEGER PRIMARY KEY AUTOINCREMENT, 
    {col_types})"""

    insert_table_qry = """
    INSERT INTO {tbl_name} ({cols})
         VALUES ({values})
     """

    def __init__(self, filename, **kwargs):
        self.data = []
        self.filename = filename
        self.lines = ''
        self.headers = OrderedDict()
        self.convert_types = kwargs.get('convert_types', True)
        self.DEF_FORMAT = kwargs.get('default_format', 'str')
        self.read()

    def __len__(self):
        return len(self.data)

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
                    # todo make update for additional headers
                    # self.headers['file_name'] = 'str'
                    # self.headers['line_no'] = 'int'
                    self.data.append(line)
        except Exception as e:
            raise CSVReadError(
                'Error in parsing file: {fil}, please check file format: {err!r}'.format(fil=self.filename,
                                                                                         err=e))

    def serialize(self, conn, tbl_name):
        """creates load to SQL"""
        self._create_table(conn, tbl_name)
        self._load_table(conn, tbl_name)

    def _create_table(self, conn, tbl_name):
        cursor = conn.cursor()
        col_types = ', '.join(' '.join((col, typ)) for col, typ in self.headers.items())
        cursor.execute(self.create_table_qry.format(tbl_name=tbl_name, col_types=col_types))
        conn.commit()

    def _load_table(self, conn, tbl_name):
        cursor = conn.cursor()
        insert = self.insert_table_qry.format(tbl_name=tbl_name,
                                              cols=', '.join(self.headers.keys()),
                                              values=', '.join(':{0}'.format(x) for x in self.headers.keys()))
        cursor.executemany(insert, self.data)
        conn.commit()


if __name__ == '__main__':
    args = {
        'filename1': 'samples/left.csv',
        'filename2': 'samples/right.csv',
    }
    f1 = args.get('filename1')
    f2 = args.get('filename2')

    f1_ = CsvReader(f1)
    f2_ = CsvReader(f2)
