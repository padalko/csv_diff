import os
import sqlite3
from collections import defaultdict, OrderedDict
from operator import itemgetter

import utils


def dict_factory(cursor, row):
    return dict(zip([x[0] for x in cursor.description], row))


class SQLDiffError(Exception):
    pass


class Sqldiff(object):
    compare_qry = """
    SELECT '{tbl1_alias}' ROWID AS Diff, *
    FROM (SELECT * FROM {tbl_name} EXCEPT SELECT * FROM {db2_alias_name}.{tbl_name})
    UNION ALL
    SELECT '{tbl2_alias}' ROWID AS Diff, *
    FROM (SELECT * FROM {db2_alias_name}.{tbl_name} EXCEPT SELECT * FROM {tbl_name});
    """

    get_colnames = "pragma TABLE_INFO({t})"

    cmp_tables_qry = """
    SELECT  '{tbl1_alias} ' || id AS Diff, *
    FROM
      (SELECT {t1_cols}
         FROM {t1}
       EXCEPT
       SELECT {t2_cols}
         FROM {t2})
    UNION ALL
    SELECT  '{tbl2_alias} ' || id AS Diff, *
    FROM
      (SELECT {t2_cols}
         FROM {t2}
       EXCEPT
       SELECT {t1_cols}
        FROM {t1});
    """

    table_names_qry = """select tbl_name
                           from sqlite_master
                          where type = 'table' and name != 'sqlite_sequence'"""

    def __init__(self, **kwargs):
        self.totals = defaultdict(list)
        self.memory_mode = kwargs.get('memory_mode')
        self.memory_conn = kwargs.get('sqlite_conn')
        self.ignored_files = kwargs.get('ignored_files', '')
        self.table_alias_name = kwargs.get('table_alias_name', 'second')
        self.path_origin = kwargs.get('path_origin')
        self.path_cmp = kwargs.get('path_cmp')

        self.databases = self._get_dbs()
        self.tbl1_alias = kwargs.get('tbl1_alias', 'table1')
        self.tbl2_alias = kwargs.get('tbl1_alias', 'table2')

        self.tbl1 = kwargs.get('tbl1_name')
        self.tbl2 = kwargs.get('tbl2_name')

        self.diff_data = {}

        if self.memory_mode and not (self.memory_conn or self.tbl1 or self.tbl2):
            raise SQLDiffError('Please provide valid SQL Lite Conn and table names')
        elif not self.memory_mode and not (self.path_origin or self.path_cmp):
            raise SQLDiffError('Please provide folders containing *.db3 files to compare')

    def _attach_db(self, conn, path, alias_name):
        attach_qry = "attach database '{0}' as {1}".format(path, alias_name)
        if self.memory_mode:
            attach_qry = "attach database '{0}' as {1}".format(path, alias_name)
        conn.execute(attach_qry)

    def _get_dbs(self):
        databases = ':memory:'
        if not self.memory_mode:
            if all(map(lambda f: os.path.isfile(f) and f.split('.')[-1] == 'db3', (self.path_origin, self.path_cmp))):
                databases = (self.path_origin, self.path_cmp)
            else:
                try:
                    databases = [x for x in os.listdir(self.path_origin) if x.split('.')[-1] == 'db3']
                except Exception as e:
                    raise SQLDiffError('error in db names {!r}'.format(e))
        return databases

    def _get_tables(self, conn):
        c = conn.cursor()
        return map(lambda x: x[0], c.execute(self.table_names_qry))

    def get_diff_lines(self):
        if self.memory_mode:
            with self.memory_conn as conn:
                tables = set(self._get_tables(conn))
                if not {self.tbl1, self.tbl2}.issubset(tables):
                    raise SQLDiffError('Invalid tables names for compare')

                conn.row_factory = dict_factory
                c = conn.cursor()

                # prepare column names for query
                t1_cols = tuple(map(itemgetter('name'), c.execute(self.get_colnames.format(t=self.tbl1))))
                t2_cols = tuple(map(itemgetter('name'), c.execute(self.get_colnames.format(t=self.tbl2))))
                t1_cols, t2_cols = utils.align_cols(t1_cols, t2_cols)

                diff = c.execute(self.cmp_tables_qry.format(
                    tbl1_alias=self.tbl1,
                    tbl2_alias=self.tbl2,
                    t1=self.tbl1,
                    t2=self.tbl2,
                    t1_cols=','.join(t1_cols),
                    t2_cols=','.join(t2_cols),
                ))
                # todo add lambda compatibility here
                self.totals = utils.index(diff, ('id',))
        else:
            for db_name in self._get_dbs():
                if db_name in self.ignored_files: continue
                db1_path = os.path.join(self.path_origin, db_name)
                db2_path = os.path.join(self.path_cmp, db_name)
                with sqlite3.connect(db1_path) as conn:
                    self._attach_db(conn, db2_path, self.table_alias_name)
                    c = conn.cursor()
                    for table_name in self._get_tables(conn):
                        diff = c.execute(self.compare_qry.format(
                            tbl1_alias=self.tbl1_alias,
                            tbl2_alias=self.tbl2_alias,
                            db2_alias_name=self.table_alias_name,
                            tbl_name=table_name
                        )).fetchall()
                        if diff:
                            self.totals[db_name].append({table_name: diff})

        self.diff_data = dict(self.totals.items())

        return self.diff_data

    def get_diff_columns(self, data):
        d = defaultdict(list)
        od = OrderedDict
        for row_idx, vals in data.items():
            if len(vals) == 1:
                vals.append(dict.fromkeys(vals[0].keys()))
            row1, row2 = vals
            d[row_idx].append(od(sorted(row1.items() - row2.items(), key=itemgetter(0))))
            d[row_idx].append(od(sorted(row2.items() - row1.items(), key=itemgetter(0))))
        return d
