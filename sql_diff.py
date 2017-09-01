import os
import sqlite3
from collections import defaultdict
from itertools import zip_longest, tee
from operator import itemgetter


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
                # ov =
    return (tuple(map(itemgetter(x), tee(ov)[x])) for x in range(2))


def DictFactory(cursor, row):
    return dict(zip([x[0] for x in cursor.description], row))


def MakeIndex(dataList, indexKeys, sortKeys=None, reverseSort=False, uniqueValues=False):
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

    get_cols_qry = "pragma TABLE_INFO({tbl_name})"

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
        self.memory_mode = kwargs.get('memory_mode')
        self.memory_conn = kwargs.get('sqlite_conn')
        self.ignored_files = kwargs.get('ignored_files', '')
        self.table_alias_name = kwargs.get('table_alias_name', 'second')
        self.totals = defaultdict(list)
        self.path_origin = kwargs.get('path_origin')
        self.path_cmp = kwargs.get('path_cmp')

        self.databases = self._get_dbs()
        self.tbl1_alias = kwargs.get('tbl1_alias', 'table1')
        self.tbl2_alias = kwargs.get('tbl1_alias', 'table2')

        self.tbl1 = kwargs.get('tbl1_name')
        self.tbl2 = kwargs.get('tbl2_name')

        if not self.memory_mode and not (self.path_origin or self.path_cmp):
            raise SQLDiffError('Please provide folders containing *.db3 files to compare')
        elif self.memory_mode and not (self.memory_conn or self.tbl1 or self.tbl2):
            raise SQLDiffError('Please provide valid SQL Lite Conn')

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

    def make_diff(self, **kwargs):
        if not self.memory_mode:
            for db_name in self._get_dbs():
                if db_name in self.ignored_files: continue
                print(db_name, 'START')
                db1_path = os.path.join(self.path_origin, db_name)
                db2_path = os.path.join(self.path_cmp, db_name)
                with sqlite3.connect(db1_path) as conn:
                    self._attach_db(conn, db2_path, self.table_alias_name)
                    c = conn.cursor()
                    for table_name in self._get_tables(conn):
                        # print(table_name)
                        diff = c.execute(self.compare_qry.format(
                            tbl1_alias=self.tbl1_alias,
                            tbl2_alias=self.tbl2_alias,
                            db2_alias_name=self.table_alias_name,
                            tbl_name=table_name
                        )).fetchall()
                        if diff:
                            self.totals[db_name].append({table_name: diff})
                print(db_name, 'END')
        else:
            conn = self.memory_conn
            tables = set(self._get_tables(conn))
            if not {self.tbl1, self.tbl2}.issubset(tables):
                raise SQLDiffError('Invalid tables names for compare')
            conn.row_factory = DictFactory
            c = conn.cursor()

            t1_cols = map(itemgetter('name'), c.execute(self.get_cols_qry.format(tbl_name=self.tbl1)).fetchall())
            t2_cols = map(itemgetter('name'), c.execute(self.get_cols_qry.format(tbl_name=self.tbl2)).fetchall())

            t1_cols, t2_cols = align_cols(t1_cols, t2_cols)

            print(self.cmp_tables_qry.format(
                tbl1_alias=self.tbl1,
                tbl2_alias=self.tbl2,
                t1=self.tbl1,
                t2=self.tbl2,
                t1_cols=','.join(t1_cols),
                t2_cols=','.join(t2_cols),
            ))

            diff = c.execute(self.cmp_tables_qry.format(
                tbl1_alias=self.tbl1,
                tbl2_alias=self.tbl2,
                t1=self.tbl1,
                t2=self.tbl2,
                t1_cols=','.join(t1_cols),
                t2_cols=','.join(t2_cols),
            )).fetchall()
            if diff:
                key = '{0}__vs__{1}'.format(self.tbl1, self.tbl2)
                indexed = MakeIndex(diff, ('id',))
                self.totals = indexed
            print('memory', 'END')

        return dict(self.totals.items())
