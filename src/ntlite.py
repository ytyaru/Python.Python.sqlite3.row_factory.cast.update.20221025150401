import sqlite3
from collections import namedtuple
import dataclasses
import inspect
import importlib  
AwareDateTime = importlib.import_module('aware-date-time').AwareDateTime 
RowTypes = namedtuple('RowTypes', 'tuple row namedtuple dataclass', defaults=(0,1,2,3))()
class RowType: pass
RowType.row_factory = None
class TupleRowType(RowType): pass
class Sqlite3RowType(RowType): pass
Sqlite3RowType.row_factory = sqlite3.Row
class NamedTupleRowType(RowType):
    def __init__(self, not_getitem=False): self._not_getitem = not_getitem
    def row_factory(self, cursor, row):
        row_type = self.new_row_type(cursor)
        if not self._not_getitem: row_type = self.set_getitem(row_type)
        return row_type(*row)
    def new_row_type(self, cursor): return namedtuple('Row', list(map(lambda d: d[0], cursor.description)))
    def set_getitem(self, row_type):
        def getitem(self, key):
            if isinstance(key, str): return getattr(self, key)
            else: return super(type(self), self).__getitem__(key)
        row_type.__getitem__ = getitem
        return row_type
class DataClassRowType(RowType):
    def __init__(self, not_getitem=False, not_slots=False, not_frozen=False):
        self._not_getitem = not_getitem
        self._not_slots = not_slots
        self._not_frozen = not_frozen
    def row_factory(self, cursor, row):
        row_type = self.new_row_type(cursor)
        if not self._not_getitem: row_type = self.set_getitem(row_type)
        return row_type(*row)
    def new_row_type(self, cursor):
        return dataclasses.make_dataclass('Row', list(tuple(map(lambda d: d[0], cursor.description))), slots=not self._not_slots, frozen=not self._not_frozen)

    def set_getitem(self, row_type):
        def getitem(self, key):
            if isinstance(key, str): return getattr(self, key)
            elif isinstance(key, int): return getattr(self, list(self.__annotations__.keys())[key])
            else: raise TypeError('The key should be int or str type.')
        row_type.__getitem__ = getitem
        return row_type
# NtLiteのコンストラクタ引数row_typeにセットする値はこのRowTypesが持ついずれかのプロパティを渡す
RowTypes = namedtuple('RowTypes', 'tuple sqlite3 namedtuple dataclass', defaults=(TupleRowType, Sqlite3RowType, NamedTupleRowType, DataClassRowType))()
NOT_USE = namedtuple('NotUse', '')()
class NtLite:
    def __init__(self, path=':memory:', row_type:RowTypes=RowTypes.namedtuple):
        self._path = path
        self._row_type = row_type
        self.RowType = row_type
        self._con = sqlite3.connect(path)
        self._set_row_factory()
        self._cur = self._con.cursor()
    def __del__(self): self._con.close()
    def table_names(self): return tuple([row.name for row in self.gets("select name from sqlite_master where type='table';")])
    def column_names(self, table_name): return tuple([row.name for row in self.table_info(table_name)])
    def table_info(self, table_name): return self.gets(f"PRAGMA table_info('{table_name}');")
    def table_xinfo(self, table_name): return self.gets(f"PRAGMA table_xinfo('{table_name}');")
    def exec(self, sql, params=()): return self.con.execute(sql, params)
    def execm(self, sql, params=()): return self.con.executemany(sql, params)
    def execs(self, sql): return self.con.executescript(sql)
    def get(self, sql, params=()): return self.exec(sql, params).fetchone()
    def gets(self, sql, params=()): return self.exec(sql, params).fetchall()
    def _cast_exec(self, sql, params): return self.exec(sql, CastPy.to_sql_by_row(params))
    def _cast_execm(self, sql, params): return self.execm(sql, CastPy.to_sql_by_rows(params))
    def _insert_sql(self, table_name, params): return f"insert into {table_name} values ({','.join('?' * len(params))})"
    def insert(self, table_name, params): return self._cast_exec(self._insert_sql(table_name, params), params)
    def inserts(self, table_name, params): return self._cast_execm(self._insert_sql(table_name, params), params)
    def get_row(self, table_name):
        if table_name not in self.table_names(): raise ValueError('存在するテーブル名を指定してください。')
        cols = self.column_names(table_name)
        #typ = namedtuple('Row', cols)
        #return namedtuple(table_name, cols)(*([NOT_USE] * len(cols)))
        return namedtuple(table_name, cols, defaults=([NOT_USE] * len(cols)))
    def _update_sql_vals(self, row, where=None): #row,whereはget_row()の戻り値で得た型のインスタンスであること
        if isinstance(row, type): raise ValueError('引数rowは型でなくインスタンスを指定してください。')
        def is_update_id(): return hasattr(row, 'id') and hasattr(where, 'id') and row.id != where.id
        def get_target_cols_kv(r):
            d = r._asdict()
            if not is_update_id(): del d['id']
            return d.items()
        def get_vals(r): return [getattr(r, k) for k,v in get_target_cols_kv(r) if v != NOT_USE]
        def get_preperds(r): return [f'{k}=?' for k,v in get_target_cols_kv(r) if v != NOT_USE]
        def set_sql(r): return ', '.join(get_preperds(r))
        def where_sql(r):
            where_sql = 'where id=?'
            if where is None: #更新するレコードを一意に特定する必要があります。引数whereがNoneのときはrowにid列と値を指定してください。id列がないテーブルのときは一意に特定できる列と値をget_row()で得た型のインスタンスで与えてください。
                if 'id' not in row._fields: raise ValueError('引数whereがNoneのときはrowにid列を指定してください。')
                if NOT_USE == row.id: raise ValueError('引数whereがNoneのときはrowにid列とその値を指定してください。')
            else:
                if isinstance(where, type): raise ValueError('引数whereは型でなくインスタンスを指定してください。')
                else: where_sql = 'where ' + ' and '.join(get_preperds(where))
            return where_sql
        row_preperd = get_vals(row)
        get_vals(row) if is_update_id() else get_vals(row) 
        table_name = row.__class__.__name__
        set_sql = set_sql(row)
        if 0 == len(set_sql): raise ValueError('引数rowに更新するデータをセットしてください。')
        return f"update {table_name} set {set_sql} {where_sql(row)};", tuple(get_vals(row) + (get_vals(where) if where else [row.id]))
        #return f"update {table_name} set {set_sql} {where_sql(row)};", CastPy.to_sql_by_row(tuple(get_vals(row) + (get_vals(where) if where else [row.id])))
    def update(self, table_name, value, where=None): return self._cast_exec(*self._update_sql_vals(table_name, value, where))
    def commit(self): return self.con.commit()
    def rollback(self): return self.con.rollback()
    @property
    def con(self): return self._con
    @property
    def cur(self): return self._cur
    @property
    def path(self): return self._path
    @property
    def RowType(self): return self._row_type
    @RowType.setter
    def RowType(self, v):
        if inspect.isclass(v):
            if issubclass(v, RowType):
                self._row_type = v() # 型が渡されたらデフォルトコンストラクタで生成したインスタンスをセットする
                return
        self._row_type = v if issubclass(type(v), RowType) else NamedTupleRowType()
    def _set_row_factory(self):
        self._con.row_factory = self._row_type.row_factory if issubclass(type(self._row_type), RowType) else NamedTupleRowType().row_factory

import re
from datetime import datetime
class CastPy:
    @classmethod
    def to_sql(cls, v):
        if isinstance(v, bool): return 1 if v else 0
        #elif isinstance(v, datetime): return f"{v:%Y-%m-%d %H:%M:%S}"
        elif isinstance(v, datetime): return f"{AwareDateTime.to_utc(AwareDateTime.if_native_to_local(v)):%Y-%m-%d %H:%M:%S}"
        else: return v
    @classmethod
    def to_sql_by_row(cls, row):
        if isinstance(row, tuple): return tuple([cls.to_sql(col) for col in row])
        else: return row
    @classmethod
    def to_sql_by_rows(cls, rows):
        if isinstance(rows, list): return [cls.to_sql_by_row(row) for row in rows]
        else: return rows

#create table文で定義した型名で型チェックしたい。もし対応する型でないPython型の値がセットされたら例外発生させたい
#* SQLite3で定義されている型名である（型チェックする）
#* 今回このツールでキャストする型名である（型チェックする）
#* 上記以外である（型チェックしない）

