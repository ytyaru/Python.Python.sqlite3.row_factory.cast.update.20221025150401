#!/usr/bin/env python3
# coding: utf8
import unittest
import os
from collections import namedtuple
import dataclasses 
from dataclasses import dataclass, field, Field
from decimal import Decimal
from datetime import datetime, date, time, timezone, timedelta
from ntlite import NtLite, RowTypes, RowType, TupleRowType, Sqlite3RowType, NamedTupleRowType, DataClassRowType, NOT_USE
class TestNtLite(unittest.TestCase):
    def setUp(self): pass
    def tearDown(self): pass
    def test_rowtypes(self):
        self.assertEqual(TupleRowType, RowTypes.tuple)
        self.assertEqual(Sqlite3RowType, RowTypes.sqlite3)
        self.assertEqual(NamedTupleRowType, RowTypes.namedtuple)
        self.assertEqual(DataClassRowType, RowTypes.dataclass)
    def test_init_args_0(self):
        db = NtLite()
        self.assertEqual(':memory:', db.path)
        self.assertTrue(db.con)
        self.assertTrue(db.cur)
        self.assertEqual(NamedTupleRowType, type(db._row_type))
        self.assertEqual(RowTypes.namedtuple, type(db._row_type))
    def test_init_path(self):
        path = 'my.db'
        if os.path.isfile(path): os.remove(path)
        db = NtLite(path)
        self.assertEqual(path, db.path)
        self.assertTrue(os.path.isfile(path))
        self.assertEqual(NamedTupleRowType, type(db._row_type))
        self.assertEqual(RowTypes.namedtuple, type(db._row_type))
        if os.path.isfile(path): os.remove(path)
    # row_type 型 テスト開始
    def test_init_row_type_none(self):
        db = NtLite(row_type=None)
        self.assertEqual(NamedTupleRowType, type(db._row_type))
        self.assertEqual(RowTypes.namedtuple, type(db._row_type))
        #self.assertEqual(None, db.row_factory)
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(('id','name'), row._fields) # _fieldsはnamedtuple固有なので型判定に使った
    # row_type RowTypes テスト開始
    def test_init_row_type_tuple(self):
        db = NtLite(row_type=RowTypes.tuple)
        self.assertEqual(TupleRowType, type(db._row_type))
        self.assertEqual(RowTypes.tuple, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(tuple, type(row))
    def test_init_row_type_sqlite_row(self):
        db = NtLite(row_type=RowTypes.sqlite3)
        self.assertEqual(Sqlite3RowType, type(db._row_type))
        self.assertEqual(RowTypes.sqlite3, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(['id','name'], row.keys()) # keys()はsqlite3.Row固有なので型判定に使った
    def test_init_row_type_namedtuple(self):
        db = NtLite(row_type=RowTypes.namedtuple)
        self.assertEqual(NamedTupleRowType, type(db._row_type))
        self.assertEqual(RowTypes.namedtuple, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(('id','name'), row._fields) # _fieldsはnamedtuple固有なので型判定に使った
    def test_init_row_type_dataclass(self):
        db = NtLite(row_type=RowTypes.dataclass)
        self.assertEqual(DataClassRowType, type(db._row_type))
        self.assertEqual(RowTypes.dataclass, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(['id','name'], list(row.__dataclass_fields__.keys())) # dataclass固有なので型判定に使った
    # row_type 型 テスト開始
    def test_init_row_type_row_type_from_type(self):
        db = NtLite(row_type=RowType)
        self.assertEqual(RowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(tuple, type(row)) # row_typeがNoneやTupleRowType()の時と同じ結果
        self.assertEqual(0, row[0])
        self.assertEqual('A', row[1])
    def test_init_row_type_sqlite_row_from_type(self):
        db = NtLite(row_type=Sqlite3RowType)
        self.assertEqual(Sqlite3RowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(['id','name'], row.keys()) # keys()はsqlite3.Row固有なので型判定に使った
    def test_init_row_type_namedtuple_from_type(self):
        db = NtLite(row_type=NamedTupleRowType)
        self.assertEqual(NamedTupleRowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(('id','name'), row._fields) # _fieldsはnamedtuple固有なので型判定に使った
    def test_init_row_type_dataclass_from_type(self):
        db = NtLite(row_type=DataClassRowType)
        self.assertEqual(DataClassRowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(['id','name'], list(row.__dataclass_fields__.keys())) # dataclass固有なので型判定に使った
    def test_init_row_type_from_type_another_class(self): # RowType継承クラス以外の型が渡されたらNamedTupleRowType
        class C: pass
        db = NtLite(row_type=C)
        self.assertEqual(NamedTupleRowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(('id','name'), row._fields) # _fieldsはnamedtuple固有なので型判定に使った
    # row_type インスタンス テスト開始
    def test_init_row_type_row_type_instance(self):
        db = NtLite(row_type=RowType())
        self.assertEqual(RowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(tuple, type(row)) # row_typeがNoneやTupleRowType()の時と同じ結果
    def test_init_row_type_tuple_instance(self):
        db = NtLite(row_type=TupleRowType())
        self.assertEqual(TupleRowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(tuple, type(row))
    def test_init_row_type_sqlite_row_instance(self):
        db = NtLite(row_type=Sqlite3RowType())
        self.assertEqual(Sqlite3RowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(['id','name'], row.keys()) # keys()はsqlite3.Row固有なので型判定に使った
    def test_init_row_type_namedtuple_instance(self):
        db = NtLite(row_type=NamedTupleRowType())
        self.assertEqual(NamedTupleRowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(('id','name'), row._fields) # _fieldsはnamedtuple固有なので型判定に使った
    def test_init_row_type_dataclass_instance(self):
        db = NtLite(row_type=DataClassRowType())
        self.assertEqual(DataClassRowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(['id','name'], list(row.__dataclass_fields__.keys())) # dataclass固有なので型判定に使った
    def test_init_row_type_another_instance(self): # RowType継承クラス以外の型が渡されたらNamedTupleRowType
        class C: pass
        db = NtLite(row_type=C)
        self.assertEqual(NamedTupleRowType, type(db._row_type))
        db.exec("create table users(id integer, name text, age integer);")
        db.exec("insert into users values(?,?,?);", (0,'A',7))
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(('id','name'), row._fields) # _fieldsはnamedtuple固有なので型判定に使った
    # row_type テスト終了
    def test_exec(self):
        db = NtLite()
        res = db.exec("create table users(id integer, name text, age integer);")
        self.assertEqual(None, res.fetchone())
        self.assertEqual([], res.fetchall())
    def test_exec_fetch_rowtype(self):
        db = NtLite(row_type=RowType)
        res = db.exec("create table users(id integer, name text, age integer);")
        row = db.exec("select count(*) from users;").fetchone() # [0]で参照できる。列名不要のためcount(*)という名前でエラーにならず
        self.assertEqual(0, row[0])
    def test_exec_fetch_tuple(self):
        db = NtLite(row_type=TupleRowType)
        res = db.exec("create table users(id integer, name text, age integer);")
        row = db.exec("select count(*) from users;").fetchone() # [0]で参照できる。列名不要のためcount(*)という名前でエラーにならず
        self.assertEqual(0, row[0])
    def test_exec_fetch_sqlite3(self):
        db = NtLite(row_type=Sqlite3RowType)
        res = db.exec("create table users(id integer, name text, age integer);")
        row = db.exec("select count(*) from users;").fetchone() # 列名は文字列型のためcount(*)という名前でエラーにならず
        self.assertEqual(0, row[0])
        self.assertEqual(0, row['count(*)'])
    def test_exec_fetch_error_namedtuple(self):
        db = NtLite(row_type=NamedTupleRowType)
        res = db.exec("create table users(id integer, name text, age integer);")
        with self.assertRaises(ValueError) as cm:
            db.exec("select count(*) from users;").fetchone() # 列名は[_a-zA-Z][_a-zA-Z0-9]*の文字でないとエラーになる
        self.assertEqual(cm.exception.args[0], "Type names and field names must be valid identifiers: 'count(*)'")
    def test_exec_fetch_error_dataclass(self):
        db = NtLite(row_type=DataClassRowType)
        res = db.exec("create table users(id integer, name text, age integer);")
        with self.assertRaises(TypeError) as cm:
            db.exec("select count(*) from users;").fetchone() # 列名は[_a-zA-Z][_a-zA-Z0-9]*の文字でないとエラーになる
        self.assertEqual(cm.exception.args[0], "Field names must be valid identifiers: 'count(*)'")
    def test_exec_rename_col(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        res = db.exec("select count(*) num from users;").fetchone()
        self.assertEqual(0, res.num)
    def test_execm(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        self.assertEqual(2, db.exec("select count(*) num from users;").fetchone().num)
        db.con.commit()
    def test_execs(self):
        db = NtLite()
        sql = """
begin;
create table users(id integer, name text, age integer);
insert into users values(0,'A',7);
insert into users values(1,'B',8);
commit;
"""
        db.execs(sql)
        self.assertEqual(2, db.exec("select count(*) num from users;").fetchone().num)
    def test_get(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        self.assertEqual(2, db.get("select count(*) num from users;").num)
    def test_get_preperd(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        self.assertEqual('A', db.get("select name from users where id=?;", (0,)).name)
    def test_gets(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        rows = db.gets("select name num from users order by name asc;")
        self.assertEqual('A', rows[0].num)
        self.assertEqual('B', rows[1].num)
    def test_gets_preperd(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8),(2,'C',6)])
        rows = db.gets("select name from users where age < ? order by name asc;", (8,))
        self.assertEqual('A', rows[0].name)
        self.assertEqual('C', rows[1].name)
    def test_name_lower_case(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        self.assertEqual('A', db.get("select NAME from users where id=?;", (0,)).name)
    def test_getitem(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        self.assertEqual('A', db.get("select name from users where id=?;", (0,))['name'])
    def test_all_fields_from_row_by_sqlite3(self):
        db = NtLite(row_type=Sqlite3RowType)
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select * from users where id=?;", (0,))
        self.assertEqual(('id','name','age'), tuple(row.keys()))
    def test_all_fields_from_row_by_namedtuple(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select * from users where id=?;", (0,))
        self.assertEqual(('id','name','age'), row._fields)
    def test_all_fields_from_row_by_namedtuple(self):
        db = NtLite(row_type=DataClassRowType)
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select * from users where id=?;", (0,))
        self.assertEqual(('id','name','age'), tuple(row.__annotations__.keys()))
    def test_get_expand_tuple_by_namedtuple(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select id, name from users where id=?;", (0,))
        id, name = row
        self.assertEqual(0, id)
        self.assertEqual('A', name)
    def test_get_to_dict_by_namedtuple(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual({'id':0, 'name':'A'}, row._asdict())
    def test_ref_by_namedtuple(self):
        db = NtLite()
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(0, row[0])
        self.assertEqual('A', row[1])
        self.assertEqual(0, row.id)
        self.assertEqual('A', row.name)
        self.assertEqual(0, row['id'])
        self.assertEqual('A', row['name'])
        with self.assertRaises(AttributeError) as cm: # 読取専用。イミュータブル。
            row.id = 999
        self.assertEqual(cm.exception.args[0], "can't set attribute")
    def test_ref_by_not_getitem_namedtuple(self):
        db = NtLite(row_type=RowTypes.namedtuple(not_getitem=True))
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(0, row[0])
        self.assertEqual('A', row[1])
        self.assertEqual(0, row.id)
        self.assertEqual('A', row.name)
        with self.assertRaises(TypeError) as cm:
            self.assertEqual(0, row['id'])
            self.assertEqual('A', row['name'])
        self.assertEqual(cm.exception.args[0], "tuple indices must be integers or slices, not str")
    def test_ref_by_dataclass(self):
        db = NtLite(row_type=DataClassRowType)
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(0, row[0])
        self.assertEqual('A', row[1])
        self.assertEqual(0, row.id)
        self.assertEqual('A', row.name)
        self.assertEqual(0, row['id'])
        self.assertEqual('A', row['name'])
        self.assertFalse(hasattr(row, '__dict__')) # slots=True
        with self.assertRaises(TypeError) as cm: # 新しいプロパティを作れない代わりに省メモリ
            row.some_prop = 'some_value'
        self.assertEqual(cm.exception.args[0], "super(type, obj): obj must be an instance or subtype of type")
        with self.assertRaises(dataclasses.FrozenInstanceError) as cm: # 読取専用。イミュータブル。
            row.id = 999
        self.assertEqual(cm.exception.args[0], "cannot assign to field 'id'")
    def test_ref_by_not_getitem_dataclass(self):
        db = NtLite(row_type=RowTypes.dataclass(not_getitem=True))
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(0, row.id)
        self.assertEqual('A', row.name)
        with self.assertRaises(TypeError) as cm: # [int]で参照できない。__getitem__がないから。
            self.assertEqual(0, row[0])
            self.assertEqual('A', row[1])
        self.assertEqual(cm.exception.args[0], "'Row' object is not subscriptable")
        with self.assertRaises(TypeError) as cm: # [str]で参照できない。__getitem__がないから。
            self.assertEqual(0, row['id'])
            self.assertEqual('A', row['name'])
        self.assertEqual(cm.exception.args[0], "'Row' object is not subscriptable")
        with self.assertRaises(TypeError) as cm: # 新しいプロパティを作れない代わりに省メモリ
            row.some_prop = 'some_value'
        self.assertEqual(cm.exception.args[0], "super(type, obj): obj must be an instance or subtype of type")
        with self.assertRaises(dataclasses.FrozenInstanceError) as cm: # 読取専用。イミュータブル。
            row.id = 999
        self.assertEqual(cm.exception.args[0], "cannot assign to field 'id'")
    def test_ref_by_not_slots_dataclass(self):
        db = NtLite(row_type=RowTypes.dataclass(not_slots=True))
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(0, row[0])
        self.assertEqual('A', row[1])
        self.assertEqual(0, row.id)
        self.assertEqual('A', row.name)
        self.assertEqual(0, row['id'])
        self.assertEqual('A', row['name'])
        self.assertTrue(hasattr(row, '__dict__')) # slots=False
        with self.assertRaises(dataclasses.FrozenInstanceError) as cm: # slotsでないので新しいプロパティを作れるがfrozenのためエラー
            row.some_prop = 'some_value'
        self.assertEqual(cm.exception.args[0], "cannot assign to field 'some_prop'")
        with self.assertRaises(dataclasses.FrozenInstanceError) as cm: # 読取専用。イミュータブル。
            row.id = 999
        self.assertEqual(cm.exception.args[0], "cannot assign to field 'id'")
    def test_ref_by_not_frozen_dataclass(self):
        db = NtLite(row_type=RowTypes.dataclass(not_frozen=True))
        db.exec("create table users(id integer, name text, age integer);")
        db.execm("insert into users values(?,?,?);", [(0,'A',7),(1,'B',8)])
        row = db.get("select id, name from users where id=?;", (0,))
        self.assertEqual(0, row[0])
        self.assertEqual('A', row[1])
        self.assertEqual(0, row.id)
        self.assertEqual('A', row.name)
        self.assertEqual(0, row['id'])
        self.assertEqual('A', row['name'])
        self.assertFalse(hasattr(row, '__dict__')) # slots=True
        with self.assertRaises(AttributeError) as cm: # flozenでないのにslotsだから新しいプロパティを作れないし省メモリでもない
            row.some_prop = 'some_value'
        self.assertEqual(cm.exception.args[0], "'Row' object has no attribute 'some_prop'")
        row.id = 999 # ミュータブル。frozenでないので代入可能
    def test_table_names(self):
        db = NtLite()
        names = db.table_names()
        #self.assertEqual([], names)
        self.assertEqual(tuple, type(names))
        self.assertEqual(0, len(names))
        db.exec("create table users(id integer, name text, age integer);")
        names = db.table_names()
        self.assertEqual(1, len(names))
        self.assertEqual(('users',), names)
        self.assertEqual('users', names[0])
    def test_column_names(self):
        db = NtLite()
        names = db.table_names()
        self.assertEqual((),names)
        db.exec("create table users(id integer, name text, age integer);")
        names = db.column_names('users')
        self.assertEqual(3, len(names))
        self.assertEqual(('id','name','age'), names)
    def test_table_info(self):
        db = NtLite()
        info = db.table_info('users')
        self.assertEqual([], info)
        db.exec("create table users(id integer primary key, name text not null, value real, birth datetime, img blob);")
        info = db.table_info('users')
        self.assertEqual(list, type(info))
        self.assertEqual(5, len(info))
        #self.assertEqual(tuple, type(info[0]))
        self.assertEqual([0,'id','INTEGER',0,None,1], [*info[0]])
        self.assertEqual([1,'name','TEXT',1,None,0], [*info[1]])
        self.assertEqual([2,'value','REAL',0,None,0], [*info[2]])
        self.assertEqual([3,'birth','datetime',0,None,0], [*info[3]])
        self.assertEqual([4,'img','BLOB',0,None,0], [*info[4]])
        self.assertEqual(0, info[0].cid)
        self.assertEqual('id', info[0].name)
        self.assertEqual('INTEGER', info[0].type)
        self.assertEqual(0, info[0].notnull)
        self.assertEqual(None, info[0].dflt_value)
        self.assertEqual(1, info[0].pk)
    def test_table_xinfo(self):
        db = NtLite()
        info = db.table_xinfo('users')
        self.assertEqual([], info)
        db.exec("create table users(id integer primary key, name text not null, value real, birth datetime, img blob);")
        info = db.table_xinfo('users')
        self.assertEqual(list, type(info))
        self.assertEqual(5, len(info))
        #self.assertEqual(tuple, type(info[0]))
        self.assertEqual([0,'id','INTEGER',0,None,1,0], [*info[0]])
        self.assertEqual([1,'name','TEXT',1,None,0,0], [*info[1]])
        self.assertEqual([2,'value','REAL',0,None,0,0], [*info[2]])
        self.assertEqual([3,'birth','datetime',0,None,0,0], [*info[3]])
        self.assertEqual([4,'img','BLOB',0,None,0,0], [*info[4]])
        self.assertEqual(0, info[0].cid)
        self.assertEqual('id', info[0].name)
        self.assertEqual('INTEGER', info[0].type)
        self.assertEqual(0, info[0].notnull)
        self.assertEqual(None, info[0].dflt_value)
        self.assertEqual(1, info[0].pk)
        self.assertEqual(0, info[0].hidden)
    def test_insert(self):
        db = NtLite()
        info = db.table_xinfo('users')
        self.assertEqual([], info)
        db.exec("create table users(id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        db.insert('users', (0, 'A', 0.1, bytes(2), True, datetime.fromisoformat('2000-01-01T00:00:00+00:00')))
        row = db.get('select * from users where id=0;')
        self.assertEqual(0, row.id)
        self.assertEqual('A', row.name)
        self.assertEqual(0.1, row.value)
        self.assertEqual(bytes(2), row.img)
        self.assertEqual(1, row.is_male)
        self.assertEqual('2000-01-01 00:00:00', row.birth)

    def test_get_row_error(self):
        db = NtLite()
        with self.assertRaises(ValueError) as e:
            db.get_row('nothing_table_name')
        self.assertEqual('存在するテーブル名を指定してください。', e.exception.args[0])
    def test_get_row(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name}(id integer, name text, age integer);")
        row = db.get_row(table_name)
        self.assertEqual('type', row.__class__.__name__)
        self.assertEqual(('id', 'name', 'age'), row._fields)
        self.assertEqual(table_name, row().__class__.__name__)
        self.assertEqual(NOT_USE, row().id)
        self.assertEqual(NOT_USE, row().name)
        self.assertEqual(NOT_USE, row().age)
        r = row(id=1, name='A', age=2)
        self.assertEqual(1, r.id)
        self.assertEqual('A', r.name)
        self.assertEqual(2, r.age)
        with self.assertRaises(AttributeError) as e: r.id = 2
        self.assertEqual("can't set attribute", e.exception.args[0])
    def test_update_sql_vals_error_row_type(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name}(id integer, name text, age integer);")
        row = db.get_row(table_name)
        with self.assertRaises(ValueError) as e: db._update_sql_vals(row)
        self.assertEqual("引数rowは型でなくインスタンスを指定してください。", e.exception.args[0])
    def test_update_sql_vals_error_where_type(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name}(id integer, name text, age integer);")
        row = db.get_row(table_name)
        #with self.assertRaises(ValueError) as e: db._update_sql_vals(row(), row)
        with self.assertRaises(ValueError) as e: db._update_sql_vals(row(id=0, name='A'), row)
        self.assertEqual("引数whereは型でなくインスタンスを指定してください。", e.exception.args[0])
    def test_update_sql_vals_error_row_not_set_update_datas(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name}(id integer, name text, age integer);")
        row = db.get_row(table_name)
        with self.assertRaises(ValueError) as e: db._update_sql_vals(row())
        self.assertEqual("引数rowに更新するデータをセットしてください。", e.exception.args[0])
    def test_update_sql_vals_id_only(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name}(id integer, name text, age integer);")
        row = db.get_row(table_name)
        with self.assertRaises(ValueError) as e: db._update_sql_vals(row())
        self.assertEqual("引数rowに更新するデータをセットしてください。", e.exception.args[0])
    def test_update_sql_vals_id_name(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        Row = db.get_row(table_name)
        sql, prepards = db._update_sql_vals(Row(id=0, name='A'))
        self.assertEqual(f'update {table_name} set name=? where id=?;', sql)
        self.assertEqual(('A',0), prepards)
    def test_update_sql_update_id(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        Row = db.get_row(table_name)
        sql, prepards = db._update_sql_vals(Row(id=0), Row(id=1))
        self.assertEqual(f'update {table_name} set id=? where id=?;', sql)
        self.assertEqual((0,1), prepards)
    def test_update_sql_vals_id_birth(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        Row = db.get_row(table_name)
        sql, prepards = db._update_sql_vals(Row(id=0, birth=datetime.fromisoformat('2000-01-01T00:00:00+00:00')))
        self.assertEqual(f'update {table_name} set birth=? where id=?;', sql)
        self.assertEqual((datetime.fromisoformat('2000-01-01 00:00:00+00:00'),0), prepards)
    def test_update_sql_vals_id_birth_native_is_local(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        Row = db.get_row(table_name)
        sql, prepards = db._update_sql_vals(Row(id=0, birth=datetime.fromisoformat('2000-01-01T00:00:00')))
        self.assertEqual(f'update {table_name} set birth=? where id=?;', sql)
        if datetime.now().astimezone().tzinfo == timezone(timedelta(seconds=32400)):
            self.assertEqual((datetime.fromisoformat('2000-01-01 00:00:00'),0), prepards)#まだキャスト前なのでローカル時解釈とUTC変換されない
            #self.assertEqual((datetime.fromisoformat('1999-12-31 15:00:00+00:00'),0), prepards)#まだキャスト前なので

    def test_update_sql_vals_id_birth_utc(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        Row = db.get_row(table_name)
        sql, prepards = db._update_sql_vals(Row(id=0, birth=datetime.fromisoformat('2000-01-01T00:00:00+00:00')))
        self.assertEqual(f'update {table_name} set birth=? where id=?;', sql)
        if datetime.now().astimezone().tzinfo == timezone(timedelta(seconds=32400)):
            self.assertEqual((datetime.fromisoformat('2000-01-01 00:00:00+00:00'),0), prepards)
    def test_update_sql_vals_id_birth_tokyo(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        Row = db.get_row(table_name)
        sql, prepards = db._update_sql_vals(Row(id=0, birth=datetime.fromisoformat('2000-01-01T00:00:00+09:00')))
        self.assertEqual(f'update {table_name} set birth=? where id=?;', sql)
        self.assertEqual((datetime.fromisoformat('2000-01-01T00:00:00+09:00'),0), prepards)
        #self.assertEqual(('1999-12-31 15:00:00',0), prepards)#まだキャスト前なので
    def test_update_sql_vals_id_is_male_false(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        Row = db.get_row(table_name)
        sql, prepards = db._update_sql_vals(Row(id=0, is_male=False))
        self.assertEqual(f'update {table_name} set is_male=? where id=?;', sql)
        self.assertEqual((0,0), prepards)
    def test_update_sql_vals_id_is_male_true(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        Row = db.get_row(table_name)
        sql, prepards = db._update_sql_vals(Row(id=0, is_male=True))
        self.assertEqual(f'update {table_name} set is_male=? where id=?;', sql)
        self.assertEqual((1,0), prepards)

    def test_update_sql_vals(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        # insert
        db.insert('users', (0, 'A', 10.1, bytes(2), True, datetime.fromisoformat('2000-01-01T00:00:00+00:00')))
        db.insert('users', (1, 'B', 10.2, bytes(3), False, datetime.fromisoformat('2000-01-02T00:00:00+00:00')))
        res = db.get("select * from users where id=?;", (0,))
        self.assertEqual(0, res.id)
        self.assertEqual('A', res.name)
        self.assertEqual(10.1, res.value)
        self.assertEqual(bytes(2), res.img)
        self.assertEqual(True, res.is_male)
        self.assertEqual('2000-01-01 00:00:00', res.birth)
        res = db.get("select * from users where id=?;", (1,))
        self.assertEqual(1, res.id)
        self.assertEqual('B', res.name)
        self.assertEqual(10.2, res.value)
        self.assertEqual(bytes(3), res.img)
        self.assertEqual(False, res.is_male)
        self.assertEqual('2000-01-02 00:00:00', res.birth)
        # update
        Row = db.get_row('users')
        db.update(Row(id=0, name='aaa'))
        res = db.get("select * from users where id=?;", (0,))
        self.assertEqual(0, res.id)
        self.assertEqual('aaa', res.name) # ここだけ'A'から'aaa'に変わってるはず
        self.assertEqual(10.1, res.value)
        self.assertEqual(bytes(2), res.img)
        self.assertEqual(True, res.is_male)
        self.assertEqual('2000-01-01 00:00:00', res.birth)
        res = db.get("select * from users where id=?;", (1,))
        self.assertEqual(1, res.id)
        self.assertEqual('B', res.name)
        self.assertEqual(10.2, res.value)
        self.assertEqual(bytes(3), res.img)
        self.assertEqual(False, res.is_male)
        self.assertEqual('2000-01-02 00:00:00', res.birth)
    def test_update_birth_native_is_local(self):
        db = NtLite()
        table_name = 'users'
        db.exec(f"create table {table_name} (id integer not null primary key, name text not null, value real, img blob, is_male bool, birth datetime);")
        db.insert(table_name, (0, 'A', 10.1, bytes(2), True, datetime.fromisoformat('2000-01-01T00:00:00+00:00')))
        Row = db.get_row(table_name)
        db.update(Row(id=0, birth=datetime.fromisoformat('2000-01-01 00:00:00')))
        res = db.get("select birth from users where id=?;", (0,))
        if datetime.now().astimezone().tzinfo == timezone(timedelta(seconds=32400)):
            self.assertEqual('1999-12-31 15:00:00', res[0])#キャスト済なのでネイティブはローカル時として解釈されUTC時に変換される


if __name__ == '__main__':
    unittest.main()
