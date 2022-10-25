#!/usr/bin/env python3
# coding: utf8
from ntlite import NtLite
db = NtLite()
db.exec("create table users(id integer, name text, age integer);")
db.exec("create table jobs(id integer, name text);")
assert ('users','jobs') == db.table_names()
assert ('id','name','age') == db.column_names('users')
print(db.table_info('users'))
