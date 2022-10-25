#!/usr/bin/env python3
# coding: utf8
import os
from ntlite import NtLite
path = 'my.db'
if os.path.isfile(path): os.remove(path)
db = NtLite(path)
db.exec("create table users(id integer, name text);")
db.execm("insert into users values(?,?);", [(0,'A'),(1,'B')])
assert 2 == db.get("select count(*) num from users;").num
rows = db.gets("select * from users;")
assert 0   == rows[0].id
assert 'A' == rows[0].name
assert 1   == rows[1].id
assert 'B' == rows[1].name

assert 0   == rows[0]['id']
assert 'A' == rows[0]['name']
assert 1   == rows[1]['id']
assert 'B' == rows[1]['name']

assert 0   == rows[0][0]
assert 'A' == rows[0][1]
assert 1   == rows[1][0]
assert 'B' == rows[1][1]

