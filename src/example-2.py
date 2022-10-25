#!/usr/bin/env python3
# coding: utf8
from ntlite import NtLite

db = NtLite()
db.get("create table users(id integer, name text, age integer);")
db.get("insert into users values(0,'A',7),(1,'B',8);")
rows = db.gets("select age from users where age>=0;")
print(rows)
print(rows[0].age) # これがやりたかった！ rows[0][0]やrows[0]['age']でなく。
print(rows[1].age)
print(getattr(rows[1], 'age')) # 名前を変数にしても参照・取得できる
#print(rows[1]['age']) # TypeError: tuple indices must be integers or slices, not str
print(rows[1]['age']) # これもできるようにした！
print(rows[1][0]) # これもできる
row = db.get("select count(*) num from users where id>=?;", (0,))
print(row)
print(row.num)

