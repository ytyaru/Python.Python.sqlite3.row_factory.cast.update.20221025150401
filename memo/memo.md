update文をラップする。

　update文をラップする。

<!-- more -->

# ブツ

* [リポジトリ][]

[リポジトリ]:https://github.com/ytyaru/Python.sqlite3.row_factory.cast.update.20221025150401
[DEMO]:https://ytyaru.github.io/Python.sqlite3.row_factory.cast.update.20221025150401/

## 実行

```sh
NAME='Python.sqlite3.row_factory.cast.update.20221025150401'
git clone https://github.com/ytyaru/$NAME
cd $NAME/src
./run.py
./test.py
```

# コード抜粋

　update文と?に渡すデータを作る。``

```python
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
    return f"update {table_name} set {set_sql} {where_sql(row)};", CastPy.to_sql_by_row(tuple(get_vals(row) + (get_vals(where) if where else [row.id])))
```

　あとはキャストして`sqlite3.executemany()`にSQL文とデータを渡すだけ。

```python
def update(self, table_name, value, where=None): return self._cast_exec(*self._update_sql_vals(table_name, value, where))
```

