---
title: "02.[译文] RangeQuery and WindowedRangeQuery"
date: 2022-04-02T20:43:53+08:00
categories: ["python"]
tags: ["python","sqlalchemy"]
keywords: ["译文","ORM", "RangeQuery"]
---

近期在使用 SQLAlchemy 读取大批量数据的时候，遇到了一个问题，因为数据量过大而查询进程被 Kill 的问题，就想到了使用生成器或者单个请求拆分多次查询的方法。在 github 上找到了一篇 Wiki，照着 Wiki 中的方式，成功解决了 SQLAlchemy 查大数据失败的问题。

这篇 Wiki 文章比较短小，英文读起来也不吃力，但想着还是顺便练习一下翻译，水一篇博客好了。


### 译文：

> Todo 

### 原文：
> 原文链接：[RangeQuery-and-WindowedRangeQuery](https://github.com/sqlalchemy/sqlalchemy/wiki/RangeQuery-and-WindowedRangeQuery)：

#### RangeQuery and WindowedRangeQuery

The goal is to select through a very large number of rows that's too large to fetch all at once. Many DBAPIs pre-buffer result sets fully, and otherwise it can be difficult to keep an active cursor when using an option like psycopg2's server side cursors. The usual alternative, i.e. to page through the results using LIMIT/OFFSET, has the downside that the OFFSET will scan through all the previous rows each time in order to get to the requested row. To overcome this, there are two approaches to page through results without using OFFSET.

The simplest is to order the results by a particular unique column (usually primary key), then fetch chunks using LIMIT only, adding a WHERE clause that will ensure we only fetch rows greater than the last one we fetched). This will work for basically any database backend and is illustrated below for MySQL. The potential downside is that the database needs to sort the full set of remaining rows for each chunk, which may inefficient, even though both recipes presented here assume the sort column is indexed. However, the approach is very simple and can likely work for most ordinary use cases for a primary key column on a database that does not support window functions.

```python
def windowed_query(q, column, windowsize):
    """"Break a Query into chunks on a given column."""

    single_entity = q.is_single_entity
    q = q.add_column(column).order_by(column)
    last_id = None

    while True:
        subq = q
        if last_id is not None:
            subq = subq.filter(column > last_id)
        chunk = subq.limit(windowsize).all()
        if not chunk:
            break
        last_id = chunk[-1][-1]
        for row in chunk:
            if single_entity:
                yield row[0]
            else:
                yield row[0:-1]


if __name__ == "__main__":
    from sqlalchemy import Column, Integer, create_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import declarative_base
    import random

    Base = declarative_base()

    class Widget(Base):
        __tablename__ = "widget"
        id = Column(Integer, primary_key=True)
        data = Column(Integer)

    e = create_engine("mysql://scott:tiger@localhost/test", echo="debug")

    Base.metadata.drop_all(e)
    Base.metadata.create_all(e)

    # get some random list of unique values
    data = set([random.randint(1, 1000000) for i in range(10000)])

    s = Session(e)
    s.add_all([Widget(id=i, data=j) for i, j in enumerate(data, 1)])
    s.commit()

    q = s.query(Widget)

    for widget in windowed_query(q, Widget.data, 1000):
        print("data:", widget.data)

```

A more elaborate way to do this, which allows that the table rows are fully sorted only once, is to use a window function in order to establish the exact range for each "chunk" ahead of time, and then to yield chunks as rows selected within that range. This works only on databases that support windows functions. This recipe has been on the SQLAlchemy Wiki for a long time but it's not clear how much advantage it has over the previous simpler approach; both approaches should be evaluated for efficiency for a given use case.

```python
import sqlalchemy
from sqlalchemy import and_, func

def column_windows(session, column, windowsize):
    """Return a series of WHERE clauses against 
    a given column that break it into windows.

    Result is an iterable of tuples, consisting of
    ((start, end), whereclause), where (start, end) are the ids.
    
    Requires a database that supports window functions, 
    i.e. Postgresql, SQL Server, Oracle.

    Enhance this yourself !  Add a "where" argument
    so that windows of just a subset of rows can
    be computed.

    """
    def int_for_range(start_id, end_id):
        if end_id:
            return and_(
                column>=start_id,
                column<end_id
            )
        else:
            return column>=start_id

    q = session.query(
                column, 
                func.row_number().\
                        over(order_by=column).\
                        label('rownum')
                ).\
                from_self(column)
    if windowsize > 1:
        q = q.filter(sqlalchemy.text("rownum %% %d=1" % windowsize))

    intervals = [id for id, in q]

    while intervals:
        start = intervals.pop(0)
        if intervals:
            end = intervals[0]
        else:
            end = None
        yield int_for_range(start, end)

def windowed_query(q, column, windowsize):
    """"Break a Query into windows on a given column."""

    for whereclause in column_windows(
                                        q.session, 
                                        column, windowsize):
        for row in q.filter(whereclause).order_by(column):
            yield row

if __name__ == '__main__':
    from sqlalchemy import Column, Integer, create_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import declarative_base
    import random

    Base = declarative_base()

    class Widget(Base):
        __tablename__ = 'widget'
        id = Column(Integer, primary_key=True)
        data = Column(Integer)

    e = create_engine('postgresql://scott:tiger@localhost/test', echo='debug')

    Base.metadata.drop_all(e)
    Base.metadata.create_all(e)

    # get some random list of unique values
    data = set([random.randint(1, 1000000) for i in xrange(10000)])

    s = Session(e)
    s.add_all([Widget(id=i, data=j) for i, j in enumerate(data)])
    s.commit()

    q = s.query(Widget)

    for widget in windowed_query(q, Widget.data, 1000):
        print "data:", widget.data
```

Here's an example of the kind of SQL this emits:

```sql
-- first, it gets a list of ranges, with 1000 values in each bucket
SELECT anon_1.widget_data AS anon_1_widget_data 
FROM (SELECT widget.data AS widget_data, row_number() OVER (ORDER BY widget.data) AS rownum 
FROM widget) AS anon_1 
WHERE rownum %% 1000=1

Col ('anon_1_widget_data',)
Row (4,)
Row (100107,)
Row (200004,)
Row (299526,)
Row (397664,)
Row (502373,)
Row (597853,)
Row (695306,)
Row (798335,)
Row (899000,)

-- then, the original query is run for each window, adding in 
-- the extra range criterion

SELECT widget.id AS widget_id, widget.data AS widget_data 
FROM widget 
WHERE widget.data >= %(data_1)s AND widget.data < %(data_2)s ORDER BY widget.data
-- values: {'data_2': 100107, 'data_1': 4}
Col ('widget_id', 'widget_data')
Row (1, 4)
Row (64, 211)
Row (5415, 554)
Row (168, 568)
Row (203, 672)
Row (275, 914)
Row (343, 1124)
Row (344, 1132)
...


SELECT widget.id AS widget_id, widget.data AS widget_data 
FROM widget 
WHERE widget.data >= %(data_1)s AND widget.data < %(data_2)s ORDER BY widget.data
-- values: {'data_2': 200004, 'data_1': 100107}
Col ('widget_id', 'widget_data')
Row (544, 100107)
Row (549, 100120)
Row (583, 100225)
Row (3564, 100235)
Row (588, 100241)
Row (594, 100258)
Row (599, 100274)

...
```
#### Non Window Function Version
Don't have window functions on the target database? Here's an approach that uses LIMIT in conjunction with tracking the last fetched primary key:

```python
def _yield_limit(qry, pk_attr, maxrq=100):
    """specialized windowed query generator (using LIMIT/OFFSET)

    This recipe is to select through a large number of rows thats too
    large to fetch at once. The technique depends on the primary key
    of the FROM clause being an integer value, and selects items
    using LIMIT."""

    firstid = None
    while True:
        q = qry
        if firstid is not None:
            q = qry.filter(pk_attr > firstid)
        rec = None
        for rec in q.order_by(pk_attr).limit(maxrq):
            yield rec
        if rec is None:
            break
        firstid = pk_attr.__get__(rec, pk_attr) if rec else None

if __name__ == '__main__':
    from sqlalchemy import create_engine, Column, Integer
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)

    e = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(e)

    sess = Session(e)

    sess.add_all([A() for i in range(2000)])

    for rec in _yield_limit(sess.query(A), A.id):
        print(rec.id, rec)
```
