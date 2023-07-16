### Импорт библиотек для работы с бд и дф


```python
import psycopg2
import pandas as pd
import sqlalchemy
```

### Кредиты для коннекта к бд


```python
conn = psycopg2.connect(host="127.0.0.1", port="5432", database="Adventureworks", 
                            user="postgres", password="postgres")
```


```python
query = f'''

 select 
        storeid,
        salesorderid,
        subtotal,
        orderdate
        from sales.customer l
        join sales.salesorderheader r
        on l.customerid = r.customerid
        where storeid in  (292, 300,658)
  
'''
df = pd.read_sql_query(query, conn)
df.head(20)
```

    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>storeid</th>
      <th>salesorderid</th>
      <th>subtotal</th>
      <th>orderdate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292</td>
      <td>44132</td>
      <td>4049.9880</td>
      <td>2011-08-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292</td>
      <td>45579</td>
      <td>4079.9880</td>
      <td>2012-01-29</td>
    </tr>
    <tr>
      <th>2</th>
      <td>292</td>
      <td>46389</td>
      <td>1104.9968</td>
      <td>2012-04-30</td>
    </tr>
    <tr>
      <th>3</th>
      <td>658</td>
      <td>47388</td>
      <td>202.3320</td>
      <td>2012-07-31</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292</td>
      <td>47454</td>
      <td>27429.5294</td>
      <td>2012-07-31</td>
    </tr>
    <tr>
      <th>5</th>
      <td>658</td>
      <td>48329</td>
      <td>1070.0565</td>
      <td>2012-10-30</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292</td>
      <td>48395</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
    </tr>
    <tr>
      <th>7</th>
      <td>292</td>
      <td>49495</td>
      <td>24232.7654</td>
      <td>2013-01-28</td>
    </tr>
    <tr>
      <th>8</th>
      <td>658</td>
      <td>50679</td>
      <td>386.2702</td>
      <td>2013-04-30</td>
    </tr>
    <tr>
      <th>9</th>
      <td>292</td>
      <td>50756</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
    </tr>
    <tr>
      <th>10</th>
      <td>300</td>
      <td>53485</td>
      <td>57771.7641</td>
      <td>2013-07-31</td>
    </tr>
    <tr>
      <th>11</th>
      <td>658</td>
      <td>53495</td>
      <td>4098.6480</td>
      <td>2013-07-31</td>
    </tr>
    <tr>
      <th>12</th>
      <td>300</td>
      <td>58931</td>
      <td>49053.4638</td>
      <td>2013-10-30</td>
    </tr>
    <tr>
      <th>13</th>
      <td>658</td>
      <td>58941</td>
      <td>564.6240</td>
      <td>2013-10-30</td>
    </tr>
    <tr>
      <th>14</th>
      <td>300</td>
      <td>65191</td>
      <td>56353.8690</td>
      <td>2014-01-29</td>
    </tr>
    <tr>
      <th>15</th>
      <td>658</td>
      <td>65214</td>
      <td>2.7480</td>
      <td>2014-01-29</td>
    </tr>
    <tr>
      <th>16</th>
      <td>300</td>
      <td>71805</td>
      <td>57990.6876</td>
      <td>2014-05-01</td>
    </tr>
    <tr>
      <th>17</th>
      <td>658</td>
      <td>71867</td>
      <td>858.9000</td>
      <td>2014-05-01</td>
    </tr>
  </tbody>
</table>
</div>



### проблема производительности, поскольку PostgreSQL должен сканировать таблицу отдельно для каждого запроса.
### Чтобы сделать его более эффективным, PostgreSQL предоставляет предложение GROUPING sets
### НАБОРЫ ГРУППИРОВКИ позволяют определять несколько наборов группирования в одном запросе.


```python
query = f'''

 select 
        storeid,
        count(salesorderid),
        sum(subtotal),
        orderdate
        from sales.customer l
        join sales.salesorderheader r
        on l.customerid = r.customerid
        where storeid in  (292, 300,658)
        group by 
        grouping sets (storeid, (storeid, orderdate))
  
'''
df = pd.read_sql_query(query, conn)
df.head(40)
```

    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>storeid</th>
      <th>count</th>
      <th>sum</th>
      <th>orderdate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292</td>
      <td>1</td>
      <td>4049.9880</td>
      <td>2011-08-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292</td>
      <td>1</td>
      <td>4079.9880</td>
      <td>2012-01-29</td>
    </tr>
    <tr>
      <th>2</th>
      <td>292</td>
      <td>1</td>
      <td>1104.9968</td>
      <td>2012-04-30</td>
    </tr>
    <tr>
      <th>3</th>
      <td>292</td>
      <td>1</td>
      <td>27429.5294</td>
      <td>2012-07-31</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292</td>
      <td>1</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
    </tr>
    <tr>
      <th>5</th>
      <td>292</td>
      <td>1</td>
      <td>24232.7654</td>
      <td>2013-01-28</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292</td>
      <td>1</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
    </tr>
    <tr>
      <th>7</th>
      <td>292</td>
      <td>7</td>
      <td>131102.9823</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>8</th>
      <td>300</td>
      <td>1</td>
      <td>57771.7641</td>
      <td>2013-07-31</td>
    </tr>
    <tr>
      <th>9</th>
      <td>300</td>
      <td>1</td>
      <td>49053.4638</td>
      <td>2013-10-30</td>
    </tr>
    <tr>
      <th>10</th>
      <td>300</td>
      <td>1</td>
      <td>56353.8690</td>
      <td>2014-01-29</td>
    </tr>
    <tr>
      <th>11</th>
      <td>300</td>
      <td>1</td>
      <td>57990.6876</td>
      <td>2014-05-01</td>
    </tr>
    <tr>
      <th>12</th>
      <td>300</td>
      <td>4</td>
      <td>221169.7845</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>13</th>
      <td>658</td>
      <td>1</td>
      <td>202.3320</td>
      <td>2012-07-31</td>
    </tr>
    <tr>
      <th>14</th>
      <td>658</td>
      <td>1</td>
      <td>1070.0565</td>
      <td>2012-10-30</td>
    </tr>
    <tr>
      <th>15</th>
      <td>658</td>
      <td>1</td>
      <td>386.2702</td>
      <td>2013-04-30</td>
    </tr>
    <tr>
      <th>16</th>
      <td>658</td>
      <td>1</td>
      <td>4098.6480</td>
      <td>2013-07-31</td>
    </tr>
    <tr>
      <th>17</th>
      <td>658</td>
      <td>1</td>
      <td>564.6240</td>
      <td>2013-10-30</td>
    </tr>
    <tr>
      <th>18</th>
      <td>658</td>
      <td>1</td>
      <td>2.7480</td>
      <td>2014-01-29</td>
    </tr>
    <tr>
      <th>19</th>
      <td>658</td>
      <td>1</td>
      <td>858.9000</td>
      <td>2014-05-01</td>
    </tr>
    <tr>
      <th>20</th>
      <td>658</td>
      <td>7</td>
      <td>7183.5787</td>
      <td>NaT</td>
    </tr>
  </tbody>
</table>
</div>



### ROLLUP - это подитог для предложения GROUP BY, который предлагает сокращенное обозначение для определения нескольких наборов группировки.  



```python
query = f'''

 select 
        storeid,
        count(salesorderid),
        sum(subtotal),
        date_trunc('year', orderdate)
        from sales.customer l
        join sales.salesorderheader r
        on l.customerid = r.customerid
        where storeid in  (292, 300,658)
        group by 
        grouping sets (storeid, (storeid,  date_trunc('year', orderdate)), rollup (storeid))
  
'''
df = pd.read_sql_query(query, conn)
df.head(30)
```

    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>storeid</th>
      <th>count</th>
      <th>sum</th>
      <th>date_trunc</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292.0</td>
      <td>1</td>
      <td>4049.9880</td>
      <td>2011-01-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292.0</td>
      <td>4</td>
      <td>65177.1680</td>
      <td>2012-01-01</td>
    </tr>
    <tr>
      <th>2</th>
      <td>292.0</td>
      <td>2</td>
      <td>61875.8263</td>
      <td>2013-01-01</td>
    </tr>
    <tr>
      <th>3</th>
      <td>292.0</td>
      <td>7</td>
      <td>131102.9823</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292.0</td>
      <td>7</td>
      <td>131102.9823</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>5</th>
      <td>300.0</td>
      <td>2</td>
      <td>106825.2279</td>
      <td>2013-01-01</td>
    </tr>
    <tr>
      <th>6</th>
      <td>300.0</td>
      <td>2</td>
      <td>114344.5566</td>
      <td>2014-01-01</td>
    </tr>
    <tr>
      <th>7</th>
      <td>300.0</td>
      <td>4</td>
      <td>221169.7845</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>8</th>
      <td>300.0</td>
      <td>4</td>
      <td>221169.7845</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>9</th>
      <td>658.0</td>
      <td>2</td>
      <td>1272.3885</td>
      <td>2012-01-01</td>
    </tr>
    <tr>
      <th>10</th>
      <td>658.0</td>
      <td>3</td>
      <td>5049.5422</td>
      <td>2013-01-01</td>
    </tr>
    <tr>
      <th>11</th>
      <td>658.0</td>
      <td>2</td>
      <td>861.6480</td>
      <td>2014-01-01</td>
    </tr>
    <tr>
      <th>12</th>
      <td>658.0</td>
      <td>7</td>
      <td>7183.5787</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>13</th>
      <td>658.0</td>
      <td>7</td>
      <td>7183.5787</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>14</th>
      <td>NaN</td>
      <td>18</td>
      <td>359456.3455</td>
      <td>NaT</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''

 select 
        storeid,
        count(salesorderid),
        sum(subtotal)
        from sales.customer l
        join sales.salesorderheader r
        on l.customerid = r.customerid
        where storeid in  (292, 300,658)
        group by rollup (storeid)
  
'''
df = pd.read_sql_query(query, conn)
df.head(30)
```

    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>storeid</th>
      <th>count</th>
      <th>sum</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>18</td>
      <td>359456.3455</td>
    </tr>
    <tr>
      <th>1</th>
      <td>300.0</td>
      <td>4</td>
      <td>221169.7845</td>
    </tr>
    <tr>
      <th>2</th>
      <td>658.0</td>
      <td>7</td>
      <td>7183.5787</td>
    </tr>
    <tr>
      <th>3</th>
      <td>292.0</td>
      <td>7</td>
      <td>131102.9823</td>
    </tr>
  </tbody>
</table>
</div>



### CUBE используется в основном для целей отчетности. С помощью CUBE пользователь может расширить функциональность предложений GROUP BY, 
### вычислив общие значения (для каждого из наборов). Перебор данных гораздо больше.
### CUBE(X,Y,Z) создает grouping sets для всех возможных значений: 


```python
query = f'''

 select 
        storeid,
        count(salesorderid),
        sum(subtotal),
        date_trunc('year', orderdate)
        from sales.customer l
        join sales.salesorderheader r
        on l.customerid = r.customerid
        where storeid in  (292, 300,658)
        group by cube (storeid, date_trunc('year', orderdate))
  
'''
df = pd.read_sql_query(query, conn)
df.head(30)
```

    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>storeid</th>
      <th>count</th>
      <th>sum</th>
      <th>date_trunc</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>18</td>
      <td>359456.3455</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>1</th>
      <td>300.0</td>
      <td>2</td>
      <td>114344.5566</td>
      <td>2014-01-01</td>
    </tr>
    <tr>
      <th>2</th>
      <td>300.0</td>
      <td>2</td>
      <td>106825.2279</td>
      <td>2013-01-01</td>
    </tr>
    <tr>
      <th>3</th>
      <td>658.0</td>
      <td>3</td>
      <td>5049.5422</td>
      <td>2013-01-01</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292.0</td>
      <td>2</td>
      <td>61875.8263</td>
      <td>2013-01-01</td>
    </tr>
    <tr>
      <th>5</th>
      <td>658.0</td>
      <td>2</td>
      <td>861.6480</td>
      <td>2014-01-01</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292.0</td>
      <td>1</td>
      <td>4049.9880</td>
      <td>2011-01-01</td>
    </tr>
    <tr>
      <th>7</th>
      <td>658.0</td>
      <td>2</td>
      <td>1272.3885</td>
      <td>2012-01-01</td>
    </tr>
    <tr>
      <th>8</th>
      <td>292.0</td>
      <td>4</td>
      <td>65177.1680</td>
      <td>2012-01-01</td>
    </tr>
    <tr>
      <th>9</th>
      <td>300.0</td>
      <td>4</td>
      <td>221169.7845</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>10</th>
      <td>658.0</td>
      <td>7</td>
      <td>7183.5787</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>11</th>
      <td>292.0</td>
      <td>7</td>
      <td>131102.9823</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>12</th>
      <td>NaN</td>
      <td>7</td>
      <td>173750.5964</td>
      <td>2013-01-01</td>
    </tr>
    <tr>
      <th>13</th>
      <td>NaN</td>
      <td>6</td>
      <td>66449.5565</td>
      <td>2012-01-01</td>
    </tr>
    <tr>
      <th>14</th>
      <td>NaN</td>
      <td>1</td>
      <td>4049.9880</td>
      <td>2011-01-01</td>
    </tr>
    <tr>
      <th>15</th>
      <td>NaN</td>
      <td>4</td>
      <td>115206.2046</td>
      <td>2014-01-01</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.rollback()
conn.close()
```
