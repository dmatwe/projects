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

select l.storeid, r.salesorderid, r.orderdate
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
  
'''
df = pd.read_sql_query(query, conn)
df.head()
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
      <th>orderdate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1046.0</td>
      <td>43659</td>
      <td>2011-05-31</td>
    </tr>
    <tr>
      <th>1</th>
      <td>722.0</td>
      <td>43660</td>
      <td>2011-05-31</td>
    </tr>
    <tr>
      <th>2</th>
      <td>852.0</td>
      <td>43661</td>
      <td>2011-05-31</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1418.0</td>
      <td>43662</td>
      <td>2011-05-31</td>
    </tr>
    <tr>
      <th>4</th>
      <td>484.0</td>
      <td>43663</td>
      <td>2011-05-31</td>
    </tr>
  </tbody>
</table>
</div>



### Функция ROW_NUMBER () - это оконная функция, которая присваивает последовательное целое число каждой строке в наборе результатов.



```python
query = f'''

select row_number() over (order by storeid, salesorderid, orderdate),
l.storeid, r.salesorderid, r.orderdate
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
  
'''
df = pd.read_sql_query(query, conn)
df.head()
```




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
      <th>row_number</th>
      <th>storeid</th>
      <th>salesorderid</th>
      <th>orderdate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>292.0</td>
      <td>44132</td>
      <td>2011-08-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>292.0</td>
      <td>45579</td>
      <td>2012-01-29</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>292.0</td>
      <td>46389</td>
      <td>2012-04-30</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>292.0</td>
      <td>47454</td>
      <td>2012-07-31</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>292.0</td>
      <td>48395</td>
      <td>2012-10-30</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''

select l.storeid, r.salesorderid, r.orderdate, subtotal
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
  
'''
df = pd.read_sql_query(query, conn)
df.head()
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
      <th>orderdate</th>
      <th>subtotal</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1046.0</td>
      <td>43659</td>
      <td>2011-05-31</td>
      <td>20565.6206</td>
    </tr>
    <tr>
      <th>1</th>
      <td>722.0</td>
      <td>43660</td>
      <td>2011-05-31</td>
      <td>1294.2529</td>
    </tr>
    <tr>
      <th>2</th>
      <td>852.0</td>
      <td>43661</td>
      <td>2011-05-31</td>
      <td>32726.4786</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1418.0</td>
      <td>43662</td>
      <td>2011-05-31</td>
      <td>28832.5289</td>
    </tr>
    <tr>
      <th>4</th>
      <td>484.0</td>
      <td>43663</td>
      <td>2011-05-31</td>
      <td>419.4589</td>
    </tr>
  </tbody>
</table>
</div>



### Функция RANK () показывает ранг связанных строк в связанном ранге.
### поэтому ранги могут быть не последовательными. 
### Кроме того, строки с одинаковыми значениями получат одинаковый ранг.
### вычисляет номер строки от предыдущего набора


```python
query = f'''

select  rank() over (order by subtotal), 
l.storeid,
r.salesorderid, r.orderdate, subtotal
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
where storeid is not null
order by subtotal

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
      <th>rank</th>
      <th>storeid</th>
      <th>salesorderid</th>
      <th>orderdate</th>
      <th>subtotal</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1904</td>
      <td>51782</td>
      <td>2013-06-30</td>
      <td>1.3740</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>658</td>
      <td>65214</td>
      <td>2014-01-29</td>
      <td>2.7480</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1074</td>
      <td>53564</td>
      <td>2013-07-31</td>
      <td>2.7480</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>1948</td>
      <td>44303</td>
      <td>2011-08-31</td>
      <td>5.7000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>1906</td>
      <td>65204</td>
      <td>2014-01-29</td>
      <td>10.7880</td>
    </tr>
    <tr>
      <th>5</th>
      <td>5</td>
      <td>1346</td>
      <td>65301</td>
      <td>2014-01-29</td>
      <td>10.7880</td>
    </tr>
    <tr>
      <th>6</th>
      <td>5</td>
      <td>864</td>
      <td>71842</td>
      <td>2014-05-01</td>
      <td>10.7880</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>850</td>
      <td>44080</td>
      <td>2011-08-01</td>
      <td>11.4000</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>1814</td>
      <td>57033</td>
      <td>2013-09-30</td>
      <td>12.1440</td>
    </tr>
    <tr>
      <th>9</th>
      <td>9</td>
      <td>1078</td>
      <td>65169</td>
      <td>2014-01-29</td>
      <td>12.1440</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>1838</td>
      <td>48047</td>
      <td>2012-09-30</td>
      <td>14.1289</td>
    </tr>
    <tr>
      <th>11</th>
      <td>12</td>
      <td>1088</td>
      <td>57182</td>
      <td>2013-09-30</td>
      <td>15.0000</td>
    </tr>
    <tr>
      <th>12</th>
      <td>13</td>
      <td>864</td>
      <td>58973</td>
      <td>2013-10-30</td>
      <td>16.1820</td>
    </tr>
    <tr>
      <th>13</th>
      <td>14</td>
      <td>844</td>
      <td>53529</td>
      <td>2013-07-31</td>
      <td>16.2720</td>
    </tr>
    <tr>
      <th>14</th>
      <td>14</td>
      <td>872</td>
      <td>59005</td>
      <td>2013-10-30</td>
      <td>16.2720</td>
    </tr>
    <tr>
      <th>15</th>
      <td>16</td>
      <td>916</td>
      <td>48024</td>
      <td>2012-09-30</td>
      <td>20.1865</td>
    </tr>
    <tr>
      <th>16</th>
      <td>16</td>
      <td>1068</td>
      <td>48029</td>
      <td>2012-09-30</td>
      <td>20.1865</td>
    </tr>
    <tr>
      <th>17</th>
      <td>16</td>
      <td>1082</td>
      <td>45530</td>
      <td>2012-01-29</td>
      <td>20.1865</td>
    </tr>
    <tr>
      <th>18</th>
      <td>16</td>
      <td>1362</td>
      <td>43903</td>
      <td>2011-07-01</td>
      <td>20.1865</td>
    </tr>
    <tr>
      <th>19</th>
      <td>20</td>
      <td>1924</td>
      <td>51796</td>
      <td>2013-06-30</td>
      <td>20.3940</td>
    </tr>
  </tbody>
</table>
</div>



### DENSE_RANK () присваивает последовательный номер каждому набору результатов. В отличие от функции RANK (), 
### функция DENSE_RANK () всегда возвращает последовательные значения ранга. 


```python
query = f'''

select  dense_rank() over (order by subtotal), 
l.storeid,
r.salesorderid, r.orderdate, subtotal
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
where storeid is not null
order by subtotal

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
      <th>dense_rank</th>
      <th>storeid</th>
      <th>salesorderid</th>
      <th>orderdate</th>
      <th>subtotal</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1904</td>
      <td>51782</td>
      <td>2013-06-30</td>
      <td>1.3740</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>658</td>
      <td>65214</td>
      <td>2014-01-29</td>
      <td>2.7480</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1074</td>
      <td>53564</td>
      <td>2013-07-31</td>
      <td>2.7480</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>1948</td>
      <td>44303</td>
      <td>2011-08-31</td>
      <td>5.7000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>1906</td>
      <td>65204</td>
      <td>2014-01-29</td>
      <td>10.7880</td>
    </tr>
    <tr>
      <th>5</th>
      <td>4</td>
      <td>1346</td>
      <td>65301</td>
      <td>2014-01-29</td>
      <td>10.7880</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4</td>
      <td>864</td>
      <td>71842</td>
      <td>2014-05-01</td>
      <td>10.7880</td>
    </tr>
    <tr>
      <th>7</th>
      <td>5</td>
      <td>850</td>
      <td>44080</td>
      <td>2011-08-01</td>
      <td>11.4000</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>1814</td>
      <td>57033</td>
      <td>2013-09-30</td>
      <td>12.1440</td>
    </tr>
    <tr>
      <th>9</th>
      <td>6</td>
      <td>1078</td>
      <td>65169</td>
      <td>2014-01-29</td>
      <td>12.1440</td>
    </tr>
    <tr>
      <th>10</th>
      <td>7</td>
      <td>1838</td>
      <td>48047</td>
      <td>2012-09-30</td>
      <td>14.1289</td>
    </tr>
    <tr>
      <th>11</th>
      <td>8</td>
      <td>1088</td>
      <td>57182</td>
      <td>2013-09-30</td>
      <td>15.0000</td>
    </tr>
    <tr>
      <th>12</th>
      <td>9</td>
      <td>864</td>
      <td>58973</td>
      <td>2013-10-30</td>
      <td>16.1820</td>
    </tr>
    <tr>
      <th>13</th>
      <td>10</td>
      <td>844</td>
      <td>53529</td>
      <td>2013-07-31</td>
      <td>16.2720</td>
    </tr>
    <tr>
      <th>14</th>
      <td>10</td>
      <td>872</td>
      <td>59005</td>
      <td>2013-10-30</td>
      <td>16.2720</td>
    </tr>
    <tr>
      <th>15</th>
      <td>11</td>
      <td>916</td>
      <td>48024</td>
      <td>2012-09-30</td>
      <td>20.1865</td>
    </tr>
    <tr>
      <th>16</th>
      <td>11</td>
      <td>1068</td>
      <td>48029</td>
      <td>2012-09-30</td>
      <td>20.1865</td>
    </tr>
    <tr>
      <th>17</th>
      <td>11</td>
      <td>1082</td>
      <td>45530</td>
      <td>2012-01-29</td>
      <td>20.1865</td>
    </tr>
    <tr>
      <th>18</th>
      <td>11</td>
      <td>1362</td>
      <td>43903</td>
      <td>2011-07-01</td>
      <td>20.1865</td>
    </tr>
    <tr>
      <th>19</th>
      <td>12</td>
      <td>1924</td>
      <td>51796</td>
      <td>2013-06-30</td>
      <td>20.3940</td>
    </tr>
  </tbody>
</table>
</div>



### Функция LAG () обеспечивает доступ к строке, которая предшествует текущей строке с указанным физическим смещением. 
### Другими словами, из текущей строки функция LAG () может получить доступ к данным предыдущей строки или строки перед предыдущей строкой и так далее.
### Функция LAG () будет очень полезна для сравнения значений текущей и предыдущей строки. 
### LAG предыдущее значение в выбираемой строке со смещением
### Lead показывает следующую значение


```python
query = f'''

select 
l.storeid,
r.salesorderid, r.orderdate, subtotal,
lag(subtotal) over (order by orderdate),
lead(subtotal) over (order by orderdate),
lag(subtotal,2) over (order by orderdate) as lag_2,
lead(subtotal,2) over (order by orderdate) as lead_2
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
where storeid = 292
order by orderdate

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
      <th>orderdate</th>
      <th>subtotal</th>
      <th>lag</th>
      <th>lead</th>
      <th>lag_2</th>
      <th>lead_2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292</td>
      <td>44132</td>
      <td>2011-08-01</td>
      <td>4049.9880</td>
      <td>NaN</td>
      <td>4079.9880</td>
      <td>NaN</td>
      <td>1104.9968</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292</td>
      <td>45579</td>
      <td>2012-01-29</td>
      <td>4079.9880</td>
      <td>4049.9880</td>
      <td>1104.9968</td>
      <td>NaN</td>
      <td>27429.5294</td>
    </tr>
    <tr>
      <th>2</th>
      <td>292</td>
      <td>46389</td>
      <td>2012-04-30</td>
      <td>1104.9968</td>
      <td>4079.9880</td>
      <td>27429.5294</td>
      <td>4049.9880</td>
      <td>32562.6538</td>
    </tr>
    <tr>
      <th>3</th>
      <td>292</td>
      <td>47454</td>
      <td>2012-07-31</td>
      <td>27429.5294</td>
      <td>1104.9968</td>
      <td>32562.6538</td>
      <td>4079.9880</td>
      <td>24232.7654</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292</td>
      <td>48395</td>
      <td>2012-10-30</td>
      <td>32562.6538</td>
      <td>27429.5294</td>
      <td>24232.7654</td>
      <td>1104.9968</td>
      <td>37643.0609</td>
    </tr>
    <tr>
      <th>5</th>
      <td>292</td>
      <td>49495</td>
      <td>2013-01-28</td>
      <td>24232.7654</td>
      <td>32562.6538</td>
      <td>37643.0609</td>
      <td>27429.5294</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292</td>
      <td>50756</td>
      <td>2013-04-30</td>
      <td>37643.0609</td>
      <td>24232.7654</td>
      <td>NaN</td>
      <td>32562.6538</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



### partition - группировка
### первое или последнее значение по группировке
### Функция NTH_VALUE () возвращает значение из n-й строки в упорядоченном разделе набора результатов.


```python
query = f'''

select distinct
date_trunc('year',orderdate),
first_value(subtotal) over (partition by date_trunc('year',orderdate)),
last_value(subtotal) over (partition by date_trunc('year',orderdate)),
nth_value(subtotal,2) over (partition by date_trunc('year',orderdate))
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
where storeid = 292


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
      <th>date_trunc</th>
      <th>first_value</th>
      <th>last_value</th>
      <th>nth_value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2011-01-01</td>
      <td>4049.9880</td>
      <td>4049.9880</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2012-01-01</td>
      <td>4079.9880</td>
      <td>32562.6538</td>
      <td>1104.9968</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2013-01-01</td>
      <td>24232.7654</td>
      <td>37643.0609</td>
      <td>37643.0609</td>
    </tr>
  </tbody>
</table>
</div>



### Накопительное


```python
query = f'''

select 
salesorderid,
subtotal,
orderdate,
date_trunc('year',orderdate),
sum(subtotal) over (order by date_trunc('year',orderdate))
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
where storeid = 292


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
      <th>salesorderid</th>
      <th>subtotal</th>
      <th>orderdate</th>
      <th>date_trunc</th>
      <th>sum</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>44132</td>
      <td>4049.9880</td>
      <td>2011-08-01</td>
      <td>2011-01-01</td>
      <td>4049.9880</td>
    </tr>
    <tr>
      <th>1</th>
      <td>45579</td>
      <td>4079.9880</td>
      <td>2012-01-29</td>
      <td>2012-01-01</td>
      <td>69227.1560</td>
    </tr>
    <tr>
      <th>2</th>
      <td>46389</td>
      <td>1104.9968</td>
      <td>2012-04-30</td>
      <td>2012-01-01</td>
      <td>69227.1560</td>
    </tr>
    <tr>
      <th>3</th>
      <td>47454</td>
      <td>27429.5294</td>
      <td>2012-07-31</td>
      <td>2012-01-01</td>
      <td>69227.1560</td>
    </tr>
    <tr>
      <th>4</th>
      <td>48395</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
      <td>2012-01-01</td>
      <td>69227.1560</td>
    </tr>
    <tr>
      <th>5</th>
      <td>49495</td>
      <td>24232.7654</td>
      <td>2013-01-28</td>
      <td>2013-01-01</td>
      <td>131102.9823</td>
    </tr>
    <tr>
      <th>6</th>
      <td>50756</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
      <td>2013-01-01</td>
      <td>131102.9823</td>
    </tr>
  </tbody>
</table>
</div>




```python
4049.9880+4079.9880+1104.9968+27429.5294+32562.6538
```




    69227.156




```python
query = f'''

select 
salesorderid,
subtotal,
orderdate,
date_trunc('year',orderdate),
sum(subtotal) over (partition by date_trunc('year',orderdate)) as prt,
sum(subtotal) over (partition by date_trunc('year',orderdate) order by orderdate) as prt_order
from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
where storeid = 292


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
      <th>salesorderid</th>
      <th>subtotal</th>
      <th>orderdate</th>
      <th>date_trunc</th>
      <th>prt</th>
      <th>prt_order</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>44132</td>
      <td>4049.9880</td>
      <td>2011-08-01</td>
      <td>2011-01-01</td>
      <td>4049.9880</td>
      <td>4049.9880</td>
    </tr>
    <tr>
      <th>1</th>
      <td>45579</td>
      <td>4079.9880</td>
      <td>2012-01-29</td>
      <td>2012-01-01</td>
      <td>65177.1680</td>
      <td>4079.9880</td>
    </tr>
    <tr>
      <th>2</th>
      <td>46389</td>
      <td>1104.9968</td>
      <td>2012-04-30</td>
      <td>2012-01-01</td>
      <td>65177.1680</td>
      <td>5184.9848</td>
    </tr>
    <tr>
      <th>3</th>
      <td>47454</td>
      <td>27429.5294</td>
      <td>2012-07-31</td>
      <td>2012-01-01</td>
      <td>65177.1680</td>
      <td>32614.5142</td>
    </tr>
    <tr>
      <th>4</th>
      <td>48395</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
      <td>2012-01-01</td>
      <td>65177.1680</td>
      <td>65177.1680</td>
    </tr>
    <tr>
      <th>5</th>
      <td>49495</td>
      <td>24232.7654</td>
      <td>2013-01-28</td>
      <td>2013-01-01</td>
      <td>61875.8263</td>
      <td>24232.7654</td>
    </tr>
    <tr>
      <th>6</th>
      <td>50756</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
      <td>2013-01-01</td>
      <td>61875.8263</td>
      <td>61875.8263</td>
    </tr>
  </tbody>
</table>
</div>




```python
24232.7654 + 37643.0609
```




    61875.8263




```python
4079.9880+1104.9968	
```




    5184.9848



### ntile  grouping
### Функция позволяет вам разделить упорядоченные строки в разделе на указанное количество ранжированных групп максимально равного размера. 
### Эти ранжированные группы называются контейнерами\buckets .
### разбиваем на 10 групп, с максимально равным составом в контейнерах


```python
query = f'''

select 
storeid,
salesorderid,
subtotal,
orderdate,
ntile(4) over (order by subtotal) as groupid
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
      <th>groupid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>658</td>
      <td>65214</td>
      <td>2.7480</td>
      <td>2014-01-29</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>658</td>
      <td>47388</td>
      <td>202.3320</td>
      <td>2012-07-31</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>658</td>
      <td>50679</td>
      <td>386.2702</td>
      <td>2013-04-30</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>658</td>
      <td>58941</td>
      <td>564.6240</td>
      <td>2013-10-30</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>658</td>
      <td>71867</td>
      <td>858.9000</td>
      <td>2014-05-01</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>658</td>
      <td>48329</td>
      <td>1070.0565</td>
      <td>2012-10-30</td>
      <td>2</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292</td>
      <td>46389</td>
      <td>1104.9968</td>
      <td>2012-04-30</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>292</td>
      <td>44132</td>
      <td>4049.9880</td>
      <td>2011-08-01</td>
      <td>2</td>
    </tr>
    <tr>
      <th>8</th>
      <td>292</td>
      <td>45579</td>
      <td>4079.9880</td>
      <td>2012-01-29</td>
      <td>2</td>
    </tr>
    <tr>
      <th>9</th>
      <td>658</td>
      <td>53495</td>
      <td>4098.6480</td>
      <td>2013-07-31</td>
      <td>2</td>
    </tr>
    <tr>
      <th>10</th>
      <td>292</td>
      <td>49495</td>
      <td>24232.7654</td>
      <td>2013-01-28</td>
      <td>3</td>
    </tr>
    <tr>
      <th>11</th>
      <td>292</td>
      <td>47454</td>
      <td>27429.5294</td>
      <td>2012-07-31</td>
      <td>3</td>
    </tr>
    <tr>
      <th>12</th>
      <td>292</td>
      <td>48395</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
      <td>3</td>
    </tr>
    <tr>
      <th>13</th>
      <td>292</td>
      <td>50756</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
      <td>3</td>
    </tr>
    <tr>
      <th>14</th>
      <td>300</td>
      <td>58931</td>
      <td>49053.4638</td>
      <td>2013-10-30</td>
      <td>4</td>
    </tr>
    <tr>
      <th>15</th>
      <td>300</td>
      <td>65191</td>
      <td>56353.8690</td>
      <td>2014-01-29</td>
      <td>4</td>
    </tr>
    <tr>
      <th>16</th>
      <td>300</td>
      <td>53485</td>
      <td>57771.7641</td>
      <td>2013-07-31</td>
      <td>4</td>
    </tr>
    <tr>
      <th>17</th>
      <td>300</td>
      <td>71805</td>
      <td>57990.6876</td>
      <td>2014-05-01</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



### Иногда вам может потребоваться создать отчет, который показывает верхние или нижние значения x% из набора данных, например, верхний 1% продуктов по доходу. 
### PostgreSQL предоставляет нам функцию CUME_DIST () для его вычисления
### возвращает относительную позицию значения в наборе значений. Берется от 1, 1 - это все
### Функция PERCENT_RANK () оценивает относительное положение значения в наборе значений Берется от 1, 1 - это все


```python
query = f'''

select 
storeid,
salesorderid,
subtotal,
orderdate,
cume_dist() over (partition by storeid order by  orderdate, subtotal) as cume_dist_pos_distrib,
percent_rank() over (partition by storeid order by orderdate, subtotal) as percent_rank_distrib
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
      <th>cume_dist_pos_distrib</th>
      <th>percent_rank_distrib</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292</td>
      <td>44132</td>
      <td>4049.9880</td>
      <td>2011-08-01</td>
      <td>0.142857</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292</td>
      <td>45579</td>
      <td>4079.9880</td>
      <td>2012-01-29</td>
      <td>0.285714</td>
      <td>0.166667</td>
    </tr>
    <tr>
      <th>2</th>
      <td>292</td>
      <td>46389</td>
      <td>1104.9968</td>
      <td>2012-04-30</td>
      <td>0.428571</td>
      <td>0.333333</td>
    </tr>
    <tr>
      <th>3</th>
      <td>292</td>
      <td>47454</td>
      <td>27429.5294</td>
      <td>2012-07-31</td>
      <td>0.571429</td>
      <td>0.500000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292</td>
      <td>48395</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
      <td>0.714286</td>
      <td>0.666667</td>
    </tr>
    <tr>
      <th>5</th>
      <td>292</td>
      <td>49495</td>
      <td>24232.7654</td>
      <td>2013-01-28</td>
      <td>0.857143</td>
      <td>0.833333</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292</td>
      <td>50756</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
      <td>1.000000</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>7</th>
      <td>300</td>
      <td>53485</td>
      <td>57771.7641</td>
      <td>2013-07-31</td>
      <td>0.250000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>8</th>
      <td>300</td>
      <td>58931</td>
      <td>49053.4638</td>
      <td>2013-10-30</td>
      <td>0.500000</td>
      <td>0.333333</td>
    </tr>
    <tr>
      <th>9</th>
      <td>300</td>
      <td>65191</td>
      <td>56353.8690</td>
      <td>2014-01-29</td>
      <td>0.750000</td>
      <td>0.666667</td>
    </tr>
    <tr>
      <th>10</th>
      <td>300</td>
      <td>71805</td>
      <td>57990.6876</td>
      <td>2014-05-01</td>
      <td>1.000000</td>
      <td>1.000000</td>
    </tr>
    <tr>
      <th>11</th>
      <td>658</td>
      <td>47388</td>
      <td>202.3320</td>
      <td>2012-07-31</td>
      <td>0.142857</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>12</th>
      <td>658</td>
      <td>48329</td>
      <td>1070.0565</td>
      <td>2012-10-30</td>
      <td>0.285714</td>
      <td>0.166667</td>
    </tr>
    <tr>
      <th>13</th>
      <td>658</td>
      <td>50679</td>
      <td>386.2702</td>
      <td>2013-04-30</td>
      <td>0.428571</td>
      <td>0.333333</td>
    </tr>
    <tr>
      <th>14</th>
      <td>658</td>
      <td>53495</td>
      <td>4098.6480</td>
      <td>2013-07-31</td>
      <td>0.571429</td>
      <td>0.500000</td>
    </tr>
    <tr>
      <th>15</th>
      <td>658</td>
      <td>58941</td>
      <td>564.6240</td>
      <td>2013-10-30</td>
      <td>0.714286</td>
      <td>0.666667</td>
    </tr>
    <tr>
      <th>16</th>
      <td>658</td>
      <td>65214</td>
      <td>2.7480</td>
      <td>2014-01-29</td>
      <td>0.857143</td>
      <td>0.833333</td>
    </tr>
    <tr>
      <th>17</th>
      <td>658</td>
      <td>71867</td>
      <td>858.9000</td>
      <td>2014-05-01</td>
      <td>1.000000</td>
      <td>1.000000</td>
    </tr>
  </tbody>
</table>
</div>



### Максимальный и следующий платеж можно сделать через  rank



```python
select  *
from 	(select customer.customer_id , customer.last_name , customer.first_name , payment.payment_date , payment.amount ,
		 rank() over ( partition by payment .customer_id  order by payment.amount desc) as rnk 
		 from payment 
		 	join customer 
		 		on customer.customer_id = payment.customer_id) as top_client
where rnk <=2;
```


```python
query = f'''

select* 
    from (
        select 
        storeid,
        salesorderid,
        subtotal,
        orderdate,
        rank() over (partition by storeid order by subtotal desc) as rnk
        from sales.customer l
        join sales.salesorderheader r
        on l.customerid = r.customerid
        where storeid in  (292, 300,658)
    ) as top_amount
where rnk <=2

'''
df = pd.read_sql_query(query, conn)
df.head(20)
```




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
      <th>rnk</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292</td>
      <td>50756</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292</td>
      <td>48395</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>300</td>
      <td>71805</td>
      <td>57990.6876</td>
      <td>2014-05-01</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>300</td>
      <td>53485</td>
      <td>57771.7641</td>
      <td>2013-07-31</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>658</td>
      <td>53495</td>
      <td>4098.6480</td>
      <td>2013-07-31</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>658</td>
      <td>48329</td>
      <td>1070.0565</td>
      <td>2012-10-30</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



###  Помимо окна можно задать фрейм и считать по фрейму


```python
query = f'''

select 
storeid,
orderdate,
subtotal,
sum(subtotal) over (partition by storeid) as sum_prt,

sum(subtotal) over (partition by storeid 
order by date_trunc('YEAR', orderdate)) as sum_prt_ord,

---- от текущей и до конца фрейма
sum(subtotal) over 
(partition by storeid 
order by date_trunc('YEAR', orderdate) 
rows between current row and unbounded following) as from_cur_to_end,

--считается текущая и все до нее

sum(subtotal) over 
(partition by storeid 
order by date_trunc('YEAR', orderdate) 
rows unbounded preceding) as cur_plus_prev,

--текущая и тре слелующих

sum(subtotal) over 
(partition by storeid 
order by date_trunc('YEAR', orderdate) 
rows between current row and 3 following) as cur_plus_3,

--две предыдущих и три последующих

sum(subtotal) over 
(partition by storeid 
order by date_trunc('YEAR', orderdate) 
rows between 2 preceding and 3 following ) as two_three

from sales.customer l
join sales.salesorderheader r
on l.customerid = r.customerid
where storeid in  (292, 300, 658)

'''
df = pd.read_sql_query(query, conn)
df.head(20)
```




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
      <th>orderdate</th>
      <th>subtotal</th>
      <th>sum_prt</th>
      <th>sum_prt_ord</th>
      <th>from_cur_to_end</th>
      <th>cur_plus_prev</th>
      <th>cur_plus_3</th>
      <th>two_three</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292</td>
      <td>2011-08-01</td>
      <td>4049.9880</td>
      <td>131102.9823</td>
      <td>4049.9880</td>
      <td>131102.9823</td>
      <td>4049.9880</td>
      <td>41797.6266</td>
      <td>41797.6266</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292</td>
      <td>2012-10-30</td>
      <td>32562.6538</td>
      <td>131102.9823</td>
      <td>69227.1560</td>
      <td>127052.9943</td>
      <td>36612.6418</td>
      <td>65177.1680</td>
      <td>69227.1560</td>
    </tr>
    <tr>
      <th>2</th>
      <td>292</td>
      <td>2012-01-29</td>
      <td>4079.9880</td>
      <td>131102.9823</td>
      <td>69227.1560</td>
      <td>94490.3405</td>
      <td>40692.6298</td>
      <td>70257.5751</td>
      <td>106870.2169</td>
    </tr>
    <tr>
      <th>3</th>
      <td>292</td>
      <td>2012-04-30</td>
      <td>1104.9968</td>
      <td>131102.9823</td>
      <td>69227.1560</td>
      <td>90410.3525</td>
      <td>41797.6266</td>
      <td>90410.3525</td>
      <td>127052.9943</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292</td>
      <td>2012-07-31</td>
      <td>27429.5294</td>
      <td>131102.9823</td>
      <td>69227.1560</td>
      <td>89305.3557</td>
      <td>69227.1560</td>
      <td>89305.3557</td>
      <td>94490.3405</td>
    </tr>
    <tr>
      <th>5</th>
      <td>292</td>
      <td>2013-04-30</td>
      <td>37643.0609</td>
      <td>131102.9823</td>
      <td>131102.9823</td>
      <td>61875.8263</td>
      <td>106870.2169</td>
      <td>61875.8263</td>
      <td>90410.3525</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292</td>
      <td>2013-01-28</td>
      <td>24232.7654</td>
      <td>131102.9823</td>
      <td>131102.9823</td>
      <td>24232.7654</td>
      <td>131102.9823</td>
      <td>24232.7654</td>
      <td>89305.3557</td>
    </tr>
    <tr>
      <th>7</th>
      <td>300</td>
      <td>2013-10-30</td>
      <td>49053.4638</td>
      <td>221169.7845</td>
      <td>106825.2279</td>
      <td>221169.7845</td>
      <td>49053.4638</td>
      <td>221169.7845</td>
      <td>221169.7845</td>
    </tr>
    <tr>
      <th>8</th>
      <td>300</td>
      <td>2013-07-31</td>
      <td>57771.7641</td>
      <td>221169.7845</td>
      <td>106825.2279</td>
      <td>172116.3207</td>
      <td>106825.2279</td>
      <td>172116.3207</td>
      <td>221169.7845</td>
    </tr>
    <tr>
      <th>9</th>
      <td>300</td>
      <td>2014-01-29</td>
      <td>56353.8690</td>
      <td>221169.7845</td>
      <td>221169.7845</td>
      <td>114344.5566</td>
      <td>163179.0969</td>
      <td>114344.5566</td>
      <td>221169.7845</td>
    </tr>
    <tr>
      <th>10</th>
      <td>300</td>
      <td>2014-05-01</td>
      <td>57990.6876</td>
      <td>221169.7845</td>
      <td>221169.7845</td>
      <td>57990.6876</td>
      <td>221169.7845</td>
      <td>57990.6876</td>
      <td>172116.3207</td>
    </tr>
    <tr>
      <th>11</th>
      <td>658</td>
      <td>2012-10-30</td>
      <td>1070.0565</td>
      <td>7183.5787</td>
      <td>1272.3885</td>
      <td>7183.5787</td>
      <td>1070.0565</td>
      <td>5935.6605</td>
      <td>5935.6605</td>
    </tr>
    <tr>
      <th>12</th>
      <td>658</td>
      <td>2012-07-31</td>
      <td>202.3320</td>
      <td>7183.5787</td>
      <td>1272.3885</td>
      <td>6113.5222</td>
      <td>1272.3885</td>
      <td>5251.8742</td>
      <td>6321.9307</td>
    </tr>
    <tr>
      <th>13</th>
      <td>658</td>
      <td>2013-10-30</td>
      <td>564.6240</td>
      <td>7183.5787</td>
      <td>6321.9307</td>
      <td>5911.1902</td>
      <td>1837.0125</td>
      <td>5052.2902</td>
      <td>6324.6787</td>
    </tr>
    <tr>
      <th>14</th>
      <td>658</td>
      <td>2013-07-31</td>
      <td>4098.6480</td>
      <td>7183.5787</td>
      <td>6321.9307</td>
      <td>5346.5662</td>
      <td>5935.6605</td>
      <td>5346.5662</td>
      <td>6113.5222</td>
    </tr>
    <tr>
      <th>15</th>
      <td>658</td>
      <td>2013-04-30</td>
      <td>386.2702</td>
      <td>7183.5787</td>
      <td>6321.9307</td>
      <td>1247.9182</td>
      <td>6321.9307</td>
      <td>1247.9182</td>
      <td>5911.1902</td>
    </tr>
    <tr>
      <th>16</th>
      <td>658</td>
      <td>2014-01-29</td>
      <td>2.7480</td>
      <td>7183.5787</td>
      <td>7183.5787</td>
      <td>861.6480</td>
      <td>6324.6787</td>
      <td>861.6480</td>
      <td>5346.5662</td>
    </tr>
    <tr>
      <th>17</th>
      <td>658</td>
      <td>2014-05-01</td>
      <td>858.9000</td>
      <td>7183.5787</td>
      <td>7183.5787</td>
      <td>858.9000</td>
      <td>7183.5787</td>
      <td>858.9000</td>
      <td>1247.9182</td>
    </tr>
  </tbody>
</table>
</div>




```python
858.9000+2.7480+386.2702
```




    1247.9182



### Агрегатная функция с ORDER BY и определением рамки окна по умолчанию будет вычисляться как «бегущая сумма»
### Чтобы агрегатная функция работала со всем разделом, следует опустить ORDER BY или использовать ROWS BETWEEN 


```python
query = f'''

        select 
        storeid,
        salesorderid,
        subtotal,
        orderdate,
        
        -- от текущей и до конца
        sum(subtotal) over (partition by storeid 
        order  by orderdate range 
        between current row and unbounded following) as range_forward,
        
        -- все предыдущие до текущей
        sum(subtotal) over (partition by storeid 
        order  by orderdate 
        range between unbounded preceding and current row) as range_back


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
      <th>range_forward</th>
      <th>range_back</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292</td>
      <td>44132</td>
      <td>4049.9880</td>
      <td>2011-08-01</td>
      <td>131102.9823</td>
      <td>4049.9880</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292</td>
      <td>45579</td>
      <td>4079.9880</td>
      <td>2012-01-29</td>
      <td>127052.9943</td>
      <td>8129.9760</td>
    </tr>
    <tr>
      <th>2</th>
      <td>292</td>
      <td>46389</td>
      <td>1104.9968</td>
      <td>2012-04-30</td>
      <td>122973.0063</td>
      <td>9234.9728</td>
    </tr>
    <tr>
      <th>3</th>
      <td>292</td>
      <td>47454</td>
      <td>27429.5294</td>
      <td>2012-07-31</td>
      <td>121868.0095</td>
      <td>36664.5022</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292</td>
      <td>48395</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
      <td>94438.4801</td>
      <td>69227.1560</td>
    </tr>
    <tr>
      <th>5</th>
      <td>292</td>
      <td>49495</td>
      <td>24232.7654</td>
      <td>2013-01-28</td>
      <td>61875.8263</td>
      <td>93459.9214</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292</td>
      <td>50756</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
      <td>37643.0609</td>
      <td>131102.9823</td>
    </tr>
    <tr>
      <th>7</th>
      <td>300</td>
      <td>53485</td>
      <td>57771.7641</td>
      <td>2013-07-31</td>
      <td>221169.7845</td>
      <td>57771.7641</td>
    </tr>
    <tr>
      <th>8</th>
      <td>300</td>
      <td>58931</td>
      <td>49053.4638</td>
      <td>2013-10-30</td>
      <td>163398.0204</td>
      <td>106825.2279</td>
    </tr>
    <tr>
      <th>9</th>
      <td>300</td>
      <td>65191</td>
      <td>56353.8690</td>
      <td>2014-01-29</td>
      <td>114344.5566</td>
      <td>163179.0969</td>
    </tr>
    <tr>
      <th>10</th>
      <td>300</td>
      <td>71805</td>
      <td>57990.6876</td>
      <td>2014-05-01</td>
      <td>57990.6876</td>
      <td>221169.7845</td>
    </tr>
    <tr>
      <th>11</th>
      <td>658</td>
      <td>47388</td>
      <td>202.3320</td>
      <td>2012-07-31</td>
      <td>7183.5787</td>
      <td>202.3320</td>
    </tr>
    <tr>
      <th>12</th>
      <td>658</td>
      <td>48329</td>
      <td>1070.0565</td>
      <td>2012-10-30</td>
      <td>6981.2467</td>
      <td>1272.3885</td>
    </tr>
    <tr>
      <th>13</th>
      <td>658</td>
      <td>50679</td>
      <td>386.2702</td>
      <td>2013-04-30</td>
      <td>5911.1902</td>
      <td>1658.6587</td>
    </tr>
    <tr>
      <th>14</th>
      <td>658</td>
      <td>53495</td>
      <td>4098.6480</td>
      <td>2013-07-31</td>
      <td>5524.9200</td>
      <td>5757.3067</td>
    </tr>
    <tr>
      <th>15</th>
      <td>658</td>
      <td>58941</td>
      <td>564.6240</td>
      <td>2013-10-30</td>
      <td>1426.2720</td>
      <td>6321.9307</td>
    </tr>
    <tr>
      <th>16</th>
      <td>658</td>
      <td>65214</td>
      <td>2.7480</td>
      <td>2014-01-29</td>
      <td>861.6480</td>
      <td>6324.6787</td>
    </tr>
    <tr>
      <th>17</th>
      <td>658</td>
      <td>71867</td>
      <td>858.9000</td>
      <td>2014-05-01</td>
      <td>858.9000</td>
      <td>7183.5787</td>
    </tr>
  </tbody>
</table>
</div>



### exclude - Исключение в разделе 


```python
query = f'''

        select 
        storeid,
        salesorderid,
        subtotal,
        orderdate,
        sum(subtotal) over (partition by storeid) as total,
        
        
        -- exclude
        -- одна предыдущая и следущая без текущей 
        sum(subtotal) over (partition by storeid 
        order by subtotal 
        rows BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) as excl 
       
        from sales.customer l
        join sales.salesorderheader r
        on l.customerid = r.customerid
        where storeid in  (292, 300,658)

'''
df = pd.read_sql_query(query, conn)
df.head(20)
```




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
      <th>total</th>
      <th>excl</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>292</td>
      <td>46389</td>
      <td>1104.9968</td>
      <td>2012-04-30</td>
      <td>131102.9823</td>
      <td>4049.9880</td>
    </tr>
    <tr>
      <th>1</th>
      <td>292</td>
      <td>44132</td>
      <td>4049.9880</td>
      <td>2011-08-01</td>
      <td>131102.9823</td>
      <td>5184.9848</td>
    </tr>
    <tr>
      <th>2</th>
      <td>292</td>
      <td>45579</td>
      <td>4079.9880</td>
      <td>2012-01-29</td>
      <td>131102.9823</td>
      <td>28282.7534</td>
    </tr>
    <tr>
      <th>3</th>
      <td>292</td>
      <td>49495</td>
      <td>24232.7654</td>
      <td>2013-01-28</td>
      <td>131102.9823</td>
      <td>31509.5174</td>
    </tr>
    <tr>
      <th>4</th>
      <td>292</td>
      <td>47454</td>
      <td>27429.5294</td>
      <td>2012-07-31</td>
      <td>131102.9823</td>
      <td>56795.4192</td>
    </tr>
    <tr>
      <th>5</th>
      <td>292</td>
      <td>48395</td>
      <td>32562.6538</td>
      <td>2012-10-30</td>
      <td>131102.9823</td>
      <td>65072.5903</td>
    </tr>
    <tr>
      <th>6</th>
      <td>292</td>
      <td>50756</td>
      <td>37643.0609</td>
      <td>2013-04-30</td>
      <td>131102.9823</td>
      <td>32562.6538</td>
    </tr>
    <tr>
      <th>7</th>
      <td>300</td>
      <td>58931</td>
      <td>49053.4638</td>
      <td>2013-10-30</td>
      <td>221169.7845</td>
      <td>56353.8690</td>
    </tr>
    <tr>
      <th>8</th>
      <td>300</td>
      <td>65191</td>
      <td>56353.8690</td>
      <td>2014-01-29</td>
      <td>221169.7845</td>
      <td>106825.2279</td>
    </tr>
    <tr>
      <th>9</th>
      <td>300</td>
      <td>53485</td>
      <td>57771.7641</td>
      <td>2013-07-31</td>
      <td>221169.7845</td>
      <td>114344.5566</td>
    </tr>
    <tr>
      <th>10</th>
      <td>300</td>
      <td>71805</td>
      <td>57990.6876</td>
      <td>2014-05-01</td>
      <td>221169.7845</td>
      <td>57771.7641</td>
    </tr>
    <tr>
      <th>11</th>
      <td>658</td>
      <td>65214</td>
      <td>2.7480</td>
      <td>2014-01-29</td>
      <td>7183.5787</td>
      <td>202.3320</td>
    </tr>
    <tr>
      <th>12</th>
      <td>658</td>
      <td>47388</td>
      <td>202.3320</td>
      <td>2012-07-31</td>
      <td>7183.5787</td>
      <td>389.0182</td>
    </tr>
    <tr>
      <th>13</th>
      <td>658</td>
      <td>50679</td>
      <td>386.2702</td>
      <td>2013-04-30</td>
      <td>7183.5787</td>
      <td>766.9560</td>
    </tr>
    <tr>
      <th>14</th>
      <td>658</td>
      <td>58941</td>
      <td>564.6240</td>
      <td>2013-10-30</td>
      <td>7183.5787</td>
      <td>1245.1702</td>
    </tr>
    <tr>
      <th>15</th>
      <td>658</td>
      <td>71867</td>
      <td>858.9000</td>
      <td>2014-05-01</td>
      <td>7183.5787</td>
      <td>1634.6805</td>
    </tr>
    <tr>
      <th>16</th>
      <td>658</td>
      <td>48329</td>
      <td>1070.0565</td>
      <td>2012-10-30</td>
      <td>7183.5787</td>
      <td>4957.5480</td>
    </tr>
    <tr>
      <th>17</th>
      <td>658</td>
      <td>53495</td>
      <td>4098.6480</td>
      <td>2013-07-31</td>
      <td>7183.5787</td>
      <td>1070.0565</td>
    </tr>
  </tbody>
</table>
</div>


