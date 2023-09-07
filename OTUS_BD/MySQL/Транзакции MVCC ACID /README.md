```python
import pandas as pd
import mysql.connector
import sqlalchemy
```


```python
cnx = mysql.connector.connect(user='root', password='password', host='127.0.0.1', database='otus_db', port='3306')
```


```python
cur = cnx.cursor()
```


```python
cur.execute('''
            DROP TABLE IF EXISTS customers;
            ''')

cur.execute('''
            CREATE TABLE customers (
            customer_id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            gender CHAR(1) NOT NULL,
            city VARCHAR(255)
            );
            ''')
```


```python
cur.execute('''
           INSERT  INTO customers
           (email, gender, city)
            VALUES
            ('dmitry@example.com', 'М', 'Зеленоград'),
            ('vasya@example.com', 'М', 'Москва'),
            ('olga@example.com', 'Ж', 'Волгоград');
            ''')

query = f'''
            SELECT * from customers;
'''
df = pd.read_sql_query(query, cnx)
df
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
      <th>customer_id</th>
      <th>email</th>
      <th>gender</th>
      <th>city</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>dmitry@example.com</td>
      <td>М</td>
      <td>Зеленоград</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>vasya@example.com</td>
      <td>М</td>
      <td>Москва</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>olga@example.com</td>
      <td>Ж</td>
      <td>Волгоград</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            DROP TABLE IF EXISTS sessions;
            ''')

cur.execute('''
            CREATE TABLE sessions (
                session_id SERIAL PRIMARY KEY,
                customer_id  BIGINT UNSIGNED NOT NULL,
                visit_dttm DATETIME NOT null,
                Purchase_flg TINYINT(1) NOT null,
            CONSTRAINT fk__sessions__customers
                FOREIGN KEY (customer_id)
                REFERENCES customers (customer_id)
            );
            ''')
```


```python
cur.execute('''
           INSERT  INTO sessions
           (customer_id, visit_dttm, Purchase_flg)
            VALUES
            ('1', now() - INTERVAL 1 DAY , 1),
            ('2', now() - INTERVAL 1 DAY, 0),
            ('3', now() - INTERVAL 1 DAY, 1),
            ('1', now(), 1),
            ('2', now(), 0),
            ('3', now(), 1);;
            ''')

query = f'''
            SELECT * from sessions;
'''
df = pd.read_sql_query(query, cnx)
df
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
      <th>session_id</th>
      <th>customer_id</th>
      <th>visit_dttm</th>
      <th>Purchase_flg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>2023-09-06 16:54:20</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2</td>
      <td>2023-09-06 16:54:20</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>3</td>
      <td>2023-09-06 16:54:20</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>1</td>
      <td>2023-09-07 16:54:20</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2</td>
      <td>2023-09-07 16:54:20</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>3</td>
      <td>2023-09-07 16:54:20</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            DROP TABLE IF EXISTS customers_purchases;
            ''')

cur.execute('''
            CREATE TABLE customers_purchases (
            date DATETIME NOT null,
            customer_id VARCHAR(255)NOT NULL,
            count_purchases BIGINT UNSIGNED NOT NULL
            );
            ''')
```

#### Процедура по заливке агрегированных данных по кол-ву покупок каждого клиента на текущую дату 


```python
cur.execute('''
                    drop procedure if exists customers_purchases_insert;
            ''')

cur.execute('''
                    CREATE procedure  customers_purchases_insert()
                    BEGIN
                    INSERT  INTO customers_purchases
                   (date, customer_id, count_purchases)
                      SELECT 
                       now() as date,
                        IF(GROUPING(l.customer_id), 'ИТОГО', l.customer_id) AS customer_id,
                        count(*) as count_purchases
                        from customers l
                        join sessions r
                         on l.customer_id = r.customer_id
                         where Purchase_flg = 1
                         group by l.customer_id WITH ROLLUP;
                    END;
            ''')

```

#### Отключим автокоммит и сделаем ручной коммит в конце


```python
cur.execute('''
             set autocommit = 0;
            ''')

cur.execute('''
             call customers_purchases_insert();
            ''')

cur.execute('''
             select * from customers_purchases;
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['date', 'customer_id', 'count_purchases'])

cur.execute('''
             commit(); 
            ''')
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
      <th>date</th>
      <th>customer_id</th>
      <th>count_purchases</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2023-09-07 16:59:05</td>
      <td>1</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2023-09-07 16:59:05</td>
      <td>3</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2023-09-07 16:59:05</td>
      <td>ИТОГО</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



#### Если не делать ручной коммит и закрыть соединение


```python
cnx.close()
```


```python
query = f'''
select * from customers_purchases;
'''
df = pd.read_sql_query(query, cnx)
df
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
      <th>date</th>
      <th>customer_id</th>
      <th>count_purchases</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>



#### Загрузить данные используя LOAD DATA 


```python
cur.execute('''
            drop TABLE if exists test_load;
            ''')

cur.execute('''
            CREATE TABLE if not exists test_load (
                hz_1 VARCHAR(255), 
                hz_2 VARCHAR(255), 
                hz_3 VARCHAR(255), 
                hz_4 VARCHAR(255));
            ''')

cur.execute('''
           LOAD DATA INFILE '/var/lib/mysql-files/Apparel.csv'
            IGNORE INTO TABLE test_load
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\n';
            ''')
```


```python
query = f'''
select * from test_load;
'''
df = pd.read_sql_query(query, cnx)
df
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
      <th>hz_1</th>
      <th>hz_2</th>
      <th>hz_3</th>
      <th>hz_4</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Handle</td>
      <td>Title</td>
      <td>Body (HTML)</td>
      <td>Vendor</td>
    </tr>
    <tr>
      <th>1</th>
      <td>the-scout-skincare-kit</td>
      <td>The Scout Skincare Kit</td>
      <td>"&lt;p&gt;&lt;em&gt;This is a demonstration store. You can...</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>&lt;p&gt;&lt;span&gt;A collection of the best Ursa Major h...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>&lt;ul&gt;</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>&lt;li&gt;&lt;span style=""line-height: 1.4;""&gt;Face Was...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>252</th>
      <td>&lt;/ul&gt;</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>253</th>
      <td>&lt;ul class=""tabs-content""&gt;&lt;/ul&gt;"</td>
      <td>United By Blue</td>
      <td>Bags</td>
      <td>Bags</td>
    </tr>
    <tr>
      <th>254</th>
      <td>hudderton-backpack</td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>255</th>
      <td>hudderton-backpack</td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>256</th>
      <td>hudderton-backpack</td>
      <td></td>
      <td></td>
      <td></td>
    </tr>
  </tbody>
</table>
<p>257 rows × 4 columns</p>
</div>


