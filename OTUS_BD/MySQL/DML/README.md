```python
import pandas as pd
import mysql.connector
import sqlalchemy
```


```python
cnx = mysql.connector.connect(user='root', password='password', host='127.0.0.1', database='otus_db', port='3306')
```


```python
cnx.close()
cur.close()
```




    True




```python
cur = cnx.cursor()
```

#### Дроп / создание таблицы customers (клиенты)
##### customer_id -  id клиента
##### email -  почта клиента
##### gender -  пол клиента
##### city -  город клиента


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

#### Наполнение таблицы customers


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



#### Дроп / создание таблицы sessions (сессии клиентов)
##### session_id - id сессии 
##### customer_id -  id клиента
##### email -  почта клиента
##### gender -  пол клиента
##### city -  город клиента


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

#### Наполнение таблицы sessions


```python
cur.execute('''
           INSERT  INTO sessions
           (customer_id, visit_dttm, Purchase_flg)
            VALUES
            ('1', now() - INTERVAL 1 DAY , 0),
            ('2', now() - INTERVAL 1 DAY, 0),
            ('3', now() - INTERVAL 1 DAY, 0),
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
      <td>7</td>
      <td>1</td>
      <td>2023-09-04 18:16:25</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>8</td>
      <td>2</td>
      <td>2023-09-04 18:16:25</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9</td>
      <td>3</td>
      <td>2023-09-04 18:16:25</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10</td>
      <td>1</td>
      <td>2023-09-05 18:16:25</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11</td>
      <td>2</td>
      <td>2023-09-05 18:16:25</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>12</td>
      <td>3</td>
      <td>2023-09-05 18:16:25</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



#### inner join
##### Посмотреть данные клиентов и сессий с флагом покупки 


```python
query = f'''
            SELECT * from customers l
            join sessions r
             on l.customer_id = r.customer_id
             where Purchase_flg = 1;
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
      <td>dmitry@example.com</td>
      <td>М</td>
      <td>Зеленоград</td>
      <td>10</td>
      <td>1</td>
      <td>2023-09-05 18:16:25</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>olga@example.com</td>
      <td>Ж</td>
      <td>Волгоград</td>
      <td>12</td>
      <td>3</td>
      <td>2023-09-05 18:16:25</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



#### left join 
##### Посмотреть данные клиентов у которых нет сессий


```python
cur.execute('''
           INSERT  INTO customers
           (email, gender, city)
            VALUES
            ('denis@example.com', 'М', 'Питер');
            ''')
```


```python
query = f'''
            SELECT * from customers l
            left join sessions r
             on l.customer_id = r.customer_id
             where session_id is null;
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
      <th>session_id</th>
      <th>customer_id</th>
      <th>visit_dttm</th>
      <th>Purchase_flg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4</td>
      <td>denis@example.com</td>
      <td>М</td>
      <td>Питер</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



#### 5 запросов с WHERE с использованием разных операторов

#### 1) Создадим агрегат витрину с количеством покупок у клиентов и зальем в нее данные 


```python
cur.execute('''
            CREATE TABLE p_sessions_count (
            customer_id BIGINT UNSIGNED NOT NULL,
            count BIGINT UNSIGNED NOT NULL,
            CONSTRAINT fk__p_sessions_count__customers
                FOREIGN KEY (customer_id)
                REFERENCES customers (customer_id)
            );
            ''')
```


```python
cur.execute('''
            INSERT INTO p_sessions_count
            (customer_id, count)
            SELECT 
                customer_id,
                count(*)
            FROM sessions
            WHERE Purchase_flg = 1
            GROUP BY customer_id;
            ''')

query = f'''
            SELECT * from p_sessions_count
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
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



#### 2) Обновим город клиента


```python
cur.execute('''
            UPDATE customers
            SET city = 'Москва'
            WHERE customer_id = 4
            ''')

query = f'''
            SELECT * from customers
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
    <tr>
      <th>3</th>
      <td>4</td>
      <td>denis@example.com</td>
      <td>М</td>
      <td>Москва</td>
    </tr>
  </tbody>
</table>
</div>



#### 3) Удалить клиента из таблицы


```python
cur.execute('''
            DELETE FROM customers l
             where customer_id = 4;
            ''')

query = f'''
            SELECT * from customers
'''
df = pd.read_sql_query(query, cnx)
df
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



#### 4) Вывести уникальные id и email клиентов у которых дата визита меньше now() и город не Москва


```python
query = f'''
            select customer_id, email
                from customers c
                join sessions s
                using(customer_id)
                where visit_dttm < now() 
                and city not in ('Москва')
                group by customer_id, email
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
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>dmitry@example.com</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>olga@example.com</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''
              select * 
            from sessions
            where Purchase_flg = 1
            ORDER BY customer_id desc
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
      <td>12</td>
      <td>3</td>
      <td>2023-09-05 18:16:25</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>10</td>
      <td>1</td>
      <td>2023-09-05 18:16:25</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



##### 5) Вывести данные по предпоследнему id клиента с флагом покупки 


```python
query = f'''
            select * 
            from sessions
            where Purchase_flg = 1
            ORDER BY customer_id desc
            LIMIT 1, 1;
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
      <td>10</td>
      <td>1</td>
      <td>2023-09-05 18:16:25</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>


