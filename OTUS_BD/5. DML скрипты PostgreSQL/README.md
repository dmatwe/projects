### Импорт библиотек для работы с бд и дф


```python
import psycopg2
import pandas as pd
import sqlalchemy
```

### Кредиты для коннекта к бд



```python
conn = psycopg2.connect(host="127.0.0.1", port="5433", database="online_store", 
                            user="admin", password="root")
```

### Наполняем таблицу clients.sessions_info 


```python
cur = conn.cursor()
cur.execute('''
            insert  into clients.sessions_info 
            (session_id, client_id, src_id, Purchase_flg, visit_dttm, duration, page_count, last_page_id)
            values 
            (2232, 100, 2, 1, '2022-09-25', 15, 4, 5),
            (4232, 200, 1, 0, '2022-09-26', 2.2, 12, 1),
            (5232, 300, 3, 1, '2022-09-24', 10, 2, 4),
            (7232, 300, 3, 0, '2022-09-20', 10, 2, 4),
            (6232, 400, 3, 1, '2022-09-21', 10, 2, 4);
            ''')

cur.execute('''
           select* from clients.sessions_info 
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['session_id', 'client_id', 'src_id', 'Purchase_flg', 'visit_dttm', 'duration', 
                              'page_count', 'last_page_id'])
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
      <th>session_id</th>
      <th>client_id</th>
      <th>src_id</th>
      <th>Purchase_flg</th>
      <th>visit_dttm</th>
      <th>duration</th>
      <th>page_count</th>
      <th>last_page_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2232</td>
      <td>100</td>
      <td>2</td>
      <td>1</td>
      <td>2022-09-25</td>
      <td>15.0000000000</td>
      <td>4</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4232</td>
      <td>200</td>
      <td>1</td>
      <td>0</td>
      <td>2022-09-26</td>
      <td>2.2000000000</td>
      <td>12</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5232</td>
      <td>300</td>
      <td>3</td>
      <td>1</td>
      <td>2022-09-24</td>
      <td>10.0000000000</td>
      <td>2</td>
      <td>4</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7232</td>
      <td>300</td>
      <td>3</td>
      <td>0</td>
      <td>2022-09-20</td>
      <td>10.0000000000</td>
      <td>2</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>6232</td>
      <td>400</td>
      <td>3</td>
      <td>1</td>
      <td>2022-09-21</td>
      <td>10.0000000000</td>
      <td>2</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



### Наполняем таблицу clients.clients_info 


```python
cur.execute('''
            insert  into clients.clients_info 
            (client_id, client_name, src_id, status_id, city, address, registration_dttm, start_dttm, end_dttm)
            values
            (100, 'Andrew Link', 2, 1, 'Moscow', 'Ul. Zatonnaia 6k1', '2022-08-25', '2022-08-25', '3000-01-01'),
            (200, 'German Gaban', 1, 1, 'Saint-Petersburg', 'Ul. Hermulich 15', '2022-08-24', '2022-08-24', '3000-01-01'),
            (300, 'Fedor Vlasov', 1, 1, 'Tver', 'Ul. Dorojnaia 8', '2022-08-24', '2022-08-24', '3000-01-01'),
            (400, 'Anna Vlasova', 3, 1, 'Tver', 'Ul. Dorojnaia 8', '2022-08-21', '2022-08-21', '3000-01-01');
            ''')

cur.execute('''
           select* from clients.clients_info 
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['client_id', 'client_name', 'src_id', 'status_id', 'city', 'address', 
                              'registration_dttm', 'start_dttm', 'end_dttm'])

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
      <th>client_id</th>
      <th>client_name</th>
      <th>src_id</th>
      <th>status_id</th>
      <th>city</th>
      <th>address</th>
      <th>registration_dttm</th>
      <th>start_dttm</th>
      <th>end_dttm</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>100</td>
      <td>Andrew Link</td>
      <td>2</td>
      <td>1</td>
      <td>Moscow</td>
      <td>Ul. Zatonnaia 6k1</td>
      <td>2022-08-25</td>
      <td>2022-08-25</td>
      <td>3000-01-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>200</td>
      <td>German Gaban</td>
      <td>1</td>
      <td>1</td>
      <td>Saint-Petersburg</td>
      <td>Ul. Hermulich 15</td>
      <td>2022-08-24</td>
      <td>2022-08-24</td>
      <td>3000-01-01</td>
    </tr>
    <tr>
      <th>2</th>
      <td>300</td>
      <td>Fedor Vlasov</td>
      <td>1</td>
      <td>1</td>
      <td>Tver</td>
      <td>Ul. Dorojnaia 8</td>
      <td>2022-08-24</td>
      <td>2022-08-24</td>
      <td>3000-01-01</td>
    </tr>
    <tr>
      <th>3</th>
      <td>400</td>
      <td>Anna Vlasova</td>
      <td>3</td>
      <td>1</td>
      <td>Tver</td>
      <td>Ul. Dorojnaia 8</td>
      <td>2022-08-21</td>
      <td>2022-08-21</td>
      <td>3000-01-01</td>
    </tr>
  </tbody>
</table>
</div>



### Наполняем таблицу orders.orders_info 


```python
cur.execute('''
            insert into orders.orders_info
            (order_id, client_id, status_id, session_id, amount, discount_flg, order_dttm, start_dttm, end_dttm)
            values
            (2367742, 100, 1, 2232, 3500, 0, '2022-09-25', '2022-09-25', '3000-01-01'),
            (3327834, 300, 1, 5232, 6000, 0, '2022-09-24', '2022-09-24', '3000-01-01'),
            (4834594, 400, 1, 6232, 1500, 0, '2022-09-21', '2022-09-21', '3000-01-01');
            ''')

cur.execute('''
            select* from orders.orders_info 
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['order_id', 'client_id', 'status_id', 'session_id', 'amount', 'discount_flg', 
                              'order_dttm', 'start_dttm', 'end_dttm'])

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
      <th>order_id</th>
      <th>client_id</th>
      <th>status_id</th>
      <th>session_id</th>
      <th>amount</th>
      <th>discount_flg</th>
      <th>order_dttm</th>
      <th>start_dttm</th>
      <th>end_dttm</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2367742</td>
      <td>100</td>
      <td>1</td>
      <td>2232</td>
      <td>3500.0000000000</td>
      <td>0</td>
      <td>2022-09-25</td>
      <td>2022-09-25</td>
      <td>3000-01-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3327834</td>
      <td>300</td>
      <td>1</td>
      <td>5232</td>
      <td>6000.0000000000</td>
      <td>0</td>
      <td>2022-09-24</td>
      <td>2022-09-24</td>
      <td>3000-01-01</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4834594</td>
      <td>400</td>
      <td>1</td>
      <td>6232</td>
      <td>1500.0000000000</td>
      <td>0</td>
      <td>2022-09-21</td>
      <td>2022-09-21</td>
      <td>3000-01-01</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.commit()
cur.close()
conn.close()
```

### Запрос с регулярным выражением для поиска клиентов с фамилиией Vlasov


```python
query = '''
 
        select * from clients.clients_info 
        where client_name ilike('%vlasov%')
  
        '''
pd.read_sql_query(query, conn, dtype='object')
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
      <th>client_id</th>
      <th>client_name</th>
      <th>src_id</th>
      <th>status_id</th>
      <th>city</th>
      <th>address</th>
      <th>registration_dttm</th>
      <th>start_dttm</th>
      <th>end_dttm</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>300</td>
      <td>Fedor Vlasov</td>
      <td>1</td>
      <td>1</td>
      <td>Tver</td>
      <td>Ul. Dorojnaia 8</td>
      <td>2022-08-24</td>
      <td>2022-08-24</td>
      <td>3000-01-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>400</td>
      <td>Anna Vlasova</td>
      <td>3</td>
      <td>1</td>
      <td>Tver</td>
      <td>Ul. Dorojnaia 8</td>
      <td>2022-08-21</td>
      <td>2022-08-21</td>
      <td>3000-01-01</td>
    </tr>
  </tbody>
</table>
</div>



## INNER JOIN 
### Объединяет записи из двух таблиц, если в связующих полях этих таблиц содержатся одинаковые значения


```python
query = '''
 
        select 
        c.client_name,
        c.src_id,
        o.order_dttm
        from clients.clients_info c
        join orders.orders_info o
        on c.client_id = o.client_id
  
        '''
pd.read_sql_query(query, conn, dtype='object')
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
      <th>client_name</th>
      <th>src_id</th>
      <th>order_dttm</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Andrew Link</td>
      <td>2</td>
      <td>2022-09-25</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Fedor Vlasov</td>
      <td>1</td>
      <td>2022-09-24</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Anna Vlasova</td>
      <td>3</td>
      <td>2022-09-21</td>
    </tr>
  </tbody>
</table>
</div>



## LEFT JOIN
### Cоздает левое внешнее соединение. С помощью левого внешнего соединения выбираются все записи первой (левой) таблицы, даже если они не соответствуют записям во второй (правой) таблице


```python
query = '''
 
        select 
        c.client_name,
        c.src_id,
        o.order_dttm
        from clients.clients_info c
        left join orders.orders_info o
        on c.client_id = o.client_id
  
        '''
pd.read_sql_query(query, conn, dtype='object')
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
      <th>client_name</th>
      <th>src_id</th>
      <th>order_dttm</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Andrew Link</td>
      <td>2</td>
      <td>2022-09-25</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Fedor Vlasov</td>
      <td>1</td>
      <td>2022-09-24</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Anna Vlasova</td>
      <td>3</td>
      <td>2022-09-21</td>
    </tr>
    <tr>
      <th>3</th>
      <td>German Gaban</td>
      <td>1</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



### Запрос с обновлением данных используя UPDATE WHERE


```python
cur = conn.cursor()
cur.execute('''
            update clients.clients_info c
            set status_id = 2
            where client_id in
            (select distinct client_id
            from orders.orders_info)
            returning c.client_name, c.status_id
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['client_name', 'status_id'])
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
      <th>client_name</th>
      <th>status_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Anna Vlasova</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Fedor Vlasov</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Andrew Link</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



### Запрос с добавление нового атрибута для подсчета кол-ва сессий и с обновлением данных используя UPDATE FROM


```python
cur = conn.cursor()
cur.execute('''
            alter table clients.clients_info 
            add column sessions_count int;
            ''')

cur.execute('''
           UPDATE clients.clients_info as c
           set sessions_count = s.sessions_count
           from 
           (select client_id, count(session_id) as sessions_count 
           from clients.sessions_info 
           group by client_id) as s
           where c.client_id = s.client_id
           returning c.client_id, c.sessions_count
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['client_id', 'sessions_count'])
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
      <th>client_id</th>
      <th>sessions_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>200</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>400</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>300</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>100</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



### Запрос на добавление данных с выводом информации о добавленных строках


```python
cur = conn.cursor()
cur.execute('''
            insert into clients.clients_info 
            (client_id, client_name, src_id, status_id, city, address, registration_dttm, start_dttm, end_dttm)
            values
            (500, 'Alex Link', 2, 1, 'Moscow', 'Ul. Zatonnaia 6k1', '2022-08-25', '2022-08-25', '3000-01-01')
            returning client_id, client_name
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['client_id', 'client_name'])
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
      <th>client_id</th>
      <th>client_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>500</td>
      <td>Alex Link</td>
    </tr>
  </tbody>
</table>
</div>



### Запрос для удаления данных с оператором DELETE используя join с другой таблицей с помощью using


```python
cur = conn.cursor()
cur.execute('''
            delete from clients.sessions_info s
            using clients.clients_info c
            where s.client_id = c.client_id
            and c.src_id = 1
            returning c.client_id
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['client_id'])
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
      <th>client_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>200</td>
    </tr>
    <tr>
      <th>1</th>
      <td>300</td>
    </tr>
    <tr>
      <th>2</th>
      <td>300</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.rollback()
conn.close()
```
