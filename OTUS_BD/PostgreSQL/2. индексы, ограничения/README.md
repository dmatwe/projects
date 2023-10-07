### Импорт библиотек для работы с бд и дф


```python
import psycopg2
import pandas as pd
```

### Кредиты для коннекта к бд


```python
conn = psycopg2.connect(host="127.0.0.1", port="5432", database="postgres", 
                            user="admin", password="root")
```

### Создаем таблицу fact_orders - фактовая сущность заказов клиентов
#### order_id - Идентификатор заказа, не может быть пустым, первичный ключ, часть состовного ключа, кардинальность - низкая 
#### amount - Сумма заказа, не может быть пустым, сумма заказа должна быть > 0, кардинальность - высокая 
#### status_id - Идентификатор статусу заказа, не может быть пустым, кардинальность - низкая 
#### order_dttm - Дата заказа, не может быть пустым, кардинальность - высокая 
#### start_dttm - Дата актуальности записи, не может быть пустым, первичный ключ, часть состовного ключа, кардинальность - высокая 


```python
cur = conn.cursor()
cur.execute('''
            DROP TABLE IF EXISTS fact_orders 
            ''')

cur.execute('''
           CREATE TABLE fact_orders (
           order_id INTEGER NOT NULL,
           amount NUMERIC(100,10) CHECK (amount > 0) NOT NULL,
           status_id VARCHAR(20) NOT NULL,
           order_dttm DATE NOT NULL,
           start_dttm DATE NOT NULL,
           PRIMARY KEY(order_id, start_dttm));
            ''')
```

### Наполняем таблицу fact_orders данными


```python
cur.execute('''
            INSERT INTO fact_orders
            (order_id, amount, status_id, order_dttm, start_dttm)
            values
            (111, 5500.5, 'confirmed', '2022-09-25', '2022-09-25'),
            (111, 5500.5, 'delivered', '2022-09-25', '2022-09-26'),
            (111, 5500.5, 'canceled', '2022-09-25', '3000-12-31'),

            (222, 15500.5, 'confirmed', '2022-09-25', '2022-09-27'),
            (222, 15500.5, 'delivered', '2022-09-25', '3000-12-31'),

            (333, 125500.5, 'confirmed', '2022-09-28', '2022-09-28'),
            (333, 125500.5, 'delivered', '2022-09-28', '3000-12-31'),

            (444, 25500.5, 'confirmed', '2022-09-29', '2022-09-29'),
            (444, 25500.5, 'delivered', '2022-09-29', '2022-09-30'),
            (444, 25500.5, 'canceled', '2022-09-29', '3000-12-31');
            ''')
```

### Select из таблицы fact_orders


```python
cur.execute('''
            select * from fact_orders
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['order_id', 'amount', 'status_id', 'order_dttm', 'start_dttm'])
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
      <th>amount</th>
      <th>status_id</th>
      <th>order_dttm</th>
      <th>start_dttm</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>111</td>
      <td>5500.5000000000</td>
      <td>confirmed</td>
      <td>2022-09-25</td>
      <td>2022-09-25</td>
    </tr>
    <tr>
      <th>1</th>
      <td>111</td>
      <td>5500.5000000000</td>
      <td>delivered</td>
      <td>2022-09-25</td>
      <td>2022-09-26</td>
    </tr>
    <tr>
      <th>2</th>
      <td>111</td>
      <td>5500.5000000000</td>
      <td>canceled</td>
      <td>2022-09-25</td>
      <td>3000-12-31</td>
    </tr>
    <tr>
      <th>3</th>
      <td>222</td>
      <td>15500.5000000000</td>
      <td>confirmed</td>
      <td>2022-09-25</td>
      <td>2022-09-27</td>
    </tr>
    <tr>
      <th>4</th>
      <td>222</td>
      <td>15500.5000000000</td>
      <td>delivered</td>
      <td>2022-09-25</td>
      <td>3000-12-31</td>
    </tr>
    <tr>
      <th>5</th>
      <td>333</td>
      <td>125500.5000000000</td>
      <td>confirmed</td>
      <td>2022-09-28</td>
      <td>2022-09-28</td>
    </tr>
    <tr>
      <th>6</th>
      <td>333</td>
      <td>125500.5000000000</td>
      <td>delivered</td>
      <td>2022-09-28</td>
      <td>3000-12-31</td>
    </tr>
    <tr>
      <th>7</th>
      <td>444</td>
      <td>25500.5000000000</td>
      <td>confirmed</td>
      <td>2022-09-29</td>
      <td>2022-09-29</td>
    </tr>
    <tr>
      <th>8</th>
      <td>444</td>
      <td>25500.5000000000</td>
      <td>delivered</td>
      <td>2022-09-29</td>
      <td>2022-09-30</td>
    </tr>
    <tr>
      <th>9</th>
      <td>444</td>
      <td>25500.5000000000</td>
      <td>canceled</td>
      <td>2022-09-29</td>
      <td>3000-12-31</td>
    </tr>
  </tbody>
</table>
</div>



### Аналитический запрос:
#### Показать id заказов и сумму заказов начиная с 2022-09-25 имеющие статус delivered


```python
cur.execute('''
            select 
            order_id,
            amount
            from fact_orders
            where amount >= 10000
            and status_id ilike ('delivered')
            and order_dttm >= '2022-09-25'
            and start_dttm >= now();
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['order_id', 'amount'])
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
      <th>amount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>222</td>
      <td>15500.5000000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>333</td>
      <td>125500.5000000000</td>
    </tr>
  </tbody>
</table>
</div>



### Обновление статистики


```python
cur.execute('''
            analyze fact_orders
            ''')
```

### Смотрим время выполнения запроса


```python
cur.execute('''
            explain analyze
            select 
            order_id,
            amount
            from fact_orders
            where amount >= 10000
            and status_id ilike ('delivered')
            and order_dttm >= '2022-09-25'
            and start_dttm >= now();
            ''')
cur.fetchall()
```




    [('Seq Scan on fact_orders  (cost=0.00..1.23 rows=1 width=12) (actual time=0.040..0.051 rows=2 loops=1)',),
     ("  Filter: ((amount >= '10000'::numeric) AND ((status_id)::text ~~* 'delivered'::text) AND (order_dttm >= '2022-09-25'::date) AND (start_dttm >= now()))",),
     ('  Rows Removed by Filter: 8',),
     ('Planning Time: 0.312 ms',),
     ('Execution Time: 0.904 ms',)]



###  Создаем индексы для таблицы fact_orders


```python
cur.execute('''
            CREATE INDEX idx_amount_order_dttm
            ON fact_orders(amount, order_dttm);
            ''')
```

### Обновление статистики


```python
cur.execute('''
            analyze fact_orders
            ''')
```

### Смотрим время выполнения запроса с учетом индексов


```python
cur.execute('''
            explain analyze
            select 
            order_id,
            amount
            from fact_orders
            where amount >= 10000
            and status_id ilike ('delivered')
            and order_dttm >= '2022-09-25'
            and start_dttm >= now();
            ''')
cur.fetchall()
```




    [('Seq Scan on fact_orders  (cost=0.00..1.23 rows=1 width=12) (actual time=0.020..0.027 rows=2 loops=1)',),
     ("  Filter: ((amount >= '10000'::numeric) AND ((status_id)::text ~~* 'delivered'::text) AND (order_dttm >= '2022-09-25'::date) AND (start_dttm >= now()))",),
     ('  Rows Removed by Filter: 8',),
     ('Planning Time: 0.142 ms',),
     ('Execution Time: 0.048 ms',)]



### Создаем последовательность для сущности dim_src


```python
cur.execute('''
            CREATE SEQUENCE dim_src_sequence;
            ''')
```

### Создаем таблицу  dim_src - справочник источников
#### src_id - Идентификатор источника, не может быть пустым, первичный ключ, заполняется числовой последовательностью, кардинальность - низкая 
#### src_name - Наименование источника, не может быть пустым, кардинальность - низкая 


```python
cur.execute('''
            DROP TABLE IF EXISTS dim_src 
            ''')
cur.execute('''
            CREATE TABLE dim_src (
            src_id INTEGER NOT NULL DEFAULT nextval('dim_src_sequence'),
            src_name VARCHAR(20),
            PRIMARY KEY(src_id));
            ''')
```

### Наполняем таблицу dim_src данными


```python
cur.execute('''
            insert into dim_src (src_name)
            values
            ('VK'),
            ('INSTAGRAM'),
            ('GOOGLE'),
            ('YANDEX');
            ''')
```

### Select из таблицы dim_src


```python
cur.execute('''
            select* from dim_src;
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['src_id', 'src_name'])
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
      <th>src_id</th>
      <th>src_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>VK</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>INSTAGRAM</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>GOOGLE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>YANDEX</td>
    </tr>
  </tbody>
</table>
</div>



### Создаем таблицу fact_sessions - фактовая сущность сессий пользователей
#### session_id - Идентификатор сессии, не может быть пустым, первичный ключ, кардинальность - высокая
#### src_id -  Идентификатор источника с которого пользователь перешел, не может быть пустым, гарантирует ссылочную целостность, ссылается на справочник dim_src, кардинальность - низкая
#### user_id - Идентификатор пользователя, не может быть пустым, кардинальность - высокая
#### purchase_flg - флаг покупки, не может быть пустым, кардинальность - низкая
#### visit_dttm - Дата посещения сайта, не может быть пустым, кардинальность - высокая


```python
cur.execute('''
            DROP TABLE IF EXISTS fact_sessions 
            ''')
cur.execute('''
            
            CREATE TABLE fact_sessions (
            session_id INTEGER NOT NULL,
            src_id INTEGER NOT NULL REFERENCES dim_src ON DELETE CASCADE,
            user_id INTEGER NOT NULL,
            purchase_flg INTEGER NOT NULL,
            visit_dttm DATE NOT NULL,
            PRIMARY KEY(session_id));
            ''')
```

### Наполняем таблицу fact_sessions данными


```python
cur.execute('''
            insert into fact_sessions 
            (session_id, src_id, user_id, purchase_flg, visit_dttm)
             values
             (12311, 1, 4334, 1, '2022-09-29'),
             (12233, 2, 1114, 0, '2022-09-22'),
             (42331, 1, 4314, 1, '2022-08-29'),
             (42321, 1, 4314, 0, '2022-01-22'),
             (92321, 1, 4314, 1, '2023-01-22'),
             (72331, 3, 1514, 1, '2022-02-21'),
             (92331, 4, 6514, 1, '2022-04-22'),
             (22331, 4, 7514, 1, '2022-04-22'),
             (22321, 4, 7514, 1, '2021-04-22');
            ''')
```

### Select из таблицы fact_sessions


```python
cur.execute('''
            select* from  fact_sessions;
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['session_id', 'src_id', 'user_id', 'purchase_flg', 'visit_dttm'])
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
      <th>src_id</th>
      <th>user_id</th>
      <th>purchase_flg</th>
      <th>visit_dttm</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>12311</td>
      <td>1</td>
      <td>4334</td>
      <td>1</td>
      <td>2022-09-29</td>
    </tr>
    <tr>
      <th>1</th>
      <td>12233</td>
      <td>2</td>
      <td>1114</td>
      <td>0</td>
      <td>2022-09-22</td>
    </tr>
    <tr>
      <th>2</th>
      <td>42331</td>
      <td>1</td>
      <td>4314</td>
      <td>1</td>
      <td>2022-08-29</td>
    </tr>
    <tr>
      <th>3</th>
      <td>42321</td>
      <td>1</td>
      <td>4314</td>
      <td>0</td>
      <td>2022-01-22</td>
    </tr>
    <tr>
      <th>4</th>
      <td>92321</td>
      <td>1</td>
      <td>4314</td>
      <td>1</td>
      <td>2023-01-22</td>
    </tr>
    <tr>
      <th>5</th>
      <td>72331</td>
      <td>3</td>
      <td>1514</td>
      <td>1</td>
      <td>2022-02-21</td>
    </tr>
    <tr>
      <th>6</th>
      <td>92331</td>
      <td>4</td>
      <td>6514</td>
      <td>1</td>
      <td>2022-04-22</td>
    </tr>
    <tr>
      <th>7</th>
      <td>22331</td>
      <td>4</td>
      <td>7514</td>
      <td>1</td>
      <td>2022-04-22</td>
    </tr>
    <tr>
      <th>8</th>
      <td>22321</td>
      <td>4</td>
      <td>7514</td>
      <td>1</td>
      <td>2021-04-22</td>
    </tr>
  </tbody>
</table>
</div>



### Аналитический запрос:
#### Для каждого источника посчитать количество уникальных пользователей и количество сессий, в которых была совершена покупка начиная с 2022-01-01


```python
cur.execute('''
            select 
            d.src_name,
            count(distinct f.user_id) as count_users,
            count(f.session_id) as count_sessions
            from fact_sessions f
            left join dim_src d
            on d.src_id = f.src_id
            where purchase_flg = 1
            and visit_dttm >= '2022-01-01' 
            group by src_name
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['src_name', 'count_users', 'count_sessions'])
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
      <th>src_name</th>
      <th>count_users</th>
      <th>count_sessions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>GOOGLE</td>
      <td>1</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>VK</td>
      <td>2</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>YANDEX</td>
      <td>2</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



### Обновление статистики


```python
cur.execute('''
            analyze fact_sessions
            ''')
cur.execute('''
            analyze dim_src
            ''')
```

### Смотрим время выполнения запроса 


```python
cur.execute('''
            explain analyze
            select 
            d.src_name,
            count(distinct f.user_id) as count_users,
            count(f.session_id) as count_sessions
            from fact_sessions f
            left join dim_src d
            on d.src_id = f.src_id
            where purchase_flg = 1
            and visit_dttm >= '2022-01-01' 
            group by src_name
            ''')
cur.fetchall()
```




    [('GroupAggregate  (cost=2.36..2.47 rows=4 width=22) (actual time=0.229..0.245 rows=3 loops=1)',),
     ('  Group Key: d.src_name',),
     ('  ->  Sort  (cost=2.36..2.38 rows=7 width=14) (actual time=0.153..0.164 rows=6 loops=1)',),
     ('        Sort Key: d.src_name',),
     ('        Sort Method: quicksort  Memory: 25kB',),
     ('        ->  Hash Left Join  (cost=1.09..2.27 rows=7 width=14) (actual time=0.109..0.123 rows=6 loops=1)',),
     ('              Hash Cond: (f.src_id = d.src_id)',),
     ('              ->  Seq Scan on fact_sessions f  (cost=0.00..1.14 rows=7 width=12) (actual time=0.011..0.013 rows=6 loops=1)',),
     ("                    Filter: ((visit_dttm >= '2022-01-01'::date) AND (purchase_flg = 1))",),
     ('                    Rows Removed by Filter: 3',),
     ('              ->  Hash  (cost=1.04..1.04 rows=4 width=10) (actual time=0.013..0.014 rows=4 loops=1)',),
     ('                    Buckets: 1024  Batches: 1  Memory Usage: 9kB',),
     ('                    ->  Seq Scan on dim_src d  (cost=0.00..1.04 rows=4 width=10) (actual time=0.004..0.005 rows=4 loops=1)',),
     ('Planning Time: 0.741 ms',),
     ('Execution Time: 0.427 ms',)]



### Создаем индексы для таблицы fact_sessions


```python
cur.execute('''
            CREATE INDEX idx_src_id_visit_dttm
            ON fact_sessions(src_id, visit_dttm);
            ''')
```

### Обновление статистики


```python
cur.execute('''
            analyze fact_sessions
            ''')
cur.execute('''
            analyze dim_src
            ''')
```

### Смотрим время выполнения запроса с учетом индексов


```python
cur.execute('''
            explain analyze
            select 
            d.src_name,
            count(distinct f.user_id) as count_users,
            count(f.session_id) as count_sessions
            from fact_sessions f
            left join dim_src d
            on d.src_id = f.src_id
            where purchase_flg = 1
            and visit_dttm >= '2022-01-01' 
            group by src_name
            ''')
cur.fetchall()
```




    [('GroupAggregate  (cost=2.36..2.47 rows=4 width=22) (actual time=0.061..0.071 rows=3 loops=1)',),
     ('  Group Key: d.src_name',),
     ('  ->  Sort  (cost=2.36..2.38 rows=7 width=14) (actual time=0.048..0.051 rows=6 loops=1)',),
     ('        Sort Key: d.src_name',),
     ('        Sort Method: quicksort  Memory: 25kB',),
     ('        ->  Hash Left Join  (cost=1.09..2.27 rows=7 width=14) (actual time=0.029..0.035 rows=6 loops=1)',),
     ('              Hash Cond: (f.src_id = d.src_id)',),
     ('              ->  Seq Scan on fact_sessions f  (cost=0.00..1.14 rows=7 width=12) (actual time=0.011..0.014 rows=6 loops=1)',),
     ("                    Filter: ((visit_dttm >= '2022-01-01'::date) AND (purchase_flg = 1))",),
     ('                    Rows Removed by Filter: 3',),
     ('              ->  Hash  (cost=1.04..1.04 rows=4 width=10) (actual time=0.010..0.011 rows=4 loops=1)',),
     ('                    Buckets: 1024  Batches: 1  Memory Usage: 9kB',),
     ('                    ->  Seq Scan on dim_src d  (cost=0.00..1.04 rows=4 width=10) (actual time=0.003..0.004 rows=4 loops=1)',),
     ('Planning Time: 0.300 ms',),
     ('Execution Time: 0.118 ms',)]




```python
cur.close()
conn.close()
```
