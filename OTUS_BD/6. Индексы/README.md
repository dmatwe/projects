```python
import psycopg2
import pandas as pd
import sqlalchemy
```


```python
conn = psycopg2.connect(host="127.0.0.1", port="5433", database="online_store", 
                            user="admin", password="root")
```


```python
cur = conn.cursor()

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



## Обновление статистики


```python
cur.execute('''
            analyze clients.sessions_info 
            ''')
```

### Смотрим время выполнения запроса без индексов
#### Посчитать кол-во сессий  источника 3


```python
cur.execute('''
            explain analyze
            select 
            count(*) as sessions_count, 
            src_id
            from clients.sessions_info 
            where src_id = 3
            group by src_id
            ''')
cur.fetchall()
```




    [('GroupAggregate  (cost=0.00..1.10 rows=2 width=12) (actual time=0.301..0.301 rows=1 loops=1)',),
     ('  Group Key: src_id',),
     ('  ->  Seq Scan on sessions_info  (cost=0.00..1.06 rows=3 width=4) (actual time=0.238..0.239 rows=3 loops=1)',),
     ('        Filter: (src_id = 3)',),
     ('        Rows Removed by Filter: 2',),
     ('Planning Time: 1.666 ms',),
     ('Execution Time: 0.760 ms',)]



### Посмотреть стоимость последовательного сканирования 


```python
cur.execute('''
            show seq_page_cost;
            ''')
cur.fetchall()
```




    [('1',)]



### Посмотреть стоимость произвольного чтения 


```python
cur.execute('''
            show random_page_cost;
            ''')
cur.fetchall()
```




    [('4',)]



### Изменить стоимость произвольного чтения 


```python
cur.execute('''
            set random_page_cost = 0.1;
            ''')
cur.execute('''
            show random_page_cost;
            ''')
cur.fetchall()
```




    [('0.1',)]



### Создаем индексы для таблицы sessions_info на поле src_id и смотрим время выполнения запроса


```python
cur.execute('''
            CREATE INDEX idx_src_id
            ON clients.sessions_info (src_id);
            ''')
cur.execute('''
            explain analyze
            select 
            count(*) as sessions_count, 
            src_id
            from clients.sessions_info 
            where src_id = 3
            group by src_id
            ''')
cur.fetchall()
```




    [('GroupAggregate  (cost=0.13..0.52 rows=2 width=12) (actual time=0.170..0.171 rows=1 loops=1)',),
     ('  Group Key: src_id',),
     ('  ->  Index Only Scan using idx_src_id on sessions_info  (cost=0.13..0.49 rows=3 width=4) (actual time=0.164..0.165 rows=3 loops=1)',),
     ('        Index Cond: (src_id = 3)',),
     ('        Heap Fetches: 3',),
     ('Planning Time: 0.875 ms',),
     ('Execution Time: 0.233 ms',)]



####  После добавления индекса скорость значительно увеличилась 


```python
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



### Смотрим время выполнения запроса без индексов
#### Посмотреть клиентов, проживающих на улице Дорожной


```python
cur.execute('''
            explain analyze
            select 
            client_name
            from clients.clients_info 
            where address ilike ('%Dorojnaia%')
            and city = 'Moscow'
            ''')
cur.fetchall()
```




    [('Seq Scan on clients_info  (cost=0.00..10.90 rows=1 width=118) (actual time=0.618..0.618 rows=0 loops=1)',),
     ("  Filter: (((address)::text ~~* '%Dorojnaia%'::text) AND ((city)::text = 'Moscow'::text))",),
     ('  Rows Removed by Filter: 4',),
     ('Planning Time: 4.649 ms',),
     ('Execution Time: 2.266 ms',)]



### Создаем индекс для полнотекстового поиска


```python
cur.execute('''
            alter table clients.clients_info
            add column tsvector_address tsvector;
            ''')

cur.execute('''
            update clients.clients_info
            set tsvector_address = to_tsvector(address)
            ''')


cur.execute('''
            CREATE INDEX idx_gin_document 
            ON clients.clients_info
            USING gin ("tsvector_address")
            ''')

cur.execute('''
            analyze clients.clients_info 
            ''')

cur.execute('''
            explain analyze
            select 
            client_name
            from clients.clients_info 
            where address ilike ('%Dorojnaia%')
            and city = 'Moscow'
            ''')
cur.fetchall()
```




    [('Seq Scan on clients_info  (cost=0.00..1.06 rows=1 width=12) (actual time=0.011..0.012 rows=0 loops=1)',),
     ("  Filter: (((address)::text ~~* '%Dorojnaia%'::text) AND ((city)::text = 'Moscow'::text))",),
     ('  Rows Removed by Filter: 4',),
     ('Planning Time: 0.354 ms',),
     ('Execution Time: 0.048 ms',)]



####  После добавления индекса скорость значительно увеличилась 


```python
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



### Смотрим время выполнения запроса без индексов
#### Посмотреть клиентов, c суммой заказов > 3000


```python
cur.execute('''
            explain analyze
            select 
            client_id,
            amount
            from orders.orders_info 
            where amount > 3000
            ''')
cur.fetchall()
```




    [('Seq Scan on orders_info  (cost=0.00..1.04 rows=1 width=50) (actual time=0.157..0.159 rows=2 loops=1)',),
     ("  Filter: (amount > '3000'::numeric)",),
     ('  Rows Removed by Filter: 1',),
     ('Planning Time: 0.621 ms',),
     ('Execution Time: 0.250 ms',)]



### Создаем индекс на часть таблицы 


```python
cur.execute('''
            CREATE INDEX idx_amount_3000
            ON orders.orders_info(amount) 
            where amount > 3000
            ''')

cur.execute('''
            analyze orders.orders_info 
            ''')

cur.execute('''
            explain analyze
            select 
            client_id,
            amount
            from orders.orders_info 
            where amount > 3000
            ''')
cur.fetchall()
```




    [('Index Scan using idx_amount_3000 on orders_info  (cost=0.13..0.46 rows=2 width=9) (actual time=0.123..0.125 rows=2 loops=1)',),
     ('Planning Time: 0.330 ms',),
     ('Execution Time: 0.162 ms',)]



### Смотрим время выполнения запроса без индексов
#### Посчитать кол-во сессий клиентов из источника 3 с флагом покупки


```python
cur.execute('''
            explain analyze
            select 
            count(client_id) 
            from clients.sessions_info
            where src_id = 3
            and Purchase_flg = 1
            ''')
cur.fetchall()
```




    [('Aggregate  (cost=1.08..1.09 rows=1 width=8) (actual time=0.373..0.373 rows=1 loops=1)',),
     ('  ->  Seq Scan on sessions_info  (cost=0.00..1.07 rows=1 width=4) (actual time=0.191..0.193 rows=2 loops=1)',),
     ('        Filter: ((src_id = 3) AND (purchase_flg = 1))',),
     ('        Rows Removed by Filter: 3',),
     ('Planning Time: 7.288 ms',),
     ('Execution Time: 1.661 ms',)]



### Создать индекс на несколько полей



```python
cur.execute('''
            CREATE INDEX idx_amount_order_dttm
            ON clients.sessions_info(src_id, Purchase_flg);
            ''')

cur.execute('''
            analyze clients.sessions_info
            ''')

cur.execute('''
            explain analyze
            select 
            count(client_id) 
            from clients.sessions_info
            where src_id = 3
            and Purchase_flg = 1
            ''')
cur.fetchall()
```




    [('Aggregate  (cost=0.38..0.39 rows=1 width=8) (actual time=0.195..0.196 rows=1 loops=1)',),
     ('  ->  Index Scan using idx_amount_order_dttm on sessions_info  (cost=0.13..0.37 rows=2 width=4) (actual time=0.181..0.182 rows=2 loops=1)',),
     ('        Index Cond: ((src_id = 3) AND (purchase_flg = 1))',),
     ('Planning Time: 1.072 ms',),
     ('Execution Time: 0.718 ms',)]



####  После добавления индекса скорость значительно увеличилась 
