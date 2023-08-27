```python
import pandas as pd
import mysql.connector
import sqlalchemy
```


```python
 cnx = mysql.connector.connect(user='root', password='password', host='127.0.0.1', database='online_store', port='3306')
```


```python
cur = cnx.cursor()
```

#### session_id - Идентификатор сессии
#### client_id - Идентификатор клиента
#### src_id - Идентификатор источника с которого пользователь перешел
#### Purchase_flg - Совершалась ли покупка
#### visit_dttm - Дата сессии
#### duration - Время сессии
#### page_count - Кол-во просмотренных страниц
#### last_page_id - Идентификатор последней страницы сессии


```python
cur.execute('''
            CREATE TABLE 
            sessions_info (
                session_id Binary(16) NOT null PRIMARY KEY,
                client_id Binary(16) NOT null,
                src_id Enum('INST', 'VK', 'WEB', 'TG') NOT null,
                Purchase_flg TINYINT(1) NOT null,
                visit_dttm DATETIME NOT null,
                duration TIME NOT null,
                page_count TINYINT UNSIGNED NOT null,	
                last_page_id SMALLINT UNSIGNED NOT null);
            ''')
```

#### uuid_to_bin(UUID()) - генерирует случайный уникальный UUID и преобразует в BINARY для оптимизации хранения 
#### now() - генерирует текущую дату и время 
#### SEC_TO_TIME(FLOOR(RAND() * 3600)) - генерирует случайное время в формате часы:минуты:секунды 
#### FLOOR(RAND() * 1000) - генерирует случайное целое число от 0 до 999


```python
cur.execute('''
            INSERT INTO sessions_info
            (session_id, client_id, src_id, Purchase_flg, 
            visit_dttm, duration, page_count, last_page_id)
            values 
            (uuid_to_bin(UUID()), uuid_to_bin(UUID()), 'INST', 1, now(),
            SEC_TO_TIME(FLOOR(RAND() * 3600)), FLOOR(RAND() * 255), FLOOR(RAND() * 1000));
            ''')
```


```python
cur.execute('''
            select * from sessions_info;
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['session_id', 'client_id', 'src_id', 'Purchase_flg',
                              'visit_dttm', 'duration', 'page_count', 'last_page_id'])
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
      <td>[54, 25, 211, 38, 69, 33, 17, 238, 151, 112, 2...</td>
      <td>[54, 25, 224, 170, 69, 33, 17, 238, 151, 112, ...</td>
      <td>INST</td>
      <td>1</td>
      <td>2023-08-27 21:32:23</td>
      <td>0 days 00:41:54</td>
      <td>7</td>
      <td>58</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            select bin_to_uuid(session_id) from sessions_info;
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['session_id'])
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
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3619d326-4521-11ee-9770-0242ac110002</td>
    </tr>
  </tbody>
</table>
</div>



#### JSON


```python
cur.execute('''
            CREATE TABLE 
            order_items (
                order_items_info json);
            ''')
```

#### order_id - Идентификатор заказа
#### item_id - Идентификатор товара
#### item_count - Кол-во товара одной позиции
#### price - Цена заказа
#### discount - Размер скидки


```python
cur.execute('''
            INSERT INTO
            order_items
            (order_items_info)
            values
            ('{"order_id": "3619d326-4521-11ee-9770-0242ac110002", "item_id": 4, 
            "item_count": 1, "price": 3000, "discount": 300}');
            ''')
```


```python
cur.execute('''
            select * from order_items;
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['session_id'])
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
      <th>order_items_info</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>{"price": 3000, "item_id": 4, "discount": 300,...</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            select 
            JSON_VALUE(order_items_info , '$.order_id') as order_id,
            JSON_VALUE(order_items_info , '$.item_id') as item_id,
            JSON_VALUE(order_items_info , '$.item_count') as item_count,
            JSON_VALUE(order_items_info , '$.price') as price,
            JSON_VALUE(order_items_info , '$.discount') as discount
            from order_items;
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['order_id', 'item_id', 'item_count', 'price', 'discount'])
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
      <th>item_id</th>
      <th>item_count</th>
      <th>price</th>
      <th>discount</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3619d326-4521-11ee-9770-0242ac110002</td>
      <td>4</td>
      <td>1</td>
      <td>3000</td>
      <td>300</td>
    </tr>
  </tbody>
</table>
</div>


