```python
import pandas as pd
import mysql.connector
import sqlalchemy
```


```python
cnx = mysql.connector.connect(user='root', password='password', 
                              host='127.0.0.1', database='otus_db', port='3306')
```


```python
cur = cnx.cursor()
```

### В этом запросе мы выбираем имена, фамилии, email 10 клиентов,подсчитываем общее количество посещений и покупок для каждого клиента, сумму потраченных ими денег. Запрос также включает подзапрос, который выбирает клиентов имеющих больше 2 посещений.


```python
query = f'''
SELECT
  ci.first_name,
  ci.last_name,
  ci.email,
  COUNT(DISTINCT wv.id) AS total_visits,
  COUNT(DISTINCT p.id) AS total_purchases,
  SUM(p.price) AS total_spent
FROM
  contact_info AS ci
  LEFT JOIN website_visits AS wv ON ci.id = wv.client_id
  LEFT JOIN purchases AS p ON ci.id = p.client_id
WHERE
  ci.id IN (
      SELECT client_id as cnt FROM website_visits group by client_id having count(*) > 2
    ) 
GROUP BY
  ci.id
  having total_spent > 0
ORDER BY
  total_spent DESC
  limit 10;

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
      <th>first_name</th>
      <th>last_name</th>
      <th>email</th>
      <th>total_visits</th>
      <th>total_purchases</th>
      <th>total_spent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Иван</td>
      <td>Сидоров</td>
      <td>user350@example.com</td>
      <td>11</td>
      <td>5</td>
      <td>44680.02</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Алексей</td>
      <td>Сидоров</td>
      <td>user269@example.com</td>
      <td>15</td>
      <td>4</td>
      <td>41746.80</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Алексей</td>
      <td>Петров</td>
      <td>user35@example.com</td>
      <td>10</td>
      <td>6</td>
      <td>40880.60</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Алексей</td>
      <td>Петров</td>
      <td>user230@example.com</td>
      <td>7</td>
      <td>7</td>
      <td>36403.22</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Иван</td>
      <td>Петров</td>
      <td>user883@example.com</td>
      <td>10</td>
      <td>6</td>
      <td>35700.90</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Иван</td>
      <td>Петров</td>
      <td>user906@example.com</td>
      <td>10</td>
      <td>7</td>
      <td>35154.40</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Иван</td>
      <td>Сидоров</td>
      <td>user673@example.com</td>
      <td>11</td>
      <td>4</td>
      <td>32263.33</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Алексей</td>
      <td>Петров</td>
      <td>user839@example.com</td>
      <td>10</td>
      <td>8</td>
      <td>32035.00</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Алексей</td>
      <td>Сидоров</td>
      <td>user87@example.com</td>
      <td>7</td>
      <td>6</td>
      <td>29563.17</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Алексей</td>
      <td>Сидоров</td>
      <td>user13@example.com</td>
      <td>9</td>
      <td>5</td>
      <td>29231.55</td>
    </tr>
  </tbody>
</table>
</div>



### Команда EXPLAIN

#### Выражение EXPLAIN предоставляет информацию о том, как MySQL выполняет запрос. Оно работает с выражениями SELECT, UPDATE, INSERT, DELETE и REPLACE.


```python
query = f'''
EXPLAIN
SELECT
  ci.first_name,
  ci.last_name,
  ci.email,
  COUNT(DISTINCT wv.id) AS total_visits,
  COUNT(DISTINCT p.id) AS total_purchases,
  SUM(p.price) AS total_spent
FROM
  contact_info AS ci
  LEFT JOIN website_visits AS wv ON ci.id = wv.client_id
  LEFT JOIN purchases AS p ON ci.id = p.client_id
WHERE
  ci.id IN (
      SELECT client_id as cnt FROM website_visits group by client_id having count(*) > 2
    ) 
GROUP BY
  ci.id
  having total_spent > 0
ORDER BY
  total_spent DESC
  limit 10;

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
      <th>id</th>
      <th>select_type</th>
      <th>table</th>
      <th>partitions</th>
      <th>type</th>
      <th>possible_keys</th>
      <th>key</th>
      <th>key_len</th>
      <th>ref</th>
      <th>rows</th>
      <th>filtered</th>
      <th>Extra</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>PRIMARY</td>
      <td>ci</td>
      <td>None</td>
      <td>index</td>
      <td>PRIMARY</td>
      <td>PRIMARY</td>
      <td>4</td>
      <td>None</td>
      <td>3000</td>
      <td>100.0</td>
      <td>Using where; Using temporary; Using filesort</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>PRIMARY</td>
      <td>wv</td>
      <td>None</td>
      <td>ref</td>
      <td>client_id</td>
      <td>client_id</td>
      <td>5</td>
      <td>otus_db.ci.id</td>
      <td>1</td>
      <td>100.0</td>
      <td>Using index</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>PRIMARY</td>
      <td>p</td>
      <td>None</td>
      <td>ref</td>
      <td>client_id</td>
      <td>client_id</td>
      <td>5</td>
      <td>otus_db.ci.id</td>
      <td>1</td>
      <td>100.0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>SUBQUERY</td>
      <td>website_visits</td>
      <td>None</td>
      <td>index</td>
      <td>client_id</td>
      <td>client_id</td>
      <td>5</td>
      <td>None</td>
      <td>5000</td>
      <td>100.0</td>
      <td>Using index</td>
    </tr>
  </tbody>
</table>
</div>



### SHOW WARNINGS
#### содержит специальные маркеры, которые не являются допустимым SQL -выражением.


```python
query = f'''
SHOW WARNINGS;
        '''
df = pd.read_sql_query(query, cnx)
for i in df['Message']:
    print(i)
```

    /* select#1 */ select `otus_db`.`ci`.`first_name` AS `first_name`,`otus_db`.`ci`.`last_name` AS `last_name`,`otus_db`.`ci`.`email` AS `email`,count(distinct `otus_db`.`wv`.`id`) AS `total_visits`,count(distinct `otus_db`.`p`.`id`) AS `total_purchases`,sum(`otus_db`.`p`.`price`) AS `total_spent` from `otus_db`.`contact_info` `ci` left join `otus_db`.`website_visits` `wv` on((`otus_db`.`wv`.`client_id` = `otus_db`.`ci`.`id`)) left join `otus_db`.`purchases` `p` on((`otus_db`.`p`.`client_id` = `otus_db`.`ci`.`id`)) where <in_optimizer>(`otus_db`.`ci`.`id`,`otus_db`.`ci`.`id` in ( <materialize> (/* select#2 */ select `otus_db`.`website_visits`.`client_id` AS `cnt` from `otus_db`.`website_visits` group by `otus_db`.`website_visits`.`client_id` having (count(0) > 2) ), <primary_index_lookup>(`otus_db`.`ci`.`id` in <temporary table> on <auto_distinct_key> where ((`otus_db`.`ci`.`id` = `<materialized_subquery>`.`cnt`))))) group by `otus_db`.`ci`.`id` having (`total_spent` > 0) order by `total_spent` desc limit 10


    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(


### EXPLAIN FORMAT = TREE 
#### позволяют читать план выполнения и информацию о стоимости запроса без использования SHOW WARNINGS.


```python
query = f'''
EXPLAIN FORMAT = TREE 
SELECT
  ci.first_name,
  ci.last_name,
  ci.email,
  COUNT(DISTINCT wv.id) AS total_visits,
  COUNT(DISTINCT p.id) AS total_purchases,
  SUM(p.price) AS total_spent
FROM
  contact_info AS ci
  LEFT JOIN website_visits AS wv ON ci.id = wv.client_id
  LEFT JOIN purchases AS p ON ci.id = p.client_id
WHERE
  ci.id IN (
      SELECT client_id as cnt FROM website_visits group by client_id having count(*) > 2
    ) 
GROUP BY
  ci.id
  having total_spent > 0
ORDER BY
  total_spent DESC
  limit 10;

        '''
df = pd.read_sql_query(query, cnx)
for i in df['EXPLAIN']:
    print(i)
```

    -> Limit: 10 row(s)
        -> Sort: total_spent DESC
            -> Filter: (total_spent > 0)
                -> Stream results  (cost=3637.12 rows=3000)
                    -> Group aggregate: count(distinct wv.id), count(distinct p.id), sum(p.price)  (cost=3637.12 rows=3000)
                        -> Nested loop left join  (cost=3167.64 rows=4695)
                            -> Nested loop left join  (cost=1524.45 rows=4695)
                                -> Filter: <in_optimizer>(ci.id,ci.id in (select #2))  (cost=304.50 rows=3000)
                                    -> Index scan on ci using PRIMARY  (cost=304.50 rows=3000)
                                    -> Select #2 (subquery in condition; run only once)
                                        -> Filter: ((ci.id = `<materialized_subquery>`.cnt))  (cost=1322.85..1322.85 rows=1)
                                            -> Limit: 1 row(s)  (cost=1322.75..1322.75 rows=1)
                                                -> Index lookup on <materialized_subquery> using <auto_distinct_key> (cnt=ci.id)
                                                    -> Materialize with deduplication  (cost=1322.75..1322.75 rows=3195)
                                                        -> Filter: (count(0) > 2)  (cost=1003.25 rows=3195)
                                                            -> Group aggregate: count(0)  (cost=1003.25 rows=3195)
                                                                -> Index scan on website_visits using client_id  (cost=503.25 rows=5000)
                                -> Covering index lookup on wv using client_id (client_id=ci.id)  (cost=0.25 rows=2)
                            -> Index lookup on p using client_id (client_id=ci.id)  (cost=0.25 rows=1)
    


    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(


#### Еще более подробную информацию можно получить, заменив FORMAT = TREE на выражение ANALYZE, которое предоставляет MySQL с версии 8.0.18.


```python
query = f'''
EXPLAIN ANALYZE 
SELECT
  ci.first_name,
  ci.last_name,
  ci.email,
  COUNT(DISTINCT wv.id) AS total_visits,
  COUNT(DISTINCT p.id) AS total_purchases,
  SUM(p.price) AS total_spent
FROM
  contact_info AS ci
  LEFT JOIN website_visits AS wv ON ci.id = wv.client_id
  LEFT JOIN purchases AS p ON ci.id = p.client_id
WHERE
  ci.id IN (
      SELECT client_id as cnt FROM website_visits group by client_id having count(*) > 2
    ) 
GROUP BY
  ci.id
  having total_spent > 0
ORDER BY
  total_spent DESC
  limit 10;

        '''
df = pd.read_sql_query(query, cnx)
for i in df['EXPLAIN']:
    print(i)
```

    -> Limit: 10 row(s)  (actual time=42.513..42.514 rows=10 loops=1)
        -> Sort: total_spent DESC  (actual time=42.512..42.513 rows=10 loops=1)
            -> Filter: (total_spent > 0)  (actual time=5.161..41.919 rows=831 loops=1)
                -> Stream results  (cost=25842.42 rows=3000) (actual time=5.159..41.774 rows=869 loops=1)
                    -> Group aggregate: count(distinct wv.id), count(distinct p.id), sum(p.price)  (cost=25842.42 rows=3000) (actual time=5.153..41.224 rows=869 loops=1)
                        -> Nested loop left join  (cost=21072.19 rows=47702) (actual time=5.070..37.236 rows=14821 loops=1)
                            -> Nested loop left join  (cost=4376.37 rows=15106) (actual time=4.963..15.678 rows=4790 loops=1)
                                -> Filter: <in_optimizer>(ci.id,ci.id in (select #2))  (cost=304.50 rows=3000) (actual time=4.942..8.970 rows=869 loops=1)
                                    -> Index scan on ci using PRIMARY  (cost=304.50 rows=3000) (actual time=0.318..1.668 rows=3000 loops=1)
                                    -> Select #2 (subquery in condition; run only once)
                                        -> Filter: ((ci.id = `<materialized_subquery>`.cnt))  (cost=1102.65..1102.65 rows=1) (actual time=0.002..0.002 rows=0 loops=3001)
                                            -> Limit: 1 row(s)  (cost=1102.55..1102.55 rows=1) (actual time=0.002..0.002 rows=0 loops=3001)
                                                -> Index lookup on <materialized_subquery> using <auto_distinct_key> (cnt=ci.id)  (actual time=0.002..0.002 rows=0 loops=3001)
                                                    -> Materialize with deduplication  (cost=1102.55..1102.55 rows=993) (actual time=4.593..4.593 rows=869 loops=1)
                                                        -> Filter: (count(0) > 2)  (cost=1003.25 rows=993) (actual time=0.613..4.366 rows=869 loops=1)
                                                            -> Group aggregate: count(0)  (cost=1003.25 rows=993) (actual time=0.611..4.290 rows=993 loops=1)
                                                                -> Covering index scan on website_visits using idx_cl_id_w  (cost=503.25 rows=5000) (actual time=0.608..3.942 rows=5000 loops=1)
                                -> Covering index lookup on wv using idx_cl_id_w (client_id=ci.id)  (cost=0.85 rows=5) (actual time=0.006..0.007 rows=6 loops=869)
                            -> Index lookup on p using idx_cl_id_p (client_id=ci.id)  (cost=0.79 rows=3) (actual time=0.003..0.004 rows=3 loops=4790)
    


    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(


#### Оптимизация

#### Для таблиц contact_info, website_visits и purchases должны быть индексы на столбцы, используемые для соединения таблиц (ci.id, wv.client_id и p.client_id соответственно). Индексы помогают ускорить выполнение запроса, особенно при больших объемах данных.

#### Чем меньше значение rows  и чем больше значение filtered,- тем лучше. Однако, если значение rows слишком велико и filtered стремится к 100 %  - это очень плохо.


```python
cur.execute('''
CREATE INDEX idx_cl_id_w
    ON website_visits (client_id);
            ''')

cur.execute('''
CREATE INDEX idx_cl_id_p
    ON purchases (client_id);
            ''')
```


```python
query = f'''
EXPLAIN
SELECT ci.first_name,
       ci.last_name,
       ci.email,
       COUNT(DISTINCT wv.id) AS total_visits,
       COUNT(DISTINCT p.id) AS total_purchases,
       SUM(p.price) AS total_spent
FROM contact_info AS ci
LEFT JOIN website_visits AS wv ON ci.id = wv.client_id
LEFT JOIN purchases AS p ON ci.id = p.client_id
INNER JOIN (
  SELECT client_id
  FROM website_visits
  GROUP BY client_id
  HAVING COUNT(*) > 2
) AS wv2 ON ci.id = wv2.client_id
WHERE p.price > 0
GROUP BY ci.id
ORDER BY total_spent DESC
LIMIT 10;

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
      <th>id</th>
      <th>select_type</th>
      <th>table</th>
      <th>partitions</th>
      <th>type</th>
      <th>possible_keys</th>
      <th>key</th>
      <th>key_len</th>
      <th>ref</th>
      <th>rows</th>
      <th>filtered</th>
      <th>Extra</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>PRIMARY</td>
      <td>&lt;derived2&gt;</td>
      <td>None</td>
      <td>ALL</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>5000</td>
      <td>100.00</td>
      <td>Using where; Using temporary; Using filesort</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>PRIMARY</td>
      <td>ci</td>
      <td>None</td>
      <td>eq_ref</td>
      <td>PRIMARY</td>
      <td>PRIMARY</td>
      <td>4</td>
      <td>wv2.client_id</td>
      <td>1</td>
      <td>100.00</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>PRIMARY</td>
      <td>p</td>
      <td>None</td>
      <td>ref</td>
      <td>idx_cl_id_p</td>
      <td>idx_cl_id_p</td>
      <td>5</td>
      <td>wv2.client_id</td>
      <td>3</td>
      <td>33.33</td>
      <td>Using where</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>PRIMARY</td>
      <td>wv</td>
      <td>None</td>
      <td>ref</td>
      <td>idx_cl_id_w</td>
      <td>idx_cl_id_w</td>
      <td>5</td>
      <td>wv2.client_id</td>
      <td>5</td>
      <td>100.00</td>
      <td>Using index</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2</td>
      <td>DERIVED</td>
      <td>website_visits</td>
      <td>None</td>
      <td>index</td>
      <td>idx_cl_id_w</td>
      <td>idx_cl_id_w</td>
      <td>5</td>
      <td>None</td>
      <td>5000</td>
      <td>100.00</td>
      <td>Using index</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''
EXPLAIN ANALYZE 
SELECT
  ci.first_name,
  ci.last_name,
  ci.email,
  COUNT(DISTINCT wv.id) AS total_visits,
  COUNT(DISTINCT p.id) AS total_purchases,
  SUM(p.price) AS total_spent
FROM
  contact_info AS ci
  LEFT JOIN website_visits AS wv ON ci.id = wv.client_id
  LEFT JOIN purchases AS p ON ci.id = p.client_id
WHERE
  ci.id IN (
      SELECT client_id as cnt FROM website_visits group by client_id having count(*) > 2
    ) 
GROUP BY
  ci.id
  having total_spent > 0
ORDER BY
  total_spent DESC
  limit 10;

        '''
df = pd.read_sql_query(query, cnx)
for i in df['EXPLAIN']:
    print(i)
```

    -> Limit: 10 row(s)  (actual time=39.276..39.278 rows=10 loops=1)
        -> Sort: total_spent DESC  (actual time=39.275..39.277 rows=10 loops=1)
            -> Filter: (total_spent > 0)  (actual time=8.708..38.903 rows=831 loops=1)
                -> Stream results  (cost=25842.42 rows=3000) (actual time=8.704..38.788 rows=869 loops=1)
                    -> Group aggregate: count(distinct wv.id), count(distinct p.id), sum(p.price)  (cost=25842.42 rows=3000) (actual time=8.692..38.255 rows=869 loops=1)
                        -> Nested loop left join  (cost=21072.19 rows=47702) (actual time=8.538..34.963 rows=14821 loops=1)
                            -> Nested loop left join  (cost=4376.37 rows=15106) (actual time=8.511..17.851 rows=4790 loops=1)
                                -> Filter: <in_optimizer>(ci.id,ci.id in (select #2))  (cost=304.50 rows=3000) (actual time=8.481..12.508 rows=869 loops=1)
                                    -> Index scan on ci using PRIMARY  (cost=304.50 rows=3000) (actual time=0.104..1.191 rows=3000 loops=1)
                                    -> Select #2 (subquery in condition; run only once)
                                        -> Filter: ((ci.id = `<materialized_subquery>`.cnt))  (cost=1102.65..1102.65 rows=1) (actual time=0.004..0.004 rows=0 loops=3001)
                                            -> Limit: 1 row(s)  (cost=1102.55..1102.55 rows=1) (actual time=0.003..0.003 rows=0 loops=3001)
                                                -> Index lookup on <materialized_subquery> using <auto_distinct_key> (cnt=ci.id)  (actual time=0.003..0.003 rows=0 loops=3001)
                                                    -> Materialize with deduplication  (cost=1102.55..1102.55 rows=993) (actual time=8.351..8.351 rows=869 loops=1)
                                                        -> Filter: (count(0) > 2)  (cost=1003.25 rows=993) (actual time=0.187..7.989 rows=869 loops=1)
                                                            -> Group aggregate: count(0)  (cost=1003.25 rows=993) (actual time=0.186..7.878 rows=993 loops=1)
                                                                -> Covering index scan on website_visits using idx_cl_id_w  (cost=503.25 rows=5000) (actual time=0.181..7.282 rows=5000 loops=1)
                                -> Covering index lookup on wv using idx_cl_id_w (client_id=ci.id)  (cost=0.85 rows=5) (actual time=0.005..0.006 rows=6 loops=869)
                            -> Index lookup on p using idx_cl_id_p (client_id=ci.id)  (cost=0.79 rows=3) (actual time=0.003..0.003 rows=3 loops=4790)
    


    /Users/denis/opt/anaconda3/lib/python3.9/site-packages/pandas/io/sql.py:761: UserWarning: pandas only support SQLAlchemy connectable(engine/connection) ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy
      warnings.warn(

