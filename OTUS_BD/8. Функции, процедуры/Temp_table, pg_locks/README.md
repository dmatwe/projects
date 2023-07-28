```python
import psycopg2
import pandas as pd
import sqlalchemy
```


```python
conn = psycopg2.connect(host="127.0.0.1", port="5432", database="Adventureworks", 
                            user="postgres", password="postgres")
cur = conn.cursor()
```


```python


cur.execute('''
           create table if not exists  customer 
            (customer_id int,
             store_id int,
             first_name varchar,
             last_name varchar)
            ''')


cur.execute('''
           insert into   customer
            values(1,1,'Mike','Mike');
            ''')


cur.execute('''
           select* from customer
        
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['customer_id', 'store_id', 'first_name', 'last_name'])
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
      <th>store_id</th>
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Mike</td>
      <td>Mike</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>Mike</td>
      <td>Mike</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
           CREATE TEMP TABLE temp_customer AS
        SELECT customer_id
             , store_id
             , first_name
             , last_name
        FROM   public.customer
        WHERE  store_id =1;
            ''')
```


```python
query = f'''
        select * from temp_customer;
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
      <th>customer_id</th>
      <th>store_id</th>
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>Mike</td>
      <td>Mike</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>Mike</td>
      <td>Mike</td>
    </tr>
  </tbody>
</table>
</div>



### использование временной таблицы в функции


```python
cur.execute('''
            Create or replace function use_temp_table() returns 
            table(first_name text, last_name text) as
            $$
            begin
            DROP TABLE IF EXISTS temp_table CASCADE;
            CREATE TEMP TABLE temp_table  AS
            SELECT customer.first_name
                 , customer.last_name
            FROM   customer
            WHERE  store_id =1;

             return query
                SELECT  temp_table.first_name::text, temp_table.last_name::text FROM 
                  temp_table;
            END
            $$ language plpgsql;
            ''')

cur.execute('''
            select * from  use_temp_table();
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['first_name', 'last_name'])
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
      <th>first_name</th>
      <th>last_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mike</td>
      <td>Mike</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Mike</td>
      <td>Mike</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.rollback()
conn.close()
```

### Просмотр pg_locks показывает, какие блокировки предоставлены и какие процессы ожидают получения блокировок.



```python
query = f'''
         select relation::regclass, * from pg_locks where not granted;
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
      <th>relation</th>
      <th>locktype</th>
      <th>database</th>
      <th>relation</th>
      <th>page</th>
      <th>tuple</th>
      <th>virtualxid</th>
      <th>transactionid</th>
      <th>classid</th>
      <th>objid</th>
      <th>objsubid</th>
      <th>virtualtransaction</th>
      <th>pid</th>
      <th>mode</th>
      <th>granted</th>
      <th>fastpath</th>
      <th>waitstart</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>



### Следующий запрос может быть полезен, чтобы увидеть, какие процессы блокируют операторы SQL 
### они находят блокировки только на уровне строк, но не на уровне объектов).


```python
query = f'''
          SELECT blocked_locks.pid     AS blocked_pid,
         blocked_activity.usename  AS blocked_user,
         blocking_locks.pid     AS blocking_pid,
         blocking_activity.usename AS blocking_user,
         blocked_activity.query    AS blocked_statement,
         blocking_activity.query   AS current_statement_in_blocking_process
   FROM  pg_catalog.pg_locks         blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks         blocking_locks 
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
    JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.granted;
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
      <th>blocked_pid</th>
      <th>blocked_user</th>
      <th>blocking_pid</th>
      <th>blocking_user</th>
      <th>blocked_statement</th>
      <th>current_statement_in_blocking_process</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>



### Эффекты команды SET LOCAL для переменной ограничиваются процедурой\функцией, внутри которой выполняется команда; 
### предыдущее значение параметра конфигурации восстанавливается после выхода из процедуры. 
###  Эффекты конфигурации сохранятся после выхода из процедуры\функции, 
### если текущая транзакция не будет отменена
### console


```python
cur.execute('''
            CREATE OR REPLACE PROCEDURE datestyle_change() 
            LANGUAGE plpgsql 
            SET datestyle TO postgres, dmy
            AS $$
                BEGIN
                    RAISE NOTICE 'Current Date is : % ', now();
                END;
            $$ ;

            ''')

cur.execute('''
            call datestyle_change();
            ''')

print(conn.notices)
```

    ['NOTICE:  relation "customer" already exists, skipping\n', 'NOTICE:  table "temp_table" does not exist, skipping\n', 'NOTICE:  Current Date is : Sat 22 Jul 19:26:39.891879 2023 UTC \n']

