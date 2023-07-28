Функции позволяют выполнять только Select-запросы, а хранимые процедуры позволяют выполнять Insert , Update , Delete операции. Хранимые процедуры очень удобны при работе со случаями, когда необходимы операции insert , update или delete .

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

### подготовим таблицу и данные


```python
cur = conn.cursor()

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
  </tbody>
</table>
</div>



## выполнение кода без определения функции или процедуры



```python
cur = conn.cursor()

cur.execute('''
           DROP TABLE table_2
            ''')
conn.commit()
```


```python
cur = conn.cursor()

cur.execute('''
           do $$
            DECLARE
                i INTEGER;
            BEGIN
                    FOR i IN 1 .. 2 
                    LOOP
                        RAISE Notice 'i = %', i;
                        execute ('create table table_' || i || '(id int);');
                    END LOOP;
            end $$;	
            ''')
conn.commit()
```


```python
query = f'''
        select* from table_1
'''
df = pd.read_sql_query(query, conn)
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
      <th>id</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>



### до pg 11 были только функции -->


```python
cur = conn.cursor()

cur.execute('''
           create or replace function first_function()
            returns setof customer
            as $$
            select * from customer;
            $$
            language sql
            ''')


cur.execute('''
           select * from first_function();
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
  </tbody>
</table>
</div>



### Процедура копирования определения таблицы  DDL внутри процедуры



```python
cur.execute('''
           Create or replace PROCEDURE copy_table(
			name_old text,
			name_new text)
            language 'plpgsql'
            As $$
            declare str_table text;
            begin
                str_table:= 'create table ' || name_new || ' as select * from ' || name_old ;
                execute str_table;
                raise notice 'str = %', str_table;
            end $$	
            ''')

cur.execute('''
           call copy_table('customer', 'copy_customer');
            ''')

cur.execute('''
           select* from copy_customer;
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
  </tbody>
</table>
</div>



### Вывести все таблицы заданной схемы


```python
cur.execute('''
           Create or replace PROCEDURE view_tables(name_sch text)
            language 'plpgsql'
            as $$
            DECLARE
                r record;
                cnt int;
            begin
                For r in
                    select table_name from information_schema.TABLES
                    where table_schema = name_sch
                    order by table_name desc
                loop
                    execute 'select count(*) cnt from ' ||  r.table_name into cnt;
                    raise notice '% - %', r.table_name, cnt;
                end loop;
            end	$$; 
            ''')

cur.execute('''
           call view_tables('public');
            ''')

print(conn.notices)
```

    ['NOTICE:  table_2 - 0\n', 'NOTICE:  table_1 - 0\n']



```python
cur.execute('''
           call view_tables('clients');
            ''')

print(conn.notices)
```

    ['NOTICE:  table_2 - 0\n', 'NOTICE:  table_1 - 0\n']


### Транзакции в хранимой процедуре



```python
cur.execute('''

            Create or replace PROCEDURE public.trans_in_sp()
            language 'plpgsql'
            as $$
            DECLARE
                r record;
                cnt int;
            begin
                create table IF NOT EXISTS t2(id int ); --ddl
                insert into t2 values(1);
                insert into t2 values(3);
            -- Новая транзакция запускается автоматически после завершения 
            -- транзакции с использованием этих команд, поэтому отдельной 
            -- команды START TRANSACTION не существует. 
           
           --dml
                --update t1
                --set id =2;
                --commit; --COMMIT TRANSACTION

            end	$$; 
            ''')

cur.execute('''
           call public.trans_in_sp();
            ''')

cur.execute('''
           select * from t2;
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['id'])
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
      <th>id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



### удаление процедуры



```python
cur.execute('''
           DROP PROCEDURE IF exists public.trans_in_sp()
            ''')
print(conn.notices)
```

    []


### обработка ошибок



```python
cur.execute('''

            Create or replace PROCEDURE public.error_handl_sp()
            language 'plpgsql'
            AS $$
            BEGIN
              CREATE TABLE test_table(
                name varchar UNIQUE
              );

              INSERT INTO test_table(name) VALUES('my name');
              INSERT INTO test_table(name) VALUES('my name'); 
             EXCEPTION
              WHEN others then
                RAISE NOTICE 'SQLSTATE: %', SQLSTATE;--Команда RAISE предназначена для вывода сообщений и вызова ошибок.
                --RAISE SQLSTATE '22012';
                --RAISE EXCEPTION 'Дубликат --> %', 'my name'
                --  USING HINT = 'Проверьте уникальность';
              /*
               WHEN unique_violation THEN
               RAISE NOTICE 'Illegal operation: %', SQLERRM;
                */
                
            END $$;


            ''')

cur.execute('''
           call public.error_handl_sp();
            ''')
print(conn.notices)
```

    ['NOTICE:  SQLSTATE: 23505\n']



```python
conn.rollback()
conn.close()
```
