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



### вызов функции из функции



```python

cur.execute('''
            Create or replace function call_function_in_function()
            returns text
            as $$
                select first_name from  first_function() limit 1;
            $$ 
            language sql;
            ''')

cur.execute('''
           select * from public.call_function_in_function();
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['first_function'])
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
      <th>first_function</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mike</td>
    </tr>
  </tbody>
</table>
</div>



### Разрешение функций с переменными параметрами


```python
cur.execute('''
            CREATE or REPLACE FUNCTION public.variadic_example(VARIADIC numeric[]) 
                RETURNS int
                  LANGUAGE sql AS 'SELECT 1';

            ''')

cur.execute('''
            SELECT public.variadic_example(0),
               public.variadic_example(0.0),
               public.variadic_example(VARIADIC array[0.0]);

            ''')

results = cur.fetchall()

```


```python
pd.DataFrame(results, columns=['variadic_example_1', 'variadic_example_2',
                               'variadic_example_3'])
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
      <th>variadic_example_1</th>
      <th>variadic_example_2</th>
      <th>variadic_example_3</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>1</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



### Функция преобразования строки 



```python
cur.execute('''
            create or replace function lower_or_upper
            (last_name text, first_name text, flag int default 0)
            returns text
            as $$
              select case $3
                            when 0 then INITCAP ($1 || ' ' || $2)
                            when 1 then upper($1 || ' ' || $2)
                            when 2 then lower($1 || ' ' || $2)
                            else $1 || ' ' || $2
                            end;
            $$ Language sql;

            ''')

cur.execute('''
           select 
           lower_or_upper('СидрОрОВ', 'ИвАн',1);
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['lower_or_upper'])
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
      <th>lower_or_upper</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>СИДРОРОВ ИВАН</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
           select lower_or_upper(last_name=> 'СидрОрОВ', first_name=>'ИвАн');
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['lower_or_upper'])
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
      <th>lower_or_upper</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Сидроров Иван</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
           select lower_or_upper('СидрОрОВ', 'ИвАн', flag=>2);
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['lower_or_upper'])
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
      <th>lower_or_upper</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>сидроров иван</td>
    </tr>
  </tbody>
</table>
</div>



### Функция возвращает несколько значений


```python
cur.execute('''
           create or replace function return_setof_int(max_i integer) 
            returns setof int as
            $$
            declare
                i integer;
            begin
              for i in 1 .. max_i
              loop
             return next i;
            end loop;
            end;
            $$ language plpgsql; 
            ''')

cur.execute('''
           select return_setof_int(5); 
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['return_setof_int'])
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
      <th>return_setof_int</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>



### пример переменной inout



```python
cur.execute('''
           create or replace function return_inout(inout result1 int, out result2 int)
            as $$
            begin
            result2 = result1;  result1= 10*result1;
            return;
            end
            $$ language plpgsql;  
            ''')


cur.execute('''
           select return_inout(9);
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['return_inout'])
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
      <th>return_inout</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>(90,9)</td>
    </tr>
  </tbody>
</table>
</div>



### вывод таблицы customer через функцию


```python
cur.execute('''
            Create or replace function return_customer(out store int) 
            as
            --do
            $$
            begin
                select store_id into store from public.customer limit 1;
                return;
            end 
            $$ language plpgsql; 
            ''')


cur.execute('''
           select * from  public.return_customer();
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['return_customer'])
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
      <th>return_customer</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



### транзакции в функции



```python
cur.execute('''
           Create or replace function fn_transaction() returns setof customer as
            $$
                update customer set first_name= 'Mike' where customer_id = 1;
                select * from customer where customer_id = 1;
            $$ language sql;

            ''')

cur.execute('''
            select * from fn_transaction();
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



### использование переменной VARIADIC массив с переменным набором аргументов
### Выдаёт ряд целых чисел от start до stop с шагом 1
### generate_subscripts — это «функция, возвращающая множество», которая будет возвращать несколько строк при ее вызове. 


```python
cur.execute('''
            CREATE or replace FUNCTION mleast(VARIADIC arr numeric[]) 
            RETURNS numeric AS $$
                SELECT min($1[i]) FROM generate_subscripts($1, 1) g(i);
            $$ LANGUAGE SQL;

            ''')

cur.execute('''
            SELECT mleast(9, -1, 5, 4);
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['mleast'])
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
      <th>mleast</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>-1</td>
    </tr>
  </tbody>
</table>
</div>



 ### Еще один вывод таблицы customer через функцию
 ### цикл for


```python
cur.execute('''
            create or replace function select_customer() 
             returns table(f_name text, l_name text) as 
             $$
             begin  
                 for f_name, l_name in select first_name, last_name from customer
                 loop
                    return next;
                 end loop;
             end
             $$ language plpgsql;
            ''')


cur.execute('''
             select * from select_customer();
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['f_name', 'l_name'])
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
      <th>f_name</th>
      <th>l_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mike</td>
      <td>Mike</td>
    </tr>
  </tbody>
</table>
</div>



 ### функция возвращает сведения о времени последнего процесса vacuum, analyze



```python
cur.execute('''
             create or replace function sql_last_v_a() 
            returns setof record as
            'select schemaname,relname, last_vacuum, last_analyze
            from pg_stat_all_tables
            order by schemaname, relname;' 
            language sql; 
            ''')

cur.execute('''
            select sql_last_v_a();
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['sql_last_v_a'])
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
      <th>sql_last_v_a</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>(humanresources,department,,)</td>
    </tr>
    <tr>
      <th>1</th>
      <td>(humanresources,employee,,)</td>
    </tr>
    <tr>
      <th>2</th>
      <td>(humanresources,employeedepartmenthistory,,)</td>
    </tr>
    <tr>
      <th>3</th>
      <td>(humanresources,employeepayhistory,,)</td>
    </tr>
    <tr>
      <th>4</th>
      <td>(humanresources,jobcandidate,,)</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>226</th>
      <td>(sales,salesterritoryhistory,,)</td>
    </tr>
    <tr>
      <th>227</th>
      <td>(sales,shoppingcartitem,,)</td>
    </tr>
    <tr>
      <th>228</th>
      <td>(sales,specialoffer,,)</td>
    </tr>
    <tr>
      <th>229</th>
      <td>(sales,specialofferproduct,,)</td>
    </tr>
    <tr>
      <th>230</th>
      <td>(sales,store,,)</td>
    </tr>
  </tbody>
</table>
<p>231 rows × 1 columns</p>
</div>



### cистемные функции https://postgrespro.ru/docs/postgrespro/13/functions-info


```python
cur.execute('''
            select current_catalog; 
            --имя текущей базы данных (в стандарте SQL она называется «каталогом»)
            ''')


results = cur.fetchall()
pd.DataFrame(results, columns=['current_catalog'])
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
      <th>current_catalog</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Adventureworks</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''   
        select current_database();
        --имя текущей базы данных
            ''')


results = cur.fetchall()
pd.DataFrame(results, columns=['current_database'])
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
      <th>current_database</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Adventureworks</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''   
        select current_user;
            ''')


results = cur.fetchall()
pd.DataFrame(results, columns=['current_user'])
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
      <th>current_user</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>postgres</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''   
        select version();
            ''')


results = cur.fetchall()
pd.DataFrame(results, columns=['version'])
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
      <th>version</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>PostgreSQL 15.1 (Debian 15.1-1.pgdg110+1) on a...</td>
    </tr>
  </tbody>
</table>
</div>



### CTE FUNC


```python
query = f'''

     select *
     FROM sales.countryregioncurrency
  
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
      <th>countryregioncode</th>
      <th>currencycode</th>
      <th>modifieddate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AE</td>
      <td>AED</td>
      <td>2014-02-08 10:17:21.510</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AR</td>
      <td>ARS</td>
      <td>2014-02-08 10:17:21.510</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AT</td>
      <td>ATS</td>
      <td>2014-02-08 10:17:21.510</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AT</td>
      <td>EUR</td>
      <td>2008-04-30 00:00:00.000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AU</td>
      <td>AUD</td>
      <td>2014-02-08 10:17:21.510</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''   
        Create or replace function public.cte_in_function()
            returns text
            --returns table(full_address text) 
            as $$
            WITH countryregioncurrency_cte AS (
                select  countryregioncode || ' ' ||
                currencycode || ' ' || modifieddate as countryregioncurrency
                FROM sales.countryregioncurrency
               )
            SELECT countryregioncurrency
            FROM countryregioncurrency_cte
            $$ Language sql;
            ''')

cur.execute('''   
        select public.cte_in_function();
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['countryregioncurrency'])
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
      <th>countryregioncurrency</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AE AED 2014-02-08 10:17:21.51</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.rollback()
conn.close()
```
