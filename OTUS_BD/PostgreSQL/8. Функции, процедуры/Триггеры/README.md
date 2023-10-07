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
query = f'''
        select * from pg_trigger;
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
      <th>oid</th>
      <th>tgrelid</th>
      <th>tgparentid</th>
      <th>tgname</th>
      <th>tgfoid</th>
      <th>tgtype</th>
      <th>tgenabled</th>
      <th>tgisinternal</th>
      <th>tgconstrrelid</th>
      <th>tgconstrindid</th>
      <th>tgconstraint</th>
      <th>tgdeferrable</th>
      <th>tginitdeferred</th>
      <th>tgnargs</th>
      <th>tgattr</th>
      <th>tgargs</th>
      <th>tgqual</th>
      <th>tgoldtable</th>
      <th>tgnewtable</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>17774</td>
      <td>16450</td>
      <td>0</td>
      <td>RI_ConstraintTrigger_a_17774</td>
      <td>1654</td>
      <td>9</td>
      <td>O</td>
      <td>True</td>
      <td>16460</td>
      <td>17711</td>
      <td>17773</td>
      <td>False</td>
      <td>False</td>
      <td>0</td>
      <td></td>
      <td>[]</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>17775</td>
      <td>16450</td>
      <td>0</td>
      <td>RI_ConstraintTrigger_a_17775</td>
      <td>1655</td>
      <td>17</td>
      <td>O</td>
      <td>True</td>
      <td>16460</td>
      <td>17711</td>
      <td>17773</td>
      <td>False</td>
      <td>False</td>
      <td>0</td>
      <td></td>
      <td>[]</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>17776</td>
      <td>16460</td>
      <td>0</td>
      <td>RI_ConstraintTrigger_c_17776</td>
      <td>1644</td>
      <td>5</td>
      <td>O</td>
      <td>True</td>
      <td>16450</td>
      <td>17711</td>
      <td>17773</td>
      <td>False</td>
      <td>False</td>
      <td>0</td>
      <td></td>
      <td>[]</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>17777</td>
      <td>16460</td>
      <td>0</td>
      <td>RI_ConstraintTrigger_c_17777</td>
      <td>1645</td>
      <td>17</td>
      <td>O</td>
      <td>True</td>
      <td>16450</td>
      <td>17711</td>
      <td>17773</td>
      <td>False</td>
      <td>False</td>
      <td>0</td>
      <td></td>
      <td>[]</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>17779</td>
      <td>16648</td>
      <td>0</td>
      <td>RI_ConstraintTrigger_a_17779</td>
      <td>1654</td>
      <td>9</td>
      <td>O</td>
      <td>True</td>
      <td>16591</td>
      <td>17471</td>
      <td>17778</td>
      <td>False</td>
      <td>False</td>
      <td>0</td>
      <td></td>
      <td>[]</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




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



### создание табилцы логов


```python
cur.execute('''
            create table logs (id serial primary key, 
            notes text, 
            added timestamp);
            ''')

cur.execute('''
            create or replace function add_to_log() returns trigger as $$
            declare
                str_total text;
            begin
                if tg_op = 'INSERT' or tg_op = 'UPDATE' then
                str_total := tg_relname || ' ' || new || ' ' || tg_op;
                insert into logs(notes, added) values (str_total, now());
                return new;
                elseif tg_op = 'DELETE' then
                str_total := tg_relname || ' ' || old || ' ' || tg_op;
                insert into logs(notes,added) values (str_total, now());
                return old;
                end if;
            end;
            $$ language plpgsql;

            ''')
```


```python
cur.execute('''
            create trigger tr_customer
            after insert or update or delete on public.customer 
            for each row execute function add_to_log();

            ''')
```


```python
cur.execute('''
            insert into customer values (999,10,'fntest','lntest');
            ''')


cur.execute('''
            delete from customer where customer_id  = 999;
            ''')

cur.execute('''
           select * from logs;
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['id', 'notes', 'added'])
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
      <th>notes</th>
      <th>added</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>customer (999,10,fntest,lntest) INSERT</td>
      <td>2023-07-22 19:14:19.566122</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>customer (999,10,fntest,lntest) DELETE</td>
      <td>2023-07-22 19:14:19.566122</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>customer (999,10,fntest,lntest) INSERT</td>
      <td>2023-07-22 19:14:19.566122</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>customer (999,10,fntest,lntest) DELETE</td>
      <td>2023-07-22 19:14:19.566122</td>
    </tr>
  </tbody>
</table>
</div>



### если вы также хотите увидеть, к какой таблице применяется каждый триггер 


```python
query = f'''
        select tgrelid::regclass, tgname from pg_trigger;
'''
df = pd.read_sql_query(query, conn)
df.head()
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
      <th>tgrelid</th>
      <th>tgname</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>person.stateprovince</td>
      <td>RI_ConstraintTrigger_a_17774</td>
    </tr>
    <tr>
      <th>1</th>
      <td>person.stateprovince</td>
      <td>RI_ConstraintTrigger_a_17775</td>
    </tr>
    <tr>
      <th>2</th>
      <td>person.address</td>
      <td>RI_ConstraintTrigger_c_17776</td>
    </tr>
    <tr>
      <th>3</th>
      <td>person.address</td>
      <td>RI_ConstraintTrigger_c_17777</td>
    </tr>
    <tr>
      <th>4</th>
      <td>production.product</td>
      <td>RI_ConstraintTrigger_a_17779</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.rollback()
conn.close()
```
