### Обычно курсоры автоматически закрываются при фиксации транзакции. Однако курсор, созданный как часть такого цикла,

### автоматически преобразуется в удерживаемый курсор при первом выполнении COMMIT или ROLLBACK.

### Это означает, что курсор полностью вычисляется при первом COMMIT или ROLLBACK, а не построчно.

### Курсор по-прежнему автоматически удаляется после цикла, поэтому это в основном невидимо для пользователя.


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
  </tbody>
</table>
</div>




```python
cur.execute('''
           create or replace function return_cursor() 
            returns refcursor as
            $$
            DECLARE
                res CONSTANT refcursor := '_result';
            begin
                open res for select * from customer;
                return res;

            end
            $$ language plpgsql;
            ''')

cur.execute('''
           begin;
            ''')

cur.execute('''
           begin;
            select * from return_cursor();
            fetch all from _result;
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
cur.execute('''
            -- получить последнего
            begin;
            select * from return_cursor();
            fetch last from _result;
            commit;
            --получить 4-го
            begin;
            select * from return_cursor();
            MOVE FORWARD 3 FROM _result;
            fetch next from _result;
            commit;
            --CLOSE закрывает связанный с курсором портал. 
            --Используется для того, чтобы освободить ресурсы раньше, чем закончится транзакция, 
            -- или чтобы освободить курсорную переменную для повторного открытия.
            ''')
```


```python
conn.rollback()
conn.close()
```
