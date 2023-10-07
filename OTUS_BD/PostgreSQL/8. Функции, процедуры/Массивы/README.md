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

### смотрим, что за данные


```python
query = f'''
         select * from sales.customer
        where territoryid =8
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
      <th>customerid</th>
      <th>personid</th>
      <th>storeid</th>
      <th>territoryid</th>
      <th>rowguid</th>
      <th>modifieddate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14</td>
      <td>NaN</td>
      <td>1020.0</td>
      <td>8</td>
      <td>2f96bedc-723d-468f-834b-b2b8ae79c849</td>
      <td>2014-09-12 11:15:07.263</td>
    </tr>
    <tr>
      <th>1</th>
      <td>32</td>
      <td>NaN</td>
      <td>1008.0</td>
      <td>8</td>
      <td>b9a28813-542a-4fbf-ae79-bad64a5e133b</td>
      <td>2014-09-12 11:15:07.263</td>
    </tr>
    <tr>
      <th>2</th>
      <td>50</td>
      <td>NaN</td>
      <td>996.0</td>
      <td>8</td>
      <td>0a3e846a-2dcc-4b6e-8ab3-968b5ab859f4</td>
      <td>2014-09-12 11:15:07.263</td>
    </tr>
    <tr>
      <th>3</th>
      <td>68</td>
      <td>NaN</td>
      <td>532.0</td>
      <td>8</td>
      <td>2138def5-80eb-4d95-b340-706bcd60adda</td>
      <td>2014-09-12 11:15:07.263</td>
    </tr>
    <tr>
      <th>4</th>
      <td>86</td>
      <td>NaN</td>
      <td>636.0</td>
      <td>8</td>
      <td>2a8b2702-4644-4002-90e4-99a8adba5261</td>
      <td>2014-09-12 11:15:07.263</td>
    </tr>
  </tbody>
</table>
</div>



### Есть два пути. Один из них - агрегировать


```python
query = f'''
         SELECT array_agg(customerid::text)
        FROM sales.customer
        where territoryid =4
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
      <th>array_agg</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>[3, 4, 5, 6, 21, 23, 24, 25, 40, 41, 42, 59, 6...</td>
    </tr>
  </tbody>
</table>
</div>



### Другой - использовать конструктор массива:
### где customerid собирается по territoryid = 8, а массив по where territoryid =10
### это только рекордсет, нельзя обратиться по имени acc_array или cus. используется в констуркции insert into select
### SELECT customerid, array(


```python
query = f'''
         SELECT customerid, array(
        SELECT rowguid ::TEXT
         FROM sales.customer
		where territoryid =10
        )
           AS acc_array 
        FROM sales.customer as cus
        where territoryid = 8;
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
      <th>customerid</th>
      <th>acc_array</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14</td>
      <td>[c9381589-d31c-4efe-8978-8d3449eb1f0f, 9edda79...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>32</td>
      <td>[c9381589-d31c-4efe-8978-8d3449eb1f0f, 9edda79...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>50</td>
      <td>[c9381589-d31c-4efe-8978-8d3449eb1f0f, 9edda79...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>68</td>
      <td>[c9381589-d31c-4efe-8978-8d3449eb1f0f, 9edda79...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>86</td>
      <td>[c9381589-d31c-4efe-8978-8d3449eb1f0f, 9edda79...</td>
    </tr>
  </tbody>
</table>
</div>



### выборки из таблицы, где несколько полей = массив



```python
cur.execute('''
           CREATE TABLE grades (
                student_id int,
                email text[][],
                test_scores int[]
            );
            ''')

cur.execute('''
            INSERT INTO grades
            VALUES(1, '{ {"work", "work1@gmail.com"}, {"home", "home1@gmail.com"} }',
            '{92, 58,98, 100}');
            ''')

cur.execute('''
            SELECT
            email[1][1] as type, -- обращаться можно по индексу массива
            email[1][2] as address,
            test_scores[1]
            FROM
            grades;
            ''')


results = cur.fetchall()
pd.DataFrame(results, columns=['type', 'address', 'test_scores'])
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
      <th>type</th>
      <th>address</th>
      <th>test_scores</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>work</td>
      <td>work1@gmail.com</td>
      <td>92</td>
    </tr>
  </tbody>
</table>
</div>



### создать свой сложный тип и его возвращать



```python
cur.execute('''
            CREATE TYPE compozit AS (f1 text, f2 text);
            ''')

cur.execute('''
            CREATE FUNCTION getCompozit() RETURNS SETOF compozit AS $$
            SELECT customerid, rowguid FROM sales.customer
            $$ LANGUAGE SQL;
            ''')

cur.execute('''
            SELECT * from getCompozit()
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['customerid', 'rowguid'])
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
      <th>customerid</th>
      <th>rowguid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>3f5ae95e-b87d-4aed-95b4-c3797afcb74f</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>e552f657-a9af-4a7d-a645-c429d6e02491</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>130774b1-db21-4ef3-98c8-c104bcd6ed6d</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>ff862851-1daa-4044-be7c-3e85583c054d</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>83905bdc-6f5e-4f71-b162-c98da069f38a</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>19815</th>
      <td>30114</td>
      <td>97154f3d-28f1-4b15-ae03-9518b781ece3</td>
    </tr>
    <tr>
      <th>19816</th>
      <td>30115</td>
      <td>e4cf8fd5-30a4-4b8e-8fd8-47032e255778</td>
    </tr>
    <tr>
      <th>19817</th>
      <td>30116</td>
      <td>ec409609-d25d-41b8-9d15-a1aa6e89fc77</td>
    </tr>
    <tr>
      <th>19818</th>
      <td>30117</td>
      <td>6f08e2fb-1cd3-4f6e-a2e6-385669598b19</td>
    </tr>
    <tr>
      <th>19819</th>
      <td>30118</td>
      <td>2495b4eb-fe8b-459e-a1b6-dba25c04e626</td>
    </tr>
  </tbody>
</table>
<p>19820 rows × 2 columns</p>
</div>




```python
conn.rollback()
conn.close()
```
