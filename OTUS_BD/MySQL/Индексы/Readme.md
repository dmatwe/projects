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

### Создадим таблицу products_info
#### product_id - идентификатор продукта
#### product_name - наименование продукта 
#### properties - свойства продукта 
#### description - описание продукта


```python
cur.execute('''
            DROP TABLE IF EXISTS products_info;
            ''')

cur.execute('''
            CREATE TABLE products_info (
                product_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                properties VARCHAR(255) NOT NULL,
                description VARCHAR(255) NOT NULL)
                ;
            ''')
```

 #### Наполним таблицу случайными данными размером 1000 строк 


```python
cur.execute('''
INSERT INTO products_info (product_name, properties, description)
SELECT 
  CASE 
    WHEN RAND() < 0.25 THEN 'testy'
    WHEN RAND() < 0.5 THEN 'very testy food'
    WHEN RAND() < 0.75 THEN 'something cool'
    ELSE 'dont eat it'
  END AS product_name,
  CASE 
    WHEN RAND() < 0.25 THEN 'hot food'
    WHEN RAND() < 0.5 THEN 'drinks'
    WHEN RAND() < 0.75 THEN 'vegetables'
    ELSE 'fruits'
  END AS properties,
  CASE 
    WHEN RAND() < 0.33 THEN 'Made in Russia'
    WHEN RAND() < 0.66 THEN 'Made in USA'
    ELSE 'Made in China'
  END AS description
FROM 
  (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) t1
   CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) t2
   CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) t3
   CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) t4
   CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) t5
   LIMIT 1000;
            ''')

query = f'''
          SELECT
          *
         from products_info
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
      <th>product_id</th>
      <th>product_name</th>
      <th>properties</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>something cool</td>
      <td>drinks</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>very testy food</td>
      <td>fruits</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>testy</td>
      <td>hot food</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>testy</td>
      <td>drinks</td>
      <td>Made in China</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>995</th>
      <td>996</td>
      <td>testy</td>
      <td>drinks</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>996</th>
      <td>997</td>
      <td>something cool</td>
      <td>hot food</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>997</th>
      <td>998</td>
      <td>something cool</td>
      <td>drinks</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>998</th>
      <td>999</td>
      <td>something cool</td>
      <td>vegetables</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>999</th>
      <td>1000</td>
      <td>testy</td>
      <td>drinks</td>
      <td>Made in USA</td>
    </tr>
  </tbody>
</table>
<p>1000 rows × 4 columns</p>
</div>




```python
cur.execute('''
            set profiling = 1;
            ''')

query = f'''
        select* from products_info
        where product_name  like '%food%'
        and properties like '%hot%'
        and description like '%Russia%'
        '''
df = pd.read_sql_query(query, cnx)
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
      <th>product_id</th>
      <th>product_name</th>
      <th>properties</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>1</th>
      <td>27</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>2</th>
      <td>109</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>3</th>
      <td>111</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>4</th>
      <td>126</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''
            show profiles;
        '''
df = pd.read_sql_query(query, cnx)
df.tail(1)
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
      <th>Query_ID</th>
      <th>Duration</th>
      <th>Query</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>14</th>
      <td>49</td>
      <td>0.004788</td>
      <td>select* from products_info\n        where prod...</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''
        EXPLAIN
        select* from products_info
        where product_name  like '%food%'
        and properties like '%hot%'
        and description like '%Russia%'
        '''
df = pd.read_sql_query(query, cnx)
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
      <td>SIMPLE</td>
      <td>products_info</td>
      <td>None</td>
      <td>ALL</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1000</td>
      <td>0.14</td>
      <td>Using where</td>
    </tr>
  </tbody>
</table>
</div>



### Создадим полнотекстовый индекс на:
#### product_name - наименование продукта 
#### properties - свойства продукта 
#### description - описание продукта


```python
cur.execute('''
            CREATE FULLTEXT index 
                fulltext_inx_product_name
                on products_info 
                (product_name)
                ;
            ''')

cur.execute('''
            CREATE FULLTEXT index 
                fulltext_inx_properties
                on products_info 
                (properties)
                ;
            ''')


cur.execute('''
            CREATE FULLTEXT index 
                fulltext_inx_description
                on products_info 
                (description)
                ;
            ''')
```

### Посмотреть индексы таблицы


```python
query = f'''
        SHOW INDEX from products_info
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
      <th>Table</th>
      <th>Non_unique</th>
      <th>Key_name</th>
      <th>Seq_in_index</th>
      <th>Column_name</th>
      <th>Collation</th>
      <th>Cardinality</th>
      <th>Sub_part</th>
      <th>Packed</th>
      <th>Null</th>
      <th>Index_type</th>
      <th>Comment</th>
      <th>Index_comment</th>
      <th>Visible</th>
      <th>Expression</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>products_info</td>
      <td>0</td>
      <td>PRIMARY</td>
      <td>1</td>
      <td>product_id</td>
      <td>A</td>
      <td>1000</td>
      <td>None</td>
      <td>None</td>
      <td></td>
      <td>BTREE</td>
      <td></td>
      <td></td>
      <td>YES</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>products_info</td>
      <td>1</td>
      <td>fulltext_inx_product_name</td>
      <td>1</td>
      <td>product_name</td>
      <td>None</td>
      <td>1000</td>
      <td>None</td>
      <td>None</td>
      <td></td>
      <td>FULLTEXT</td>
      <td></td>
      <td></td>
      <td>YES</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>products_info</td>
      <td>1</td>
      <td>fulltext_inx_properties</td>
      <td>1</td>
      <td>properties</td>
      <td>None</td>
      <td>1000</td>
      <td>None</td>
      <td>None</td>
      <td></td>
      <td>FULLTEXT</td>
      <td></td>
      <td></td>
      <td>YES</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>products_info</td>
      <td>1</td>
      <td>fulltext_inx_description</td>
      <td>1</td>
      <td>description</td>
      <td>None</td>
      <td>1000</td>
      <td>None</td>
      <td>None</td>
      <td></td>
      <td>FULLTEXT</td>
      <td></td>
      <td></td>
      <td>YES</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''
        EXPLAIN
        select* from products_info
        where match (product_name)  against ('food')
        and match (properties)  against ('hot')
        and match (description)  against ('Russia')
        
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
      <td>SIMPLE</td>
      <td>products_info</td>
      <td>None</td>
      <td>fulltext</td>
      <td>fulltext_inx_product_name,fulltext_inx_propert...</td>
      <td>fulltext_inx_product_name</td>
      <td>0</td>
      <td>const</td>
      <td>1</td>
      <td>5.0</td>
      <td>Using where; Ft_hints: sorted</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''
        select* from products_info
        where match (product_name)  against ('food')
        and match (properties)  against ('hot')
        and match (description)  against ('Russia')
        
        '''
df = pd.read_sql_query(query, cnx)
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
      <th>product_id</th>
      <th>product_name</th>
      <th>properties</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>1</th>
      <td>27</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>2</th>
      <td>109</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>3</th>
      <td>111</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>4</th>
      <td>126</td>
      <td>very testy food</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
  </tbody>
</table>
</div>




```python
query = f'''
            show profiles;
        '''
df = pd.read_sql_query(query, cnx)
df.tail(1)
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
      <th>Query_ID</th>
      <th>Duration</th>
      <th>Query</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>14</th>
      <td>54</td>
      <td>0.003588</td>
      <td>select* from products_info\n        where matc...</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.close()
cnx.close()
```
