```python
import pandas as pd
import mysql.connector
import sqlalchemy
```


```python
cnx.close()
```


```python
cnx = mysql.connector.connect(user='root', password='password', 
                              host='127.0.0.1', database='otus_db', port='3306')
```


```python
cur = cnx.cursor()
```

#### Создадим таблицу products_info
#### product_id - идентификатор продукта
#### product_name - наименование продукта
#### price - цена продукта
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
                price BIGINT UNSIGNED NOT NULL,
                properties VARCHAR(255) NOT NULL,
                description VARCHAR(255) NOT NULL)
                ;
            ''')
```


```python
cur.execute('''
INSERT INTO products_info (product_name, price, properties, description)
SELECT 
  CASE 
    WHEN RAND() < 0.25 THEN 'testy'
    WHEN RAND() < 0.5 THEN 'very testy food'
    WHEN RAND() < 0.75 THEN 'something cool'
    ELSE 'dont eat it'
  END AS product_name,
  FLOOR(RAND() * 10000) + 1 as price,
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
      <th>price</th>
      <th>properties</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>something cool</td>
      <td>6731</td>
      <td>vegetables</td>
      <td>Made in China</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>something cool</td>
      <td>315</td>
      <td>drinks</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>very testy food</td>
      <td>9888</td>
      <td>vegetables</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>something cool</td>
      <td>8493</td>
      <td>fruits</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>dont eat it</td>
      <td>6334</td>
      <td>fruits</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>995</th>
      <td>996</td>
      <td>something cool</td>
      <td>2818</td>
      <td>drinks</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>996</th>
      <td>997</td>
      <td>very testy food</td>
      <td>7592</td>
      <td>drinks</td>
      <td>Made in China</td>
    </tr>
    <tr>
      <th>997</th>
      <td>998</td>
      <td>testy</td>
      <td>5536</td>
      <td>hot food</td>
      <td>Made in Russia</td>
    </tr>
    <tr>
      <th>998</th>
      <td>999</td>
      <td>testy</td>
      <td>9778</td>
      <td>vegetables</td>
      <td>Made in USA</td>
    </tr>
    <tr>
      <th>999</th>
      <td>1000</td>
      <td>something cool</td>
      <td>2055</td>
      <td>drinks</td>
      <td>Made in USA</td>
    </tr>
  </tbody>
</table>
<p>1000 rows × 5 columns</p>
</div>




```python
query = f'''
          SELECT
          distinct
          properties,
          description,
          count(product_id) over (partition by properties, description) as count_products,
          max(price) over (partition by properties, description) as max_price
         from products_info
         where price > 4000
         and properties = 'fruits'
         order by count_products
         limit 3
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
      <th>properties</th>
      <th>description</th>
      <th>count_products</th>
      <th>max_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>fruits</td>
      <td>Made in China</td>
      <td>18</td>
      <td>9998</td>
    </tr>
    <tr>
      <th>1</th>
      <td>fruits</td>
      <td>Made in Russia</td>
      <td>25</td>
      <td>9944</td>
    </tr>
    <tr>
      <th>2</th>
      <td>fruits</td>
      <td>Made in USA</td>
      <td>25</td>
      <td>9766</td>
    </tr>
  </tbody>
</table>
</div>



#### Создать процедуру выборки товаров с использованием различных фильтров: категория, цена, производитель, различные дополнительные параметры
#### Также в качестве параметров передавать по какому полю сортировать выборку, и параметры постраничной выдачи


```python
cur.execute('''
            drop procedure if exists products_info_count;
            ''')

cur.execute('''
            CREATE procedure  products_info_count(
             IN in_properties VARCHAR(50),
             IN in_price bigint,
             IN in_limit bigint
             
            )
                    BEGIN
                    SELECT
                      distinct
                      properties,
                      description,
                      count(product_id) over (partition by properties, description) as count_products,
                      max(price) over (partition by properties, description) as max_price
                     from products_info
                     where price > in_price
                     and properties = in_properties
                     order by count_products
                     limit in_limit;
                    END;
            ''')
```


```python
cur.callproc('products_info_count', ['fruits', 4000, 3])
result = []
for result_set in cur.stored_results():
    result.append(result_set.fetchall())
df = pd.DataFrame(result[0], columns=['properties', 'description', 'count_products', 'max_price'])
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
      <th>properties</th>
      <th>description</th>
      <th>count_products</th>
      <th>max_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>fruits</td>
      <td>Made in China</td>
      <td>18</td>
      <td>9998</td>
    </tr>
    <tr>
      <th>1</th>
      <td>fruits</td>
      <td>Made in Russia</td>
      <td>25</td>
      <td>9944</td>
    </tr>
    <tr>
      <th>2</th>
      <td>fruits</td>
      <td>Made in USA</td>
      <td>25</td>
      <td>9766</td>
    </tr>
  </tbody>
</table>
</div>



#### Создаем пользователя 'client' с паролем 'client_password'
#### Создаем пользователя 'manager' с паролем 'manager_password'


```python
cur.execute('''
            CREATE USER 'client'@'localhost' IDENTIFIED BY 'client_password';
            ''')

cur.execute('''
            CREATE USER 'manager'@'localhost' IDENTIFIED BY 'manager_password';
            ''')
```

#### Даем права пользователю 'client' на запуск процедуры 'products_info_count'


```python
cur.execute('''
            GRANT EXECUTE ON PROCEDURE otus_db.products_info_count TO 'client'@'localhost';
            ''')
```


```python
cur.execute('''
            drop TABLE if exists products_sales 
                ;
            ''')

cur.execute('''
            CREATE TABLE products_sales (
                  id INT AUTO_INCREMENT PRIMARY KEY,
                  product_id BIGINT,
                  date_sales TIMESTAMP,
                  quantity BIGINT
                );
            ''')


cur.execute('''
            INSERT INTO products_sales (product_id, date_sales, quantity)
                SELECT 
                  FLOOR(RAND() * 100) + 1 AS product_id,
                  NOW() - INTERVAL FLOOR(RAND() * 365) DAY AS date_sales,
                  FLOOR(RAND() * 100) + 1 AS quantity
                FROM
                  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) AS t1,
                  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) AS t2,
                  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) AS t3,
                  (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) AS t4
                LIMIT 1000;
            ''')

query = f'''
          SELECT
          *
         from products_sales
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
      <th>product_id</th>
      <th>date_sales</th>
      <th>quantity</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>47</td>
      <td>2022-12-11 11:29:11</td>
      <td>54</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>31</td>
      <td>2022-10-24 11:29:11</td>
      <td>69</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>68</td>
      <td>2023-06-04 11:29:11</td>
      <td>53</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>69</td>
      <td>2022-11-20 11:29:11</td>
      <td>19</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>39</td>
      <td>2023-05-17 11:29:11</td>
      <td>64</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>620</th>
      <td>621</td>
      <td>89</td>
      <td>2023-09-08 11:29:11</td>
      <td>56</td>
    </tr>
    <tr>
      <th>621</th>
      <td>622</td>
      <td>65</td>
      <td>2023-03-08 11:29:11</td>
      <td>82</td>
    </tr>
    <tr>
      <th>622</th>
      <td>623</td>
      <td>45</td>
      <td>2022-12-15 11:29:11</td>
      <td>55</td>
    </tr>
    <tr>
      <th>623</th>
      <td>624</td>
      <td>38</td>
      <td>2023-06-21 11:29:11</td>
      <td>18</td>
    </tr>
    <tr>
      <th>624</th>
      <td>625</td>
      <td>11</td>
      <td>2022-10-03 11:29:11</td>
      <td>58</td>
    </tr>
  </tbody>
</table>
<p>625 rows × 4 columns</p>
</div>



#### Создать процедуру get_orders - которая позволяет просматривать отчет по продажам за определенный период (час, день, неделя)
#### с различными уровнями группировки (по товару, по категории, по производителю)


```python
query = f'''
          select
          ps.product_id,
          sum(ps.quantity),
          pi.properties,
          pi.description,
          MONTH(date_sales) as time
          from products_sales ps
          left join products_info pi
          on pi.product_id = ps.product_id
          where 
          pi.properties = 'drinks'
          and pi.description = 'Made in Russia'
          and DATE_FORMAT(ps.date_sales, '%Y-%m') = '2023-02'
          group by ps.product_id, time, pi.properties, pi.description
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
      <th>sum(ps.quantity)</th>
      <th>properties</th>
      <th>description</th>
      <th>time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>57</td>
      <td>66.0</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>46</td>
      <td>83.0</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>45</td>
      <td>58.0</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>63</td>
      <td>2.0</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>25</td>
      <td>51.0</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>75</td>
      <td>77.0</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>6</th>
      <td>44</td>
      <td>79.0</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>91</td>
      <td>23.0</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop procedure if exists get_orders;
            ''')

cur.execute('''
            CREATE procedure get_orders(
             IN in_time VARCHAR(50),
             IN in_drinks VARCHAR(50),
             IN in_description VARCHAR(50),
             IN in_date VARCHAR(50)
            )
            BEGIN
                SET @sql = CONCAT('SELECT
                      ps.product_id,
                      SUM(ps.quantity),
                      pi.properties,
                      pi.description,
                      ', in_time, '(date_sales) AS time
                      FROM products_sales ps
                      LEFT JOIN products_info pi ON pi.product_id = ps.product_id
                      WHERE 
                      pi.properties = "', in_drinks, '"
                      AND pi.description = "', in_description, '"
                      AND DATE_FORMAT(ps.date_sales, ''%Y-%m'') = "', in_date, '"
                      GROUP BY ps.product_id, time, pi.properties, pi.description');
                      
                PREPARE stmt FROM @sql;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
            END;
            ''')

cur.callproc('get_orders', ['MONTH', 'drinks', 'Made in Russia', '2023-02'])
result = []
for result_set in cur.stored_results():
    result.append(result_set.fetchall())
df = pd.DataFrame(result[0], columns=['product_id', 'sum', 'properties', 'description', 'time'])
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
      <th>product_id</th>
      <th>sum</th>
      <th>properties</th>
      <th>description</th>
      <th>time</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>57</td>
      <td>66</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>46</td>
      <td>83</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>45</td>
      <td>58</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>63</td>
      <td>2</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>25</td>
      <td>51</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>75</td>
      <td>77</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>6</th>
      <td>44</td>
      <td>79</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
    <tr>
      <th>7</th>
      <td>91</td>
      <td>23</td>
      <td>drinks</td>
      <td>Made in Russia</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



#### Даем права пользователю 'manager' на запуск процедуры 'get_orders'


```python
cur.execute('''
            GRANT EXECUTE ON PROCEDURE otus_db.get_orders TO 'manager'@'localhost';
            ''')
```
