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


```python
группировки с ипользованием CASE, HAVING, ROLLUP, GROUPING() :
для магазина к предыдущему списку продуктов добавить максимальную и минимальную цену и кол-во предложений
сделать выборку показывающую самый дорогой и самый дешевый товар в каждой категории
сделать rollup с количеством товаров по категориям
```


      Input In [25]
        группировки с ипользованием CASE, HAVING, ROLLUP, GROUPING() :
                    ^
    SyntaxError: invalid syntax



#### Создадим таблицу products и наполним данными 
##### title - наименование товара
##### category - категория товара
##### price - цена товара
##### rating - рейтинг товара
##### sales - кол-во продаж товара
##### sales - кол-во продаж товара
##### status - статус наличия товара


```python
cur.execute('''
            DROP TABLE IF EXISTS products;
            ''')

cur.execute('''
            CREATE TABLE products (
                title VARCHAR(32) NOT NULL,
                category VARCHAR(32),
                price INT,
                rating INT,
                sales INT,
                status VARCHAR(32) NOT NULL,
                PRIMARY KEY (title));
            ''')

cur.execute('''
            INSERT INTO products
                (title, category, price, rating, sales, status)
            VALUES
                ('Агдам', 'Напитки', 150, 2, 4, 'В наличии'),
                ('Килька', 'Консервы', 45, 4, 10, 'Распродан'),
                ('Оливки', 'Консервы', 250, 5, 24, 'Распродан'),
                ('Текила', 'Напитки', 3000, 5, 23, 'В наличии'),
                ('Шмурдяк', 'Напитки', 120, 1, 15, 'Распродан'),
                ('Арахис', 'Орехи', 250, 5, 12, 'Распродан'),
                ('Фисташки', 'Орехи', 450, 5, 4, 'В наличии');
            ''')
query = f'''
            SELECT * from products;
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
      <th>title</th>
      <th>category</th>
      <th>price</th>
      <th>rating</th>
      <th>sales</th>
      <th>status</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Агдам</td>
      <td>Напитки</td>
      <td>150</td>
      <td>2</td>
      <td>4</td>
      <td>В наличии</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Арахис</td>
      <td>Орехи</td>
      <td>250</td>
      <td>5</td>
      <td>12</td>
      <td>Распродан</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Килька</td>
      <td>Консервы</td>
      <td>45</td>
      <td>4</td>
      <td>10</td>
      <td>Распродан</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Оливки</td>
      <td>Консервы</td>
      <td>250</td>
      <td>5</td>
      <td>24</td>
      <td>Распродан</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Текила</td>
      <td>Напитки</td>
      <td>3000</td>
      <td>5</td>
      <td>23</td>
      <td>В наличии</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Фисташки</td>
      <td>Орехи</td>
      <td>450</td>
      <td>5</td>
      <td>4</td>
      <td>В наличии</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Шмурдяк</td>
      <td>Напитки</td>
      <td>120</td>
      <td>1</td>
      <td>15</td>
      <td>Распродан</td>
    </tr>
  </tbody>
</table>
</div>



#### Группировки с ипользованием CASE
#####  Наличие товаров каждой категории


```python
query = f'''
          SELECT
          category,
          SUM( CASE WHEN status = 'В наличии'
                THEN 1
                ELSE 0
                END ) AS total
        FROM products
        GROUP BY category;
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
      <th>category</th>
      <th>total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Напитки</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Орехи</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Консервы</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>



#### Группировки с ипользованием HAVING
#### Вывести категории товары которых были проданы более 20 раз


```python
query = f'''
          SELECT
          category,
          SUM(sales) AS total
        FROM products
        GROUP BY category
        HAVING total > 20;
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
      <th>category</th>
      <th>total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Напитки</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Консервы</td>
      <td>34.0</td>
    </tr>
  </tbody>
</table>
</div>



#### Группировки с ипользованием ROLLUP,  GROUPING()
#####  Вывести кол-во продаж каждой категории и общий итог 


```python
query = f'''
        SELECT
          IF(GROUPING(category), 'ИТОГО', category) AS category,
          SUM(sales) AS total
        FROM products
        GROUP BY category WITH ROLLUP;
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
      <th>category</th>
      <th>total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Консервы</td>
      <td>34.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Напитки</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Орехи</td>
      <td>16.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ИТОГО</td>
      <td>92.0</td>
    </tr>
  </tbody>
</table>
</div>



#### для магазина к предыдущему списку продуктов добавить максимальную и минимальную цену и кол-во предложений


```python
query = f'''
        SELECT
          category,
          title,
          price,
          sales,
          min(price) over (partition by category) as min_price_in_category,
          max(price) over (partition by category) as max_price_in_category,
          sum(sales) over (partition by category) as sales_in_category
        FROM products;
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
      <th>category</th>
      <th>title</th>
      <th>price</th>
      <th>sales</th>
      <th>min_price_in_category</th>
      <th>max_price_in_category</th>
      <th>sales_in_category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Консервы</td>
      <td>Килька</td>
      <td>45</td>
      <td>10</td>
      <td>45</td>
      <td>250</td>
      <td>34.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Консервы</td>
      <td>Оливки</td>
      <td>250</td>
      <td>24</td>
      <td>45</td>
      <td>250</td>
      <td>34.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Напитки</td>
      <td>Агдам</td>
      <td>150</td>
      <td>4</td>
      <td>120</td>
      <td>3000</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Напитки</td>
      <td>Текила</td>
      <td>3000</td>
      <td>23</td>
      <td>120</td>
      <td>3000</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Напитки</td>
      <td>Шмурдяк</td>
      <td>120</td>
      <td>15</td>
      <td>120</td>
      <td>3000</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Орехи</td>
      <td>Арахис</td>
      <td>250</td>
      <td>12</td>
      <td>250</td>
      <td>450</td>
      <td>16.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Орехи</td>
      <td>Фисташки</td>
      <td>450</td>
      <td>4</td>
      <td>250</td>
      <td>450</td>
      <td>16.0</td>
    </tr>
  </tbody>
</table>
</div>



#### сделать выборку показывающую самый дорогой и самый дешевый товар в каждой категории


```python
query = f'''
with min_max as (
        SELECT
          category,
          title,
          price,
          min(price) over (partition by category) as min_price,
          max(price) over (partition by category) as max_price
        FROM products)
        
select 
        l.category,
        l.title as min_title,
        r.title as max_title
        from min_max l
        join min_max r
        on l.category = r.category
        and r.max_price = r.price
        where l.min_price = l.price;
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
      <th>category</th>
      <th>min_title</th>
      <th>max_title</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Консервы</td>
      <td>Килька</td>
      <td>Оливки</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Напитки</td>
      <td>Шмурдяк</td>
      <td>Текила</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Орехи</td>
      <td>Арахис</td>
      <td>Фисташки</td>
    </tr>
  </tbody>
</table>
</div>



#### сделать rollup с количеством товаров по категориям


```python
query = f'''
        SELECT
          IF(GROUPING(category), 'ИТОГО', category) AS category,
          SUM( CASE WHEN status = 'В наличии'
                THEN 1
                ELSE 0
                END ) AS total
        FROM products
        GROUP BY category WITH ROLLUP;
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
      <th>category</th>
      <th>total</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Консервы</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Напитки</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Орехи</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ИТОГО</td>
      <td>3.0</td>
    </tr>
  </tbody>
</table>
</div>


