```python
import pandas as pd
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
```


```python
cnx = mysql.connector.connect(user='root', password='password', 
                              host='127.0.0.1', database='otus_db', port='3306')
cur = cnx.cursor()
```


```python
engine = create_engine('mysql+pymysql://root:password@127.0.0.1/otus_db')
```


```python
df = pd.read_csv('some_customers.csv')
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
      <th>title</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>correspondence_language</th>
      <th>birth_date</th>
      <th>gender</th>
      <th>marital_status</th>
      <th>country</th>
      <th>postal_code</th>
      <th>region</th>
      <th>city</th>
      <th>street</th>
      <th>building_number</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Dr.</td>
      <td>Danilo</td>
      <td>Ambrosini</td>
      <td>IT</td>
      <td>1900-01-01</td>
      <td>Unknown</td>
      <td>NaN</td>
      <td>IT</td>
      <td>21100</td>
      <td>NaN</td>
      <td>Varese</td>
      <td>Via dolomiti</td>
      <td>13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Mrs</td>
      <td>Deborah</td>
      <td>Thomas</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Female</td>
      <td>NaN</td>
      <td>GB</td>
      <td>RH16 3TQ</td>
      <td>NaN</td>
      <td>Haywards heath</td>
      <td>Cobbetts mead</td>
      <td>31</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Herr</td>
      <td>Markus</td>
      <td>Hönninge</td>
      <td>DE</td>
      <td>1900-01-01</td>
      <td>Male</td>
      <td>NaN</td>
      <td>DE</td>
      <td>69493</td>
      <td>NaN</td>
      <td>Hirschberg an der bergstraße</td>
      <td>Breitgasse 5a</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Mrs</td>
      <td>Sheila</td>
      <td>Spiller</td>
      <td>EN</td>
      <td>NaN</td>
      <td>Unknown</td>
      <td>NaN</td>
      <td>GB</td>
      <td>TN6 1ST</td>
      <td>East sussex</td>
      <td>Crowborough</td>
      <td>Ghyll road</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Mrs</td>
      <td>Rosemary</td>
      <td>Bailey</td>
      <td>EN</td>
      <td>NaN</td>
      <td>Unknown</td>
      <td>NaN</td>
      <td>GB</td>
      <td>NR34 7SH</td>
      <td>NaN</td>
      <td>Beccles</td>
      <td>Pepys avenue</td>
      <td>24</td>
    </tr>
  </tbody>
</table>
</div>



### Справочник стран 


```python
df_country = df[['country']].drop_duplicates().reset_index(drop=True).reset_index() \
.rename(columns={"index": "country_id"}).astype({'country': 'string'})
df_country.head()
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
      <th>country_id</th>
      <th>country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>IT</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>GB</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>DE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>PL</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>BE</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_country.dtypes
```




    country_id     int64
    country       string
    dtype: object




```python
cur.execute('''
            drop TABLE if exists dim_country;
            ''')


cur.execute(f'''
CREATE TABLE 
            dim_country (
                country_id INT UNSIGNED NOT null PRIMARY KEY,
                country VARCHAR(25) NOT null);
''')

df_country.to_sql('dim_country', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from dim_country;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>country_id</th>
      <th>country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>IT</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>GB</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>DE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>PL</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>BE</td>
    </tr>
  </tbody>
</table>
</div>



### Справочник регионов 


```python
df_region = df[['region']].drop_duplicates().reset_index(drop=True).reset_index() \
.rename(columns={"index": "region_id"}).astype({'region': 'string'}).fillna('NULL')
df_region.head()
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
      <th>region_id</th>
      <th>region</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>East sussex</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Ancona</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Cornwall</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>Surrey</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_region.dtypes
```




    region_id     int64
    region       string
    dtype: object




```python
cur.execute('''
            drop TABLE if exists dim_region;
            ''')


cur.execute(f'''
CREATE TABLE 
            dim_region (
                region_id INT UNSIGNED NOT null PRIMARY KEY,
                region VARCHAR(25) NOT null);
''')

df_region.to_sql('dim_region', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from dim_region;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>region_id</th>
      <th>region</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>East sussex</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Ancona</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Cornwall</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>Surrey</td>
    </tr>
  </tbody>
</table>
</div>



### Справочник городов 


```python
df_city = df[['city']].drop_duplicates().reset_index(drop=True).reset_index() \
.rename(columns={"index": "city_id"}).astype({'city': 'string'}).fillna('NULL')
df_city.head()
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
      <th>city_id</th>
      <th>city</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Varese</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Haywards heath</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Hirschberg an der bergstraße</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Crowborough</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>Beccles</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop TABLE if exists dim_city;
            ''')


cur.execute(f'''
CREATE TABLE 
            dim_city (
                city_id INT UNSIGNED NOT null PRIMARY KEY,
                city VARCHAR(255) NOT null);
''')

df_city.to_sql('dim_city', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from dim_city;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>city_id</th>
      <th>city</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Varese</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Haywards heath</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Hirschberg an der bergstraße</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Crowborough</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>Beccles</td>
    </tr>
  </tbody>
</table>
</div>



### Справочник адресов 


```python
df_address = df[['postal_code', 'street', 'building_number']] \
.drop_duplicates().reset_index(drop=True).reset_index() \
.rename(columns={"index": "address_id"}) \
.astype({'postal_code': 'string', 'street': 'string', 'building_number': 'string'}) \
.fillna('NULL')
df_address.head()
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
      <th>address_id</th>
      <th>postal_code</th>
      <th>street</th>
      <th>building_number</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>21100</td>
      <td>Via dolomiti</td>
      <td>13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>RH16 3TQ</td>
      <td>Cobbetts mead</td>
      <td>31</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>69493</td>
      <td>Breitgasse 5a</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>TN6 1ST</td>
      <td>Ghyll road</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>NR34 7SH</td>
      <td>Pepys avenue</td>
      <td>24</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop TABLE if exists dim_address;
            ''')


cur.execute(f'''
CREATE TABLE 
            dim_address (
                address_id INT UNSIGNED NOT null PRIMARY KEY,
                postal_code VARCHAR(255) NOT null,
                street VARCHAR(255) NOT null,
                building_number VARCHAR(255) NOT null
                )
                ;
''')

df_address.to_sql('dim_address', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from dim_address;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>address_id</th>
      <th>postal_code</th>
      <th>street</th>
      <th>building_number</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>21100</td>
      <td>Via dolomiti</td>
      <td>13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>RH16 3TQ</td>
      <td>Cobbetts mead</td>
      <td>31</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>69493</td>
      <td>Breitgasse 5a</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>TN6 1ST</td>
      <td>Ghyll road</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>NR34 7SH</td>
      <td>Pepys avenue</td>
      <td>24</td>
    </tr>
  </tbody>
</table>
</div>



### Справочник клиентов 


```python
df_clients = df[['first_name', 'last_name', 'birth_date']] \
.drop_duplicates().reset_index(drop=True).reset_index() \
.rename(columns={"index": "client_id"}) \
.astype({'first_name': 'string', 'last_name': 'string', 'birth_date': 'string'}) \
.fillna('NULL')
df_clients['birth_date'] = df_clients['birth_date'].replace('NULL', '1900-01-01')
df_clients['birth_date'] = pd.to_datetime(df_clients['birth_date'], format='%Y-%m-%d', errors='coerce')  
df_clients['birth_date'] = df_clients['birth_date'].fillna('1900-01-01')
df_clients.head()
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
      <th>client_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>birth_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Danilo</td>
      <td>Ambrosini</td>
      <td>1900-01-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Deborah</td>
      <td>Thomas</td>
      <td>1900-01-01</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Markus</td>
      <td>Hönninge</td>
      <td>1900-01-01</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Sheila</td>
      <td>Spiller</td>
      <td>1900-01-01</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>Rosemary</td>
      <td>Bailey</td>
      <td>1900-01-01</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop TABLE if exists dim_clients;
            ''')


cur.execute(f'''
CREATE TABLE 
            dim_clients (
                client_id INT UNSIGNED NOT null PRIMARY KEY,
                first_name VARCHAR(255) NOT null,
                last_name VARCHAR(255) NOT null,
                birth_date DATE NOT null
                )
                ;
''')

df_clients.to_sql('dim_clients', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from dim_clients;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>client_id</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>birth_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Danilo</td>
      <td>Ambrosini</td>
      <td>1900-01-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Deborah</td>
      <td>Thomas</td>
      <td>1900-01-01</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Markus</td>
      <td>Hönninge</td>
      <td>1900-01-01</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Sheila</td>
      <td>Spiller</td>
      <td>1900-01-01</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>Rosemary</td>
      <td>Bailey</td>
      <td>1900-01-01</td>
    </tr>
  </tbody>
</table>
</div>



### Справочник языков


```python
df_language = df[['correspondence_language']].drop_duplicates().reset_index(drop=True).reset_index() \
.rename(columns={"index": "language_id"}).astype({'correspondence_language': 'string'}).fillna('NULL')
df_language.head()
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
      <th>language_id</th>
      <th>correspondence_language</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>IT</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>DE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>EN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>PL</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop TABLE if exists dim_language;
            ''')


cur.execute(f'''
CREATE TABLE 
            dim_language (
                language_id INT UNSIGNED NOT null PRIMARY KEY,
                correspondence_language VARCHAR(255) NOT null);
''')

df_language.to_sql('dim_language', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from dim_language;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>language_id</th>
      <th>correspondence_language</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>IT</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>NULL</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>DE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>EN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>PL</td>
    </tr>
  </tbody>
</table>
</div>



### Справочник полов


```python
df_gender = df[['gender']].drop_duplicates().reset_index(drop=True).reset_index() \
.rename(columns={"index": "gender_id"}).astype({'gender': 'string'}).fillna('NULL')
df_gender.head()
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
      <th>gender_id</th>
      <th>gender</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Unknown</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Female</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Male</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>NULL</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop TABLE if exists dim_gender;
            ''')


cur.execute(f'''
CREATE TABLE 
            dim_gender (
                gender_id INT UNSIGNED NOT null PRIMARY KEY,
                gender VARCHAR(25) NOT null);
''')

df_gender.to_sql('dim_gender', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from dim_gender;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>gender_id</th>
      <th>gender</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Unknown</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Female</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Male</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>NULL</td>
    </tr>
  </tbody>
</table>
</div>



### Справочник титулов 


```python
df_title = df[['title']].drop_duplicates().reset_index(drop=True).reset_index() \
.rename(columns={"index": "title_id"}).astype({'title': 'string'}).fillna('NULL')
df_title.head()
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
      <th>title_id</th>
      <th>title</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Dr.</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Mrs</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Herr</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Mr</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>NULL</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop TABLE if exists dim_title;
            ''')


cur.execute(f'''
CREATE TABLE 
            dim_title (
                title_id INT UNSIGNED NOT null PRIMARY KEY,
                title VARCHAR(25) NOT null);
''')

df_title.to_sql('dim_title', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from dim_title;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>title_id</th>
      <th>title</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Dr.</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>Mrs</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>Herr</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>Mr</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>NULL</td>
    </tr>
  </tbody>
</table>
</div>




```python
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
      <th>title</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>correspondence_language</th>
      <th>birth_date</th>
      <th>gender</th>
      <th>marital_status</th>
      <th>country</th>
      <th>postal_code</th>
      <th>region</th>
      <th>city</th>
      <th>street</th>
      <th>building_number</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Dr.</td>
      <td>Danilo</td>
      <td>Ambrosini</td>
      <td>IT</td>
      <td>1900-01-01</td>
      <td>Unknown</td>
      <td>NaN</td>
      <td>IT</td>
      <td>21100</td>
      <td>NaN</td>
      <td>Varese</td>
      <td>Via dolomiti</td>
      <td>13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Mrs</td>
      <td>Deborah</td>
      <td>Thomas</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Female</td>
      <td>NaN</td>
      <td>GB</td>
      <td>RH16 3TQ</td>
      <td>NaN</td>
      <td>Haywards heath</td>
      <td>Cobbetts mead</td>
      <td>31</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Herr</td>
      <td>Markus</td>
      <td>Hönninge</td>
      <td>DE</td>
      <td>1900-01-01</td>
      <td>Male</td>
      <td>NaN</td>
      <td>DE</td>
      <td>69493</td>
      <td>NaN</td>
      <td>Hirschberg an der bergstraße</td>
      <td>Breitgasse 5a</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Mrs</td>
      <td>Sheila</td>
      <td>Spiller</td>
      <td>EN</td>
      <td>NaN</td>
      <td>Unknown</td>
      <td>NaN</td>
      <td>GB</td>
      <td>TN6 1ST</td>
      <td>East sussex</td>
      <td>Crowborough</td>
      <td>Ghyll road</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Mrs</td>
      <td>Rosemary</td>
      <td>Bailey</td>
      <td>EN</td>
      <td>NaN</td>
      <td>Unknown</td>
      <td>NaN</td>
      <td>GB</td>
      <td>NR34 7SH</td>
      <td>NaN</td>
      <td>Beccles</td>
      <td>Pepys avenue</td>
      <td>24</td>
    </tr>
  </tbody>
</table>
</div>




```python
df = df.fillna('NULL')
df['birth_date'] = df['birth_date'].replace('NULL', '1900-01-01')
df['birth_date'] = pd.to_datetime(df_clients['birth_date'], format='%Y-%m-%d', errors='coerce')  
df['birth_date'] = df['birth_date'].fillna('1900-01-01')
df = df.astype({'title': 'string', 'first_name': 'string', 'last_name': 'string',
          'correspondence_language': 'string', 'gender': 'string', 'marital_status': 'string',
          'country': 'string', 'postal_code': 'string','region': 'string',
          'city': 'string', 'street': 'string',
          'building_number': 'string'})
```


```python
df.dtypes
```




    title                              string
    first_name                         string
    last_name                          string
    correspondence_language            string
    birth_date                 datetime64[ns]
    gender                             string
    marital_status                     string
    country                            string
    postal_code                        string
    region                             string
    city                               string
    street                             string
    building_number                    string
    dtype: object




```python
new_df = df.merge(df_clients, on = ['first_name', 'last_name', 'birth_date']) \
.merge(df_language, on = ['correspondence_language']) \
.merge(df_gender, on = ['gender']) \
.merge(df_title, on = ['title']) \
.merge(df_address, on = ['postal_code', 'street', 'building_number']) \
.merge(df_city, on = ['city']) \
.merge(df_region, on = ['region']) \
.merge(df_country, on = ['country']) 

new_df.head()
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
      <th>title</th>
      <th>first_name</th>
      <th>last_name</th>
      <th>correspondence_language</th>
      <th>birth_date</th>
      <th>gender</th>
      <th>marital_status</th>
      <th>country</th>
      <th>postal_code</th>
      <th>region</th>
      <th>...</th>
      <th>street</th>
      <th>building_number</th>
      <th>client_id</th>
      <th>language_id</th>
      <th>gender_id</th>
      <th>title_id</th>
      <th>address_id</th>
      <th>city_id</th>
      <th>region_id</th>
      <th>country_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Dr.</td>
      <td>Danilo</td>
      <td>Ambrosini</td>
      <td>IT</td>
      <td>1900-01-01</td>
      <td>Unknown</td>
      <td>NULL</td>
      <td>IT</td>
      <td>21100</td>
      <td>NULL</td>
      <td>...</td>
      <td>Via dolomiti</td>
      <td>13</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Dr.</td>
      <td>Enzo</td>
      <td>Lusini</td>
      <td>IT</td>
      <td>1900-01-01</td>
      <td>Unknown</td>
      <td>NULL</td>
      <td>IT</td>
      <td>50141</td>
      <td>NULL</td>
      <td>...</td>
      <td>Via aligi barducci</td>
      <td>44</td>
      <td>74</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>75</td>
      <td>72</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Dr.</td>
      <td>Pietro</td>
      <td>Ciprietti</td>
      <td>IT</td>
      <td>1900-01-01</td>
      <td>Unknown</td>
      <td>NULL</td>
      <td>IT</td>
      <td>64018</td>
      <td>NULL</td>
      <td>...</td>
      <td>Via panoramica</td>
      <td>23</td>
      <td>251</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>252</td>
      <td>216</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Dr.</td>
      <td>Gabriele</td>
      <td>Gardin</td>
      <td>IT</td>
      <td>1900-01-01</td>
      <td>Unknown</td>
      <td>NULL</td>
      <td>IT</td>
      <td>25040</td>
      <td>NULL</td>
      <td>...</td>
      <td>Via cesare battisti</td>
      <td>7</td>
      <td>291</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>293</td>
      <td>253</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Dr.</td>
      <td>Marco</td>
      <td>Tognoni</td>
      <td>IT</td>
      <td>1900-01-01</td>
      <td>Unknown</td>
      <td>NULL</td>
      <td>IT</td>
      <td>54033</td>
      <td>NULL</td>
      <td>...</td>
      <td>Via antonio bertoloni</td>
      <td>29</td>
      <td>389</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>393</td>
      <td>335</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 21 columns</p>
</div>



### Витрина с прочей информацией по клиентам


```python
clients_other_info = new_df[['client_id', 'title_id', 'gender_id', 'language_id']] \
.drop_duplicates()
clients_other_info.head()
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
      <th>client_id</th>
      <th>title_id</th>
      <th>gender_id</th>
      <th>language_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>74</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>251</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>291</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>389</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop TABLE if exists clients_other_info;
            ''')


cur.execute(f'''
CREATE TABLE 
            clients_other_info (
                client_id INT NOT null,
                 title_id INT NOT null,
                  gender_id INT NOT null,
                   language_id INT NOT null);
''')

clients_other_info.to_sql('clients_other_info', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from clients_other_info;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>client_id</th>
      <th>title_id</th>
      <th>gender_id</th>
      <th>language_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>74</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>251</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>291</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>389</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



### Витрина с адресной информацией по клиентам


```python
clients_address_info = new_df[['client_id', 'country_id', 'region_id', 'address_id', 'city_id']] \
.drop_duplicates()
clients_address_info.head()
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
      <th>client_id</th>
      <th>country_id</th>
      <th>region_id</th>
      <th>address_id</th>
      <th>city_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>74</td>
      <td>0</td>
      <td>0</td>
      <td>75</td>
      <td>72</td>
    </tr>
    <tr>
      <th>2</th>
      <td>251</td>
      <td>0</td>
      <td>0</td>
      <td>252</td>
      <td>216</td>
    </tr>
    <tr>
      <th>3</th>
      <td>291</td>
      <td>0</td>
      <td>0</td>
      <td>293</td>
      <td>253</td>
    </tr>
    <tr>
      <th>4</th>
      <td>389</td>
      <td>0</td>
      <td>0</td>
      <td>393</td>
      <td>335</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
            drop TABLE if exists clients_address_info;
            ''')


cur.execute(f'''
CREATE TABLE 
            clients_address_info (
                client_id INT NOT null,
                 country_id INT NOT null,
                  region_id INT NOT null,
                   address_id INT NOT null,
                   city_id INT NOT null);
''')

clients_address_info.to_sql('clients_address_info', con = engine,  if_exists='append', index=False)
 
query = f'''
select * from clients_address_info;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>client_id</th>
      <th>country_id</th>
      <th>region_id</th>
      <th>address_id</th>
      <th>city_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>74</td>
      <td>0</td>
      <td>0</td>
      <td>75</td>
      <td>72</td>
    </tr>
    <tr>
      <th>2</th>
      <td>251</td>
      <td>0</td>
      <td>0</td>
      <td>252</td>
      <td>216</td>
    </tr>
    <tr>
      <th>3</th>
      <td>291</td>
      <td>0</td>
      <td>0</td>
      <td>293</td>
      <td>253</td>
    </tr>
    <tr>
      <th>4</th>
      <td>389</td>
      <td>0</td>
      <td>0</td>
      <td>393</td>
      <td>335</td>
    </tr>
  </tbody>
</table>
</div>



### Проверка JOIN


```python
query = f'''
select *
from clients_address_info l
left join dim_address r
on l.address_id = r.address_id;
        '''
df_sql = pd.read_sql_query(query, cnx)
df_sql.head()
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
      <th>client_id</th>
      <th>country_id</th>
      <th>region_id</th>
      <th>address_id</th>
      <th>city_id</th>
      <th>address_id</th>
      <th>postal_code</th>
      <th>street</th>
      <th>building_number</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>21100</td>
      <td>Via dolomiti</td>
      <td>13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>74</td>
      <td>0</td>
      <td>0</td>
      <td>75</td>
      <td>72</td>
      <td>75</td>
      <td>50141</td>
      <td>Via aligi barducci</td>
      <td>44</td>
    </tr>
    <tr>
      <th>2</th>
      <td>251</td>
      <td>0</td>
      <td>0</td>
      <td>252</td>
      <td>216</td>
      <td>252</td>
      <td>64018</td>
      <td>Via panoramica</td>
      <td>23</td>
    </tr>
    <tr>
      <th>3</th>
      <td>291</td>
      <td>0</td>
      <td>0</td>
      <td>293</td>
      <td>253</td>
      <td>293</td>
      <td>25040</td>
      <td>Via cesare battisti</td>
      <td>7</td>
    </tr>
    <tr>
      <th>4</th>
      <td>389</td>
      <td>0</td>
      <td>0</td>
      <td>393</td>
      <td>335</td>
      <td>393</td>
      <td>54033</td>
      <td>Via antonio bertoloni</td>
      <td>29</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
