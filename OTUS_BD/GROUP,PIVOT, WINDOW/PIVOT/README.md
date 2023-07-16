### Импорт библиотек для работы с бд и дф


```python
import psycopg2
import pandas as pd
import sqlalchemy
```

### Кредиты для коннекта к бд


```python
conn = psycopg2.connect(host="127.0.0.1", port="5433", database="online_store", 
                            user="admin", password="root")
```

### Выдаёт «повёрнутую таблицу», содержащую имена строк плюс N столбцов значений


```python
cur = conn.cursor()
cur.execute('''
            CREATE EXTENSION tablefunc;
            ''')

cur.execute('''
           CREATE TABLE sales(year int, month int, qty int);
            ''')

cur.execute('''
           INSERT INTO sales VALUES
           (2007, 1, 1000),
           (2007, 2, 1500),
           (2007, 7, 500), 
           (2007, 11, 1500),
           (2007, 12, 2000),
           (2008, 1, 1000),
           (2009, 5, 2500),
           (2009, 9, 800);

            ''')

cur.execute('''
           SELECT * FROM sales;
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['year', 'month', 'qty'])
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
      <th>year</th>
      <th>month</th>
      <th>qty</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2007</td>
      <td>1</td>
      <td>1000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2007</td>
      <td>2</td>
      <td>1500</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2007</td>
      <td>7</td>
      <td>500</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2007</td>
      <td>11</td>
      <td>1500</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2007</td>
      <td>12</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2008</td>
      <td>1</td>
      <td>1000</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2009</td>
      <td>5</td>
      <td>2500</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2009</td>
      <td>9</td>
      <td>800</td>
    </tr>
  </tbody>
</table>
</div>




```python
cur.execute('''
           SELECT * FROM crosstab(
          -- два запроса особой структуры :
          -- необходимо использовать "ORDER BY 1" 
          $$ SELECT year, month, qty FROM sales ORDER BY 1 $$, 
          
          -- Второй параметр может быть любым запросом, 
          -- который возвращает по одной строке для каждого атрибута, 
          -- соответствующего порядку определения столбца в конце. 
          $$ SELECT m FROM generate_series(1,12) m $$ 
        ) AS ( -- квотирование в долларах, чтобы упростить цитирование. 
          year int, "Jan" int, "Feb" int, "Mar" int, "Apr" int, 
          "May" int, "Jun" int, "Jul" int, "Aug" int, "Sep" int, 
          "Oct" int, "Nov" int, "Dec" int
        );
            ''')
```


```python

results = cur.fetchall()
pd.DataFrame(results, columns=['year', 'Jan', 'Feb',
                              'Mar', 'Apr', 'May',
                               'Jun', 'Jul', 'Aug',
                               'Sep', 'Oct', 'Nov',
                               'Dec'
                              ])
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
      <th>year</th>
      <th>Jan</th>
      <th>Feb</th>
      <th>Mar</th>
      <th>Apr</th>
      <th>May</th>
      <th>Jun</th>
      <th>Jul</th>
      <th>Aug</th>
      <th>Sep</th>
      <th>Oct</th>
      <th>Nov</th>
      <th>Dec</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2007</td>
      <td>1000.0</td>
      <td>1500.0</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>500.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>1500.0</td>
      <td>2000.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2008</td>
      <td>1000.0</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2009</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>2500.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>800.0</td>
      <td>None</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



### FILTER PIVOT


```python
cur = conn.cursor()
cur.execute('''
            CREATE TEMP TABLE t4 (
            EFF_DATE date,
            VERSION_ID character,
            amnt integer
            )
            ''')

cur.execute('''
            INSERT INTO t4 VALUES  
            ('2012-01-01', 1, 10),
            ('2012-01-01', 0, 20),
            ('2012-02-01', 1, 30),
            ('2012-02-01', 0, 40)
            ''')

cur.execute('''
            SELECT 
            VERSION_ID,
            sum(amnt) FILTER (WHERE EFF_DATE = '2012-01-01') as jun,
            sum(amnt) FILTER (WHERE EFF_DATE = '2012-02-01') as feb
            from t4
            group by VERSION_ID
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['VERSION_ID', 'jun', 'feb'])
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
      <th>VERSION_ID</th>
      <th>jun</th>
      <th>feb</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>20</td>
      <td>40</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>10</td>
      <td>30</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.rollback()
conn.close()
```
