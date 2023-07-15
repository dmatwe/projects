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

### Создание таблицы и наполнение ее данными


```python
cur = conn.cursor()
cur.execute('''
            CREATE TABLE statistic(
            player_name VARCHAR(100) NOT NULL,
            player_id INT NOT NULL,
            year_game SMALLINT NOT NULL CHECK (year_game > 0),
            points DECIMAL(12,2) CHECK (points >= 0),
            PRIMARY KEY (player_name,year_game)
            );
            ''')

cur.execute('''
           INSERT INTO
            statistic(player_name, player_id, year_game, points)
            VALUES
            ('Mike',1,2018,18),
            ('Jack',2,2018,14),
            ('Jackie',3,2018,30),
            ('Jet',4,2018,30),
            ('Luke',1,2019,16),
            ('Mike',2,2019,14),
            ('Jack',3,2019,15),
            ('Jackie',4,2019,28),
            ('Jet',5,2019,25),
            ('Luke',1,2020,19),
            ('Mike',2,2020,17),
            ('Jack',3,2020,18),
            ('Jackie',4,2020,29),
            ('Jet',5,2020,27);
            ''')

cur.execute('''
           SELECT 
           player_name, player_id, year_game, points
           from statistic
            ''')

results = cur.fetchall()
pd.DataFrame(results, columns=['player_name', 'player_id', 'year_game', 'points'])
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
      <th>player_name</th>
      <th>player_id</th>
      <th>year_game</th>
      <th>points</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Mike</td>
      <td>1</td>
      <td>2018</td>
      <td>18.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Jack</td>
      <td>2</td>
      <td>2018</td>
      <td>14.00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Jackie</td>
      <td>3</td>
      <td>2018</td>
      <td>30.00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Jet</td>
      <td>4</td>
      <td>2018</td>
      <td>30.00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Luke</td>
      <td>1</td>
      <td>2019</td>
      <td>16.00</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Mike</td>
      <td>2</td>
      <td>2019</td>
      <td>14.00</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Jack</td>
      <td>3</td>
      <td>2019</td>
      <td>15.00</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Jackie</td>
      <td>4</td>
      <td>2019</td>
      <td>28.00</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Jet</td>
      <td>5</td>
      <td>2019</td>
      <td>25.00</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Luke</td>
      <td>1</td>
      <td>2020</td>
      <td>19.00</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Mike</td>
      <td>2</td>
      <td>2020</td>
      <td>17.00</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Jack</td>
      <td>3</td>
      <td>2020</td>
      <td>18.00</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Jackie</td>
      <td>4</td>
      <td>2020</td>
      <td>29.00</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Jet</td>
      <td>5</td>
      <td>2020</td>
      <td>27.00</td>
    </tr>
  </tbody>
</table>
</div>



### запрос суммы очков с группировкой и сортировкой по годам


```python
cur.execute('''
           SELECT 
           year_game,
           sum(points) as sum_points
           from statistic
           group by year_game
           order by year_game
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['year_game', 'sum_points'])
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
      <th>year_game</th>
      <th>sum_points</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018</td>
      <td>92.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2019</td>
      <td>98.00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2020</td>
      <td>110.00</td>
    </tr>
  </tbody>
</table>
</div>



### CTE Common Table Expressions 
### Используя функцию LAG вывести кол-во очков по всем игрокам за текущий код и за предыдущий


```python
cur.execute('''
           with cte_sum as (
               SELECT 
               year_game,
               sum(points) as sum_points
               from statistic
               group by year_game
               order by year_game
           )
           select 
               year_game, 
               sum_points as cur_y,
               LAG(sum_points) over (order by year_game) as lst_y
           from cte_sum
            ''')
results = cur.fetchall()
pd.DataFrame(results, columns=['year_game', 'cur_y','lst_y'])
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
      <th>year_game</th>
      <th>cur_y</th>
      <th>lst_y</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2018</td>
      <td>92.00</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2019</td>
      <td>98.00</td>
      <td>92.00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2020</td>
      <td>110.00</td>
      <td>98.00</td>
    </tr>
  </tbody>
</table>
</div>




```python
conn.rollback()
conn.close()
```
