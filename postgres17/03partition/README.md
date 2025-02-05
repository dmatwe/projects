**Установка Linux**

Яндекс облако: Ubuntu 24.04 

***Ресурсы:***

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/03partition/img/Screenshot%202025-02-05%20at%2012.07.12.png)

3 SSD / 20 GB

**Установка Postgres 17**

sudo apt update && sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y && sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - && sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt -y install postgresql-17 unzip atop htop


**Конфиг калькулятор**

https://pgconfigurator.cybertec.at/


```python
sudo su postgres 

nano /etc/postgresql/17/main/postgresql.conf

pg_ctlcluster 17 main stop && pg_ctlcluster 17 main start
```

**Insert авторской БД Аристова**

https://github.com/aeuge/postgres16book/tree/main/database

```python
sudo su postgres
cd 
wget https://storage.googleapis.com/thaibus/thai_small.tar.gz && tar -xf thai_small.tar.gz && psql < thai.sql

psql -d thai
```
**Создание таблицы tickets для партиционирования**

```python
CREATE TABLE book.tickets_p (
    id BIGINT NOT NULL,
    fkride INTEGER,
    fio TEXT,
    contact JSONB,
    fkseat INTEGER,
    PRIMARY KEY (id, fkride) -- Комбинированный ключ, если нужно
) PARTITION BY RANGE (fkride);
```
**Создание 10 партиций для tickets**

```python
DO $$
DECLARE
    v_min_fkride INTEGER;
    v_max_fkride INTEGER;
    v_range_size INTEGER;
BEGIN
    -- Получаем минимальное и максимальное значения fkride
    SELECT MIN(fkride), MAX(fkride) INTO v_min_fkride, v_max_fkride FROM book.tickets;

    -- Рассчитываем размер диапазона для каждой партиции
    v_range_size := (v_max_fkride - v_min_fkride + 1) / 10;

    -- Создаем 10 партиций
    FOR i IN 0..9 LOOP
        EXECUTE format('
            CREATE TABLE book.tickets_part_%s PARTITION OF book.tickets_p 
            FOR VALUES FROM (%s) TO (%s)', 
            i, 
            v_min_fkride + i * v_range_size, 
            v_min_fkride + (i + 1) * v_range_size
        );
    END LOOP;
END $$;
```

**INSERT Данных в tickets_p**

```python
insert into book.tickets_p select * from book.tickets;

INSERT 0 5185505
```


**Создание таблицы ride для партиционирования**

```python
CREATE TABLE book.ride_p (
    id SERIAL,
    startdate date,
    fkbus integer,
    fkschedule integer
) PARTITION BY LIST (startdate);
```

**Создание 100 партиций для ride**
```python
thai=# select min(startdate), max(startdate), count(distinct(startdate)) from book.ride;
    min     |    max     | count 
------------+------------+-------
 2000-01-01 | 2000-04-09 |   100
(1 row)
```

```python
DO $do$
DECLARE
    v_date date;
BEGIN
    FOR v_date IN SELECT distinct(startdate) FROM book.ride ORDER BY 1 ASC
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS book."ride_part_%s"', v_date);
        EXECUTE format('CREATE TABLE book."ride_part_%s" PARTITION OF book.ride_p FOR VALUES IN (''%s'')', v_date, v_date);
    END LOOP;
END;
$do$;
```

**INSERT в ride**
```python
insert into book.ride_p select * from book.ride;
INSERT 0 144000
```


**Переименовываем таблицы**


```python
alter table book.ride rename to ride_no_parts;

alter table book.ride_p rename to ride;

alter table book.tickets rename to tickets_no_parts;

alter table book.tickets_p rename to tickets;

```





**EXPLAIN ANALYZE Без партиций**

```python
EXPLAIN ANALYZE SELECT 
 t.fkride
 FROM book.tickets_no_parts t
 WHERE t.id = 5176481;
                                                           QUERY PLAN                                                            
---------------------------------------------------------------------------------------------------------------------------------
 Index Scan using tickets_pkey on tickets_no_parts t  (cost=0.43..2.45 rows=1 width=4) (actual time=0.024..0.026 rows=1 loops=1)
   Index Cond: (id = 5176481)
 Planning Time: 0.078 ms
 Execution Time: 0.041 ms
(4 rows)

```
**EXPLAIN ANALYZE 1 партиция** 
```python
EXPLAIN ANALYZE SELECT 
 t.fkride
 FROM book.tickets_part_0 t
 WHERE t.id = 5176481;



                                                                QUERY PLAN                                                                 
-------------------------------------------------------------------------------------------------------------------------------------------
 Index Only Scan using tickets_part_0_pkey on tickets_part_0 t  (cost=0.42..1.44 rows=1 width=4) (actual time=0.019..0.020 rows=1 loops=1)
   Index Cond: (id = 5176481)
   Heap Fetches: 0
 Planning Time: 0.069 ms
 Execution Time: 0.035 ms
(5 rows)
```


**EXPLAIN ANALYZE 10 партиций**

```python
EXPLAIN ANALYZE SELECT 
 t.fkride
 FROM book.tickets t
 WHERE t.id = 5176481;

         Heap Fetches: 0
   ->  Index Only Scan using tickets_part_4_pkey on tickets_part_4 t_5  (cost=0.42..1.44 rows=1 width=4) (actual time=0.020..0.020 rows=0 loops=1)
         Index Cond: (id = 5176481)
         Heap Fetches: 0
   ->  Index Only Scan using tickets_part_5_pkey on tickets_part_5 t_6  (cost=0.42..1.44 rows=1 width=4) (actual time=0.027..0.027 rows=0 loops=1)
         Index Cond: (id = 5176481)
         Heap Fetches: 0
   ->  Index Only Scan using tickets_part_6_pkey on tickets_part_6 t_7  (cost=0.42..1.44 rows=1 width=4) (actual time=0.018..0.018 rows=0 loops=1)
         Index Cond: (id = 5176481)
         Heap Fetches: 0
   ->  Index Only Scan using tickets_part_7_pkey on tickets_part_7 t_8  (cost=0.42..1.44 rows=1 width=4) (actual time=0.018..0.018 rows=0 loops=1)
         Index Cond: (id = 5176481)
         Heap Fetches: 0
   ->  Index Only Scan using tickets_part_8_pkey on tickets_part_8 t_9  (cost=0.42..1.44 rows=1 width=4) (actual time=0.022..0.022 rows=0 loops=1)
         Index Cond: (id = 5176481)
         Heap Fetches: 0
   ->  Index Only Scan using tickets_part_9_pkey on tickets_part_9 t_10  (cost=0.42..1.44 rows=1 width=4) (actual time=0.020..0.020 rows=0 loops=1)
         Index Cond: (id = 5176481)
         Heap Fetches: 0
 Planning Time: 0.763 ms
 Execution Time: 0.222 ms
```

**EXPLAIN ANALYZE 100 партиций через join**

```python
EXPLAIN ANALYZE SELECT 
 t.fkride,
 r.fkbus,
 r.startdate
 FROM book.tickets t
 LEFT JOIN book.ride r ON t.fkride = r.id
 WHERE t.id = 5176481;


Nested Loop Left Join  (cost=0.70..1951.95 rows=7200 width=12) (actual time=0.182..1.423 rows=1 loops=1)
   ->  Append  (cost=0.42..16.95 rows=10 width=4) (actual time=0.084..0.271 rows=1 loops=1)
         ->  Index Only Scan using tickets_part_0_pkey on tickets_part_0 t_1  (cost=0.42..1.69 rows=1 width=4) (actual time=0.083..0.084 rows=1 loops=1)
               Index Cond: (id = 5176481)
               Heap Fetches: 0
.....
.....
         ->  Index Only Scan using tickets_part_8_pkey on tickets_part_8 t_9  (cost=0.42..1.69 rows=1 width=4) (actual time=0.021..0.021 rows=0 loops=1)
               Index Cond: (id = 5176481)
               Heap Fetches: 0
         ->  Index Only Scan using tickets_part_9_pkey on tickets_part_9 t_10  (cost=0.42..1.69 rows=1 width=4) (actual time=0.021..0.021 rows=0 loops=1)
               Index Cond: (id = 5176481)
               Heap Fetches: 0
   ->  Append  (cost=0.28..192.50 rows=100 width=12) (actual time=0.088..1.141 rows=1 loops=1)
         ->  Index Scan using "ride_part_2000-01-01_id_startdate_key" on "ride_part_2000-01-01" r_1  (cost=0.28..1.92 rows=1 width=12) (actual time=0.010..0.010 rows=0 loops=1)

......
......
         ->  Index Scan using "ride_part_2000-04-08_id_startdate_key" on "ride_part_2000-04-08" r_99  (cost=0.28..1.92 rows=1 width=12) (actual time=0.008..0.008 rows=0 loops=1)
               Index Cond: (id = t.fkride)
         ->  Index Scan using "ride_part_2000-04-09_id_startdate_key" on "ride_part_2000-04-09" r_100  (cost=0.28..1.92 rows=1 width=12) (actual time=0.010..0.011 rows=0 loops=1)
               Index Cond: (id = t.fkride)
 Planning Time: 9.095 ms
 Execution Time: 2.527 ms
(235 rows)
```



**EXPLAIN ANALYZE Join без партиций**

```python
EXPLAIN ANALYZE SELECT 
 t.fkride,
 r.fkbus,
 r.startdate
 FROM book.tickets_no_parts t
 LEFT JOIN book.ride_no_parts r ON t.fkride = r.id
 WHERE t.id = 5176481;




                                                              QUERY PLAN                                                               
---------------------------------------------------------------------------------------------------------------------------------------
 Nested Loop Left Join  (cost=0.85..5.89 rows=1 width=12) (actual time=2.540..2.542 rows=1 loops=1)
   ->  Index Scan using tickets_pkey on tickets_no_parts t  (cost=0.43..2.95 rows=1 width=4) (actual time=0.020..0.022 rows=1 loops=1)
         Index Cond: (id = 5176481)
   ->  Index Scan using ride_pkey on ride_no_parts r  (cost=0.42..2.94 rows=1 width=12) (actual time=2.515..2.515 rows=1 loops=1)
         Index Cond: (id = t.fkride)
 Planning Time: 5.590 ms
 Execution Time: 2.623 ms
(7 rows)
```

