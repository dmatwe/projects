**1. Установка Linux**

Яндекс облако: Ubuntu 24.04 

***Ресурсы:***

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2014.13.02.png)

**2. Установка Postgres 17**

sudo apt update && sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y && sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - && sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt -y install postgresql-17 unzip atop htop

***Пояснения:***


***sudo apt update*** — обновляет список доступных пакетов и их версий, но не устанавливает их. Это позволяет системе узнать, какие пакеты доступны для установки или обновления.

***sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y*** — обновляет все установленные пакеты до последних версий. Параметр -y автоматически подтверждает все запросы на установку, что делает процесс неинтерактивным. Переменная окружения DEBIAN_FRONTEND=noninteractive позволяет избежать появления интерактивных окон, что полезно в скриптах.

***sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt (lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'*** — добавляет репозиторий PostgreSQL в список источников пакетов. Здесь используется команда lsb_release -cs, которая возвращает кодовое имя текущего дистрибутива (например, focal для Ubuntu 20.04). Это добавляет новую строку в файл /etc/apt/sources.list.d/pgdg.list, что позволяет системе получать пакеты PostgreSQL из этого репозитория.

***wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - ***— загружает ключи для аутентификации пакетов из репозитория PostgreSQL и добавляет их в систему. Это необходимо для того, чтобы система могла доверять пакетам, загружаемым из этого репозитория.

***sudo apt-get update*** — снова обновляет список пакетов, теперь уже с учетом нового репозитория PostgreSQL.

***sudo DEBIAN_FRONTEND=noninteractive apt -y install postgresql-17 unzip atop htop*** — устанавливает PostgreSQL версии 17 и несколько утилит: unzip (для распаковки zip-архивов), atop (для мониторинга системы) и htop (для интерактивного мониторинга процессов).

**Insert авторской БД Аристова**

```python
sudo su postgres
cd 
wget https://storage.googleapis.com/thaibus/thai_small.tar.gz && tar -xf thai_small.tar.gz && psql < thai.sql

psql -d thai
```


**TEST производительности**

```json
postgres@compute-vm-2-2-20-ssd-1738228408146:/home/vmadmin$ pgbench -i postgres

dropping old tables...
creating tables...
generating data (client-side)...
100000 of 100000 tuples (100%) of pgbench_accounts done (elapsed 0.07 s, remaini                                                                                vacuuming...
creating primary keys...
done in 1.08 s (drop tables 0.01 s, create tables 0.01 s, client-side generate 0.91 s, vacuum 0.06 s, primary keys 0.10 s).

postgres@compute-vm-2-2-20-ssd-1738228408146:/home/vmadmin$ pgbench -P 1 -T 10 postgres

pgbench (17.2 (Ubuntu 17.2-1.pgdg24.04+1))
starting vacuum...end.
progress: 1.0 s, 369.0 tps, lat 2.675 ms stddev 2.499, 0 failed
progress: 2.0 s, 265.0 tps, lat 3.792 ms stddev 2.981, 0 failed
progress: 3.0 s, 256.0 tps, lat 3.838 ms stddev 3.993, 0 failed
^[[Aprogress: 4.0 s, 171.0 tps, lat 5.826 ms stddev 6.481, 0 failed
progress: 5.0 s, 171.0 tps, lat 5.902 ms stddev 6.043, 0 failed
^[[Bprogress: 6.0 s, 298.0 tps, lat 3.386 ms stddev 3.032, 0 failed
progress: 7.0 s, 190.0 tps, lat 5.223 ms stddev 4.514, 0 failed
progress: 8.0 s, 213.0 tps, lat 4.731 ms stddev 3.781, 0 failed
progress: 9.0 s, 231.0 tps, lat 4.314 ms stddev 4.401, 0 failed
progress: 10.0 s, 333.0 tps, lat 3.016 ms stddev 2.858, 0 failed
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: simple
number of clients: 1
number of threads: 1
maximum number of tries: 1
duration: 10 s
number of transactions actually processed: 2498
number of failed transactions: 0 (0.000%)
latency average = 4.002 ms
latency stddev = 4.087 ms
initial connection time = 3.439 ms
tps = 249.872463 (without initial connection time)
```

```json
postgres@compute-vm-2-2-20-ssd-1738228408146:/home/vmadmin$ pgbench -P 1 -c 10 -T 10 postgres

pgbench (17.2 (Ubuntu 17.2-1.pgdg24.04+1))
starting vacuum...end.
progress: 1.0 s, 420.0 tps, lat 22.413 ms stddev 19.388, 0 failed
progress: 2.0 s, 346.0 tps, lat 28.649 ms stddev 25.626, 0 failed
progress: 3.0 s, 349.0 tps, lat 29.268 ms stddev 27.448, 0 failed
progress: 4.0 s, 305.0 tps, lat 32.969 ms stddev 24.916, 0 failed
progress: 5.0 s, 183.0 tps, lat 49.397 ms stddev 65.586, 0 failed
progress: 6.0 s, 125.0 tps, lat 79.463 ms stddev 69.576, 0 failed
progress: 7.0 s, 124.0 tps, lat 84.752 ms stddev 82.606, 0 failed
progress: 8.0 s, 168.0 tps, lat 61.004 ms stddev 68.041, 0 failed
progress: 9.0 s, 168.0 tps, lat 58.770 ms stddev 49.726, 0 failed
progress: 10.0 s, 128.0 tps, lat 77.273 ms stddev 61.049, 0 failed
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: simple
number of clients: 10
number of threads: 1
maximum number of tries: 1
duration: 10 s
number of transactions actually processed: 2326
number of failed transactions: 0 (0.000%)
latency average = 43.063 ms
latency stddev = 49.961 ms
initial connection time = 24.945 ms
tps = 231.331448 (without initial connection time)
```

```json
postgres@compute-vm-2-2-20-ssd-1738228408146:/home/vmadmin$ pgbench -P 1 -c 10 -j 4 -T 10 postgres

pgbench (17.2 (Ubuntu 17.2-1.pgdg24.04+1))
starting vacuum...end.
progress: 1.0 s, 274.0 tps, lat 34.788 ms stddev 28.657, 0 failed
progress: 2.0 s, 285.0 tps, lat 33.619 ms stddev 29.660, 0 failed
progress: 3.0 s, 186.0 tps, lat 55.138 ms stddev 44.187, 0 failed
progress: 4.0 s, 125.0 tps, lat 79.418 ms stddev 74.529, 0 failed
progress: 5.0 s, 105.0 tps, lat 95.105 ms stddev 67.487, 0 failed
progress: 6.0 s, 130.0 tps, lat 79.140 ms stddev 90.648, 0 failed
progress: 7.0 s, 176.0 tps, lat 56.055 ms stddev 51.439, 0 failed
progress: 8.0 s, 134.0 tps, lat 74.027 ms stddev 57.862, 0 failed
progress: 9.0 s, 134.0 tps, lat 74.324 ms stddev 81.388, 0 failed
progress: 10.0 s, 293.0 tps, lat 34.880 ms stddev 33.320, 0 failed
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: simple
number of clients: 10
number of threads: 4
maximum number of tries: 1
duration: 10 s
number of transactions actually processed: 1852
number of failed transactions: 0 (0.000%)
latency average = 54.008 ms
latency stddev = 56.875 ms
initial connection time = 17.714 ms
tps = 184.899520 (without initial connection time)
```


```json
thai=# explain SELECT count(id) FROM book.tickets;
                                         QUERY PLAN                                         
--------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=86982.32..86982.33 rows=1 width=8)
   ->  Gather  (cost=86982.10..86982.31 rows=2 width=8)
         Workers Planned: 2
         ->  Partial Aggregate  (cost=85982.10..85982.11 rows=1 width=8)
               ->  Parallel Seq Scan on tickets  (cost=0.00..80580.88 rows=2160488 width=8)
(5 rows)

thai=# explain (analyze, buffers) SELECT count(id) FROM book.tickets;
                                                                  QUERY PLAN                                                                  
----------------------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=86982.32..86982.33 rows=1 width=8) (actual time=683.491..684.737 rows=1 loops=1)
   Buffers: shared hit=1 read=58975
   ->  Gather  (cost=86982.10..86982.31 rows=2 width=8) (actual time=681.299..684.729 rows=3 loops=1)
         Workers Planned: 2
         Workers Launched: 2
         Buffers: shared hit=1 read=58975
         ->  Partial Aggregate  (cost=85982.10..85982.11 rows=1 width=8) (actual time=674.848..674.850 rows=1 loops=3)
               Buffers: shared hit=1 read=58975
               ->  Parallel Seq Scan on tickets  (cost=0.00..80580.88 rows=2160488 width=8) (actual time=0.062..356.149 rows=1728502 loops=3)
                     Buffers: shared hit=1 read=58975
 Planning Time: 0.068 ms
 Execution Time: 684.773 ms
(12 rows)
```json

**Настройка оптимальной производительности**

**Тюнинг Линукса**

***vm.swappiness*** 

изменяем параметр swappiness в системе Linux. Этот параметр управляет тем, как система использует своп (swap) — пространство на диске, которое используется в качестве расширения оперативной памяти (RAM). Принимает значения от 0 до 100.
Система будет стараться держать данные в оперативной памяти как можно дольше, прежде чем начать использовать своп.

```json
-- swapiness 60-> 1..10

cat /proc/sys/vm/swappiness

sysctl vm.swappiness

-- онлайн изменение

sudo sysctl vm.swappiness=1

-- для сохранения изменений

sudo nano /etc/sysctl.conf

vm.swappiness=1
```

***transparent_hugepage***

Прозрачные большие страницы — это механизм управления памятью, который позволяет объединять несколько обычных страниц (обычно размером 4 КБ) в одну большую страницу (обычно 2 МБ или 1 ГБ). Это может снизить накладные расходы на управление памятью и улучшить производительность при работе с большими объемами данных.
THP обеспечивает автоматическую агрегацию страниц в большие страницы, что может улучшить производительность приложений, работающих с большими объемами памяти.

Отключаете использование прозрачных больших страниц на уровне ядра. Это означает, что система не будет автоматически объединять страницы в большие страницы для процессов.

отключает функцию прозрачных больших страниц (Transparent Huge Pages, THP) в Linux.
```json
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled  
```

Улучшение производительности для некоторых приложений: Отключение этой функции может улучшить производительность. Например, некоторые базы данных и высоконагруженные серверные приложения могут лучше работать с обычными страницами памяти.

**Тюнинг Postgres**

***Конфиг калькулятор***

https://pgconfigurator.cybertec.at/

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2015.14.41.png)

```json
nano /etc/postgresql/17/main/postgresql.conf

pg_ctlcluster 17 main stop && pg_ctlcluster 17 main start
```

***Инициализация базы данных***
```json
pgbench -i postgres
```
Описание команды:

**pgbench** — это инструмент для нагрузочного тестирования PostgreSQL.

**-i** — ключ для инициализации базы данных.

**postgres** — имя базы данных, которую нужно инициализировать.

***Что делает эта команда:***

Инициализация: Создает необходимые таблицы и данные для тестирования. При этом заполняется стандартная схема и генерируются тестовые данные, которые будут использоваться в дальнейших тестах.
Зачем это нужно: Перед началом нагрузочного тестирования необходимо подготовить базу данных, чтобы у нее была соответствующая структура и начальные данные. Это позволяет получить корректные и воспроизводимые результаты тестов.


***Мониторинг производительности с выводом состояния***

```json
pgbench -P 1 -T 10 postgres
```

Описание команды:

***-P 1*** — выводит информацию о производительности каждые 1 секунду.

***-T 10***— продолжает выполнение теста в течение 10 секунд.

***postgres***- — имя базы данных, к которой будет производиться подключение.

***Что делает эта команда:***

***Мониторинг:*** Позволяет получить представление о производительности базы данных в реальном времени в течение 10 секунд, не создавая при этом нагрузки.

***Зачем это нужно:*** Полезно для оценки текущего состояния производительности базы данных, без выполнения фактических запросов. Это может помочь выявить проблемы или узкие места в производительности.


![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2015.48.23.png)

***Запуск теста с 10 клиентами***


```json
pgbench -P 1 -c 10 -T 10 postgres
```

***Описание команды:***

-c 10 — указывает, что тест будет выполняться с 10 параллельными клиентами.

-P 1 — выводит информацию о производительности каждые 1 секунду.

-T 10 — тест будет выполняться в течение 10 секунд.

postgres — имя базы данных.

***Что делает эта команда:***

***Параллельное выполнение:*** Симулирует 10 клиентов, одновременно выполняющих запросы к базе данных.

***Зачем это нужно:*** Позволяет оценить, как база данных справляется с параллельными запросами. Это важно для понимания производительности базы данных в условиях многопользовательской нагрузки.

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2015.58.00.png)


***Запуск теста с 4 потоками по числу ядер***

```json
pgbench -P 1 -c 10 -j 4 -T 10 postgres
```


***Описание команды:***

-c 10 — тест будет выполняться с 10 параллельными клиентами.

-j 4 — указывает, что тест будет выполняться с 4 потоками (параллельные потоки для обработки клиентов).

-P 1 — выводит информацию о производительности каждые 1 секунду.

-T 10 — тест будет выполняться в течение 10 секунд.

postgres — имя базы данных.

Что делает эта команда:

***Эффективное использование ресурсов:*** Использует 4 потока для обработки 10 клиентов, что позволяет более эффективно использовать многопоточные возможности системы, особенно если у вас многоядерный процессор.

***Зачем это нужно:*** Это позволяет получить более точные результаты нагрузки, так как потоковая обработка может улучшить производительность при высоких нагрузках. Это также важно для тестирования производительности в условиях, приближенных к реальным.


![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2015.58.27.png)



**Select count* авторской БД Аристова**

```json

psql -d thai

\timing

SELECT count(*) FROM book.tickets;

SELECT count(1) FROM book.tickets;
```

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2016.19.00.png)


***Зачем устанавливать random_page_cost в 1?***

***random_page_cost:*** Это стоимость (в "стоимости выполнения") чтения страницы данных, когда доступ к данным осуществляется случайным образом, например, при выполнении запросов с использованием индексов. По умолчанию значение этого параметра составляет 4.0, что предполагает, что доступ к случайным страницам данных в 4 раза дороже, чем доступ к последовательным страницам.

В процессе тестирования или отладки вы можете временно установить random_page_cost в 1, чтобы увидеть, как это влияет на выбор планов выполнения. Это может помочь вам понять, как различные параметры конфигурации влияют на производительность вашей базы данных.

Если ваша система в основном использует индексы, и вы хотите, чтобы PostgreSQL более активно использовал их, изменение этого параметра может помочь снизить общий "стоимость" случайного доступа, что может привести к более частому использованию индексов в планах выполнения.

```json
set random_page_cost = 1;
```

***vacuum***

Делать после загрузки большого объема данных 

```json
vacuum analyze book.tickets;
```

Команда выполняет следующие действия:

Очищает таблицу tickets от "мертвых" строк, освобождая пространство и поддерживая производительность базы данных.
Собирает статистику о содержимом таблицы tickets, что позволяет оптимизировать выполнение запросов к этой таблице в будущем.

```json
explain SELECT count(id) FROM book.tickets;

explain (analyze, buffers) SELECT count(id) FROM book.tickets;
```

***EXPLAIN***: Команда EXPLAIN позволяет увидеть план выполнения SQL-запроса. Она не выполняет запрос, а показывает, как PostgreSQL планирует его выполнить.

***ANALYZE***: Выполняет запрос и показывает фактическое время выполнения и количество строк, которые были обработаны. Это позволяет увидеть реальную производительность запроса.

***BUFFERS***: Показывает информацию о том, сколько страниц данных было прочитано из кэша и сколько страниц было загружено из диска. Это помогает понять, насколько эффективно использовалась память и кэш.

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2018.05.45.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2018.06.06.png)


***Как ускорить count***

Разница 0.01%

```json
thai=# SELECT count(*) FROM book.tickets;
  count  
---------
 5185505
(1 row)

Time: 417.973 ms
thai=# SELECT reltuples::BIGINT AS estimate

FROM pg_class

WHERE oid = 'book.tickets'::regclass;
 estimate 
----------
  5185492
(1 row)

Time: 0.370 ms
```

**Настройка интсанса на максимальную производительность не обращая внимания на ACID**

1. Отключение синхронного коммита

psql -c "ALTER SYSTEM SET synchronous_commit = off;"

2. Увеличение размера буфера работы

psql -c "ALTER SYSTEM SET work_mem = '128MB';"  -- или больше, в зависимости от доступной памяти

3. Увеличение размера буфера для операций сортировки и хеширования

psql -c "ALTER SYSTEM SET maintenance_work_mem = '1GB';"  -- или больше, в зависимости от доступной памяти

4. Отключение WAL (Write-Ahead Logging)

psql -c "ALTER SYSTEM SET wal_level = minimal;"

5. Увеличение размера WAL буфера

psql -c "ALTER SYSTEM SET wal_buffers = '16MB';"

6. Отключение логирования неудачных транзакций

psql -c "ALTER SYSTEM SET log_statement = 'none';"

7. Установка параметров для параллельной обработки
sql
psql -c "ALTER SYSTEM SET max_parallel_workers_per_gather = 4;"

***Результаты:***

```json
postgres@compute-vm-2-2-20-ssd-1738166070528:~$ pgbench -P 1 -T 10 postgres

pgbench (17.2 (Ubuntu 17.2-1.pgdg24.04+1))
starting vacuum...end.
progress: 1.0 s, 1911.0 tps, lat 0.521 ms stddev 0.042, 0 failed
progress: 2.0 s, 2048.0 tps, lat 0.488 ms stddev 0.025, 0 failed
progress: 3.0 s, 1952.9 tps, lat 0.512 ms stddev 0.026, 0 failed
progress: 4.0 s, 2010.1 tps, lat 0.497 ms stddev 0.026, 0 failed
progress: 5.0 s, 2000.9 tps, lat 0.499 ms stddev 0.025, 0 failed
progress: 6.0 s, 1974.1 tps, lat 0.506 ms stddev 0.025, 0 failed
progress: 7.0 s, 2008.9 tps, lat 0.497 ms stddev 0.027, 0 failed
progress: 8.0 s, 1934.1 tps, lat 0.517 ms stddev 0.025, 0 failed
progress: 9.0 s, 1942.9 tps, lat 0.514 ms stddev 0.025, 0 failed
progress: 10.0 s, 1962.1 tps, lat 0.509 ms stddev 0.024, 0 failed
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: simple
number of clients: 1
number of threads: 1
maximum number of tries: 1
duration: 10 s
number of transactions actually processed: 19746
number of failed transactions: 0 (0.000%)
latency average = 0.506 ms
latency stddev = 0.029 ms
initial connection time = 3.732 ms
tps = 1975.247684 (without initial connection time)
```

```json
postgres@compute-vm-2-2-20-ssd-1738166070528:~$ pgbench -P 1 -c 10 -T 10 postgres

pgbench (17.2 (Ubuntu 17.2-1.pgdg24.04+1))
starting vacuum...end.
progress: 1.0 s, 2681.8 tps, lat 3.619 ms stddev 2.336, 0 failed
progress: 2.0 s, 2723.8 tps, lat 3.666 ms stddev 2.246, 0 failed
progress: 3.0 s, 2676.2 tps, lat 3.733 ms stddev 2.318, 0 failed
progress: 4.0 s, 2706.0 tps, lat 3.688 ms stddev 2.332, 0 failed
progress: 5.0 s, 2655.1 tps, lat 3.764 ms stddev 2.372, 0 failed
progress: 6.0 s, 2665.0 tps, lat 3.748 ms stddev 2.280, 0 failed
progress: 7.0 s, 2703.7 tps, lat 3.694 ms stddev 2.313, 0 failed
progress: 8.0 s, 2726.1 tps, lat 3.663 ms stddev 2.220, 0 failed
progress: 9.0 s, 2707.3 tps, lat 3.690 ms stddev 2.236, 0 failed
progress: 10.0 s, 2666.9 tps, lat 3.743 ms stddev 2.285, 0 failed
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: simple
number of clients: 10
number of threads: 1
maximum number of tries: 1
duration: 10 s
number of transactions actually processed: 26921
number of failed transactions: 0 (0.000%)
latency average = 3.703 ms
latency stddev = 2.300 ms
initial connection time = 25.722 ms
tps = 2694.559324 (without initial connection time)
```json

```json
postgres@compute-vm-2-2-20-ssd-1738166070528:~$ pgbench -P 1 -c 10 -j 4 -T 10 postgres

pgbench (17.2 (Ubuntu 17.2-1.pgdg24.04+1))
starting vacuum...end.
progress: 1.0 s, 2665.8 tps, lat 3.666 ms stddev 2.345, 0 failed
progress: 2.0 s, 2838.0 tps, lat 3.525 ms stddev 2.185, 0 failed
progress: 3.0 s, 2830.1 tps, lat 3.531 ms stddev 2.262, 0 failed
progress: 4.0 s, 2851.9 tps, lat 3.505 ms stddev 2.245, 0 failed
progress: 5.0 s, 2894.6 tps, lat 3.457 ms stddev 2.259, 0 failed
progress: 6.0 s, 2828.2 tps, lat 3.534 ms stddev 2.240, 0 failed
progress: 7.0 s, 2810.1 tps, lat 3.557 ms stddev 2.146, 0 failed
progress: 8.0 s, 2864.1 tps, lat 3.490 ms stddev 2.208, 0 failed
progress: 9.0 s, 2890.8 tps, lat 3.458 ms stddev 2.151, 0 failed
progress: 10.0 s, 2923.0 tps, lat 3.414 ms stddev 2.216, 0 failed
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 1
query mode: simple
number of clients: 10
number of threads: 4
maximum number of tries: 1
duration: 10 s
number of transactions actually processed: 28410
number of failed transactions: 0 (0.000%)
latency average = 3.515 ms
latency stddev = 2.232 ms
initial connection time = 19.471 ms
tps = 2841.712417 (without initial connection time)
```






```json
thai=# explain SELECT count(id) FROM book.tickets;

                                                      QUERY PLAN                                                      
----------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=60908.30..60908.31 rows=1 width=8)
   ->  Gather  (cost=60907.88..60908.29 rows=4 width=8)
         Workers Planned: 4
         ->  Partial Aggregate  (cost=59907.88..59907.89 rows=1 width=8)
               ->  Parallel Index Only Scan using tickets_pkey on tickets  (cost=0.43..56667.02 rows=1296344 width=8)
(5 rows)
```

```json
thai=# explain (analyze, buffers) SELECT count(id) FROM book.tickets;


                                                                               QUERY PLAN                                                                               
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Finalize Aggregate  (cost=60908.30..60908.31 rows=1 width=8) (actual time=738.616..743.149 rows=1 loops=1)
   Buffers: shared hit=14175
   ->  Gather  (cost=60907.88..60908.29 rows=4 width=8) (actual time=738.438..743.139 rows=2 loops=1)
         Workers Planned: 4
         Workers Launched: 1
         Buffers: shared hit=14175
         ->  Partial Aggregate  (cost=59907.88..59907.89 rows=1 width=8) (actual time=735.717..735.718 rows=1 loops=2)
               Buffers: shared hit=14175
               ->  Parallel Index Only Scan using tickets_pkey on tickets  (cost=0.43..56667.02 rows=1296344 width=8) (actual time=0.050..475.518 rows=2592752 loops=2)
                     Heap Fetches: 0
                     Buffers: shared hit=14175
 Planning:
   Buffers: shared hit=61
 Planning Time: 0.333 ms
 Execution Time: 743.216 ms
(15 rows)
```
