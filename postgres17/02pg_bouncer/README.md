**Установка Linux**

Яндекс облако: Ubuntu 24.04 

***Ресурсы:***

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2014.13.02.png)

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

**Скрипт для тестирования нагрузки**
Скрипт создает SQL-файл, который будет выполнять выборку случайной записи из таблицы book.tickets, используя случайное значение для id.

```python
cat > ~/workload.sql << EOL

\set r random(1, 5000000)

SELECT id, fkRide, fio, contact, fkSeat FROM book.tickets WHERE id = :r;

EOL
```


Команда запускает нагрузочное тестирование базы данных PostgreSQL, используя 8 клиентских соединений и 4 потока, выполняя SQL-запросы из файла /workload.sql в течение 10 секунд. Тест выполняется от имени пользователя postgres и направлен на базу данных thai.

```python
/usr/lib/postgresql/17/bin/pgbench -c 8 -j 4 -T 10 -f ~/workload.sql -n -U postgres thai
```

**Результат**

```python
pgbench (17.2 (Ubuntu 17.2-1.pgdg24.04+1))
transaction type: /var/lib/postgresql/workload.sql
scaling factor: 1
query mode: simple
number of clients: 8
number of threads: 4
maximum number of tries: 1
duration: 10 s
number of transactions actually processed: 201392
number of failed transactions: 0 (0.000%)
latency average = 0.397 ms
initial connection time = 15.553 ms
tps = 20169.280465 (without initial connection time)
```

**Установка PG BOUNCER**

```python
exit

sudo DEBIAN_FRONTEND=noninteractive apt install -y pgbouncer

sudo systemctl status pgbouncer
```

**Настройка PG BOUNCER**

```python
cat > temp.cfg << EOF
[databases]
thai = host=127.0.0.1 port=5432 dbname=thai
[pgbouncer]
logfile = /var/log/postgresql/pgbouncer.log
pidfile = /var/run/postgresql/pgbouncer.pid
listen_addr = *
listen_port = 6432
#auth_type = md5
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt
admin_users = admindb
max_client_conn = 2000
EOF

cat temp.cfg | sudo tee -a /etc/pgbouncer/pgbouncer.ini
```


**Создание пользователей**

```python
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'admin123#';";
sudo -u postgres psql -c "create user postgres2 with password 'admin123#';";
sudo -u postgres psql -c "create user admindb with password 'admin123#';";
sudo -u postgres psql -c "create user admindb2 with password 'md5a1edc6f635a68ce9926870fe752e8f2b';";

sudo -u postgres psql -c "select usename,passwd from pg_shadow;"
```



**Подготовка файла с пользователями для pgbouncer**

```python
psql -Atc "SELECT concat('\"', usename, '\" \"', passwd, '\"') FROM pg_shadow ORDER BY 1" > /etc/pgbouncer/userlist.txt

cat /etc/pgbouncer/userlist.txt 
"adb" "SCRAM-SHA-256$4096:YnTY9qulDsylCCwOTkc1Gw==$xo/QufctB6s9ii4JZGN97BVQfP44TZETjBYrFBf3UfY=:Me5GqN7IbY0clwYd9Ar9sGAZpX9z6xac8i1kip9kT00="
"app" "SCRAM-SHA-256$4096:90OXwg1yBMiB4rwNJAkItw==$ArHlYLwiM+O5c69Hwv/It28TMQMVRUM57DVTfLc8Dm4=:cyxHdNH9G/kIe2pJ9phbzZHpCeYLBSTBup+f7klLCcA="
"postgres" "SCRAM-SHA-256$4096:wUtAnRPLU093IF8y0tN78Q==$h4m4La/vtS592W/TEcclZkQZ4L7bUz9ObCsijMRGGD0=:83GlZu+i1qP69V3wCLwfYNXLSeMM9JvhSj9rLjjKjF4="
```

**Создание файла .pgpass для подключения без запроса пароля**

```python
echo "localhost:5432:thai:postgres:123" > ~/.pgpass
echo "localhost:6432:thai:postgres:123" >> ~/.pgpass
chmod 0600 ~/.pgpass

psql -h localhost -d thai
```


**Тестирование нагрузки pgbouncer**

```python
sudo systemctl start pgbouncer

sudo systemctl status pgbouncer

```

```python
pgbench -c 8 -j 4 -T 10 -f ~/workload.sql -U postgres -h localhost -p 6432 thai -n | grep -E 'clients|tps'


number of clients: 8
tps = 9486.919903 (without initial connection time)
```


```python
for i in 100 200 300 400 500 600 700 800 900 1000; do
pgbench -c $i -j 4 -T 10 -f ~/workload.sql -U postgres -h localhost -p 6432 thai -n | grep -E 'clients|tps'
done
```


