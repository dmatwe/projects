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
sudo systemctl stop pgbouncer

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

sudo systemctl start pgbouncer
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
"admindb" "SCRAM-SHA-256$4096:o8lWjbVbS2jCPFoPh3pTvA==$OjABRU3Q9PhlM/djkTkNu71nUrKxlpUAzOycRf4oV5Y=:9h49FR8YqU3Ow4Extgchd6dLIZYidhOBb4qFRb2gTgU="
"admindb2" "md5a1edc6f635a68ce9926870fe752e8f2b"
"postgres" "SCRAM-SHA-256$4096:Ihs5xvNd8XlMh6YH7aJvCw==$P49fBjk49SQwuhEXr4OGQ8ihBXa5KfW8WhZEwZNdyhE=:QQgo78QoT9cZ7DM5J6dYRN9Vqy69rbxbEdK7qRZ7iGQ="
"postgres2" "SCRAM-SHA-256$4096:BiidMf8CW3agL/guI851CA==$XkaptGJbAkIy8S8oB238zWBjTeVY3XhOIiox8mooVyM=:B+DWfpChK+FJpuiGamLxkx98u62vV2+sfP7OBcNbAaA="
```

**Создание файла .pgpass для подключения без запроса пароля**

```python
echo "localhost:5432:thai:postgres:admin123#" > ~/.pgpass
echo "localhost:6432:thai:postgres:admin123#" >> ~/.pgpass
chmod 0600 ~/.pgpass

psql -h localhost -d thai
```


**Тестирование нагрузки pgbouncer**

Запускаем

```python
sudo systemctl start pgbouncer

sudo systemctl status pgbouncer

```
8 клиентов

```python
pgbench -c 8 -j 4 -T 10 -f ~/workload.sql -U postgres -h localhost -p 6432 thai -n | grep -E 'clients|tps'


number of clients: 8
tps = 9486.919903 (without initial connection time)
```

100-900 клиентов
```python
for i in 100 200 300 400 500 600 700 800 900; do pgbench -c $i -j 4 -T 10 -f ~/workload.sql -U postgres -h localhost -p 6432 thai -n | grep -E 'clients|tps'; done

number of clients: 100
tps = 8663.629282 (without initial connection time)

number of clients: 200
tps = 8603.703739 (without initial connection time)

number of clients: 300
tps = 8416.367568 (without initial connection time)

number of clients: 400
tps = 8089.888848 (without initial connection time)

number of clients: 500
tps = 7555.908321 (without initial connection time)

number of clients: 600
tps = 7277.903774 (without initial connection time)

number of clients: 700
tps = 7210.155776 (without initial connection time)

number of clients: 800
tps = 6800.548392 (without initial connection time)

number of clients: 900
tps = 6377.178932 (without initial connection time)
```