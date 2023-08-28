# Virtual Machines (Compute Cloud)

## Создание виртуальной машины:
name vm: otus-db-mysql-vm-01

Создать сеть:
Каталог: default
Имя: otus-db-mysql-net-01

Создать подсеть:
Имя: otus-db-mysql-subnet-01
CIDR: 10.80.0.0/16

Доступ
username: otus

Сгенерировать ssh-key:
```bash
cd ~/Otus/RDBMS/MySQL/MySQL_DDL/demo

# ssh-keygen -t rsa -b 2048 -f ~/Otus/RDBMS/MySQL/MySQL_DDL/demo/otus_key
# name ssh-key: otus_key
# passphrase: pasS$12345
# chmod 400 ~/Otus/RDBMS/MySQL/MySQL_DDL/demo/otus_key.pub
cat ~/Otus/RDBMS/MySQL/MySQL_DDL/demo/otus_key.pub
```

## Подключение к VM:
cd ~/Otus/RDBMS/MySQL/MySQL_DDL/demo
ssh -i otus_key otus@158.160.8.103

## Установка MySQL:
cd /tmp && sudo apt update && sudo apt install -y curl gnupg2 lsb-release mc nano \
    && curl -O https://repo.percona.com/apt/percona-release_latest.generic_all.deb \
    && sudo apt install -y ./percona-release_latest.generic_all.deb \
    && sudo apt update && sudo percona-release setup pdps-8.0 \
    && sudo apt install -y percona-server-server \
    && sudo apt install -y percona-mysql-shell

Password: root

mysql -h127.0.0.1 -uroot -proot -e "CREATE FUNCTION fnv1a_64 RETURNS INTEGER SONAME 'libfnv1a_udf.so';
    CREATE FUNCTION fnv_64 RETURNS INTEGER SONAME 'libfnv_udf.so';
    CREATE FUNCTION murmur_hash RETURNS INTEGER SONAME 'libmurmur_udf.so';"

# DDL
cd ~/Otus/RDBMS/MySQL/MySQL_DDL/demo
ssh -i otus_key otus@158.160.8.103

mysqlsh -h127.0.0.1 -uroot -proot --sqlc
mysql -h127.0.0.1 -uroot -proot

## Create DB
<!-- https://dev.mysql.com/doc/refman/8.0/en/create-database.html -->
SHOW DATABASES;
DROP DATABASE otus_db;
DROP DATABASE IF EXISTS otus_db;
CREATE DATABASE IF NOT EXISTS otus_db;

SHOW DATABASES;

## Create User
<!-- https://dev.mysql.com/doc/refman/8.0/en/create-user.html -->
DROP USER IF EXISTS 'otus_user'@'%';
<!-- CREATE USER IF NOT EXISTS 'otus_user'@'%' IDENTIFIED WITH mysql_native_password BY 'otus123'; -->
<!-- CREATE USER IF NOT EXISTS 'otus_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'otus123'; -->

SHOW VARIABLES LIKE 'default_authentication_plugin';
CREATE USER IF NOT EXISTS 'otus_user'@'%' IDENTIFIED BY 'otus123';
SELECT * FROM mysql.user WHERE user LIKE '%otus%' \G;

SELECT id, user, host, db, command FROM information_schema.processlist;
SHOW PROCESSLIST;
KILL 20;
SHOW ENGINE INNODB STATUS \G;


## Grant
### Anoter session
cd ~/Otus/RDBMS/MySQL/MySQL_DDL/demo
ssh -i otus_key otus@158.160.8.103

mysqlsh -h127.0.0.1 -uotus_user -potus123 --sqlc
SHOW DATABASES;
mysqlsh -h127.0.0.1 -uotus_user -potus123 -Dotus_db --sqlc

### Main session
GRANT ALL PRIVILEGES ON otus_db.* TO 'otus_user'@'%';
<!-- GRANT SELECT (col1), INSERT (col1, col2) ON otus.t1 TO 'otus'@'localhost'; -->
FLUSH PRIVILEGES;
SHOW GRANTS FOR 'otus_user';

### Anoter session
mysqlsh -h127.0.0.1 -uotus_user -potus123 -Dotus_db --sqlc

### Main session
REVOKE ALL ON otus_db.* FROM 'otus_user'@'%';
FLUSH PRIVILEGES;

<!-- DROP ROLE 'otus_role'; -->
CREATE ROLE 'otus_role';
GRANT ALL PRIVILEGES ON otus_db.* TO 'otus_role';
GRANT 'otus_role' TO 'otus_user'@'%';
<!-- REVOKE 'otus_role' FROM 'otus_user'@'%'; -->
<!-- DROP ROLE 'otus_role'; -->

SHOW GRANTS FOR 'otus_user'@'%';
SHOW GRANTS FOR 'otus_user'@'%' USING 'otus_role';

SET DEFAULT ROLE ALL TO 'otus_user'@'%';
SET DEFAULT ROLE NONE TO 'otus_user'@'%';
<!-- SELECT CURRENT_ROLE(); -->

## Create table
USE otus_db;

CREATE TABLE t1 (c1 INT PRIMARY KEY);
DESC t1;
SHOW CREATE TABLE t1 \G;

INSERT INTO t1 (c1) VALUES (1);
SELECT * from t1;

sudo ls -lh /var/lib/mysql/otus_db

sudo ls -lh /var/lib/mysql

SHOW VARIABLES LIKE 'innodb_file_per_table';

<!-- DROP TABLESPACE otus_ts1; -->
<!-- CREATE TABLESPACE otus_ts1 ADD DATAFILE '/var/lib/mysql/otus_db/otus_ts1.ibd' Engine=InnoDB; -->
CREATE TABLESPACE otus_ts1 ADD DATAFILE '/var/lib/mysql/otus_ts1.ibd' Engine=InnoDB;

CREATE TABLE t2 (c1 INT PRIMARY KEY) TABLESPACE otus_ts1;
INSERT INTO t2 (c1) VALUES (1);
sudo ls -lh /var/lib/mysql/otus_db
sudo ls -lh /var/lib/mysql

ALTER TABLE t1 TABLESPACE otus_ts1;
sudo ls -lh /var/lib/mysql/otus_db
sudo ls -lh /var/lib/mysql

ALTER TABLE t2 TABLESPACE innodb_file_per_table;
SELECT * from t2;

SELECT a.NAME AS space_name, b.NAME AS table_name FROM INFORMATION_SCHEMA.INNODB_TABLESPACES a,
       INFORMATION_SCHEMA.INNODB_TABLES b WHERE a.SPACE=b.SPACE AND a.NAME LIKE 'otus_ts1';
       
SELECT
    TABLESPACE_NAME as "Tablespace Name", FILE_NAME AS "File Name",
    INITIAL_SIZE AS "Initial Size",	DATA_FREE AS "Free Space",
    TOTAL_EXTENTS * EXTENT_SIZE AS "Total Size", MAXIMUM_SIZE AS "Max" 
FROM information_schema.FILES;

DROP TABLESPACE otus_ts1;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLESPACE otus_ts1;

# Партиционирование (Partitioning) 
<!-- https://dev.mysql.com/doc/refman/8.0/en/partitioning-overview.html -->
USE otus_db;
CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
);

SELECT * FROM employees;

sudo ls -lh /var/lib/mysql/otus_db/

DROP TABLE employees;

## PARTITION BY RANGE
CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY RANGE (store_id) (
    PARTITION p0 VALUES LESS THAN (6),
    PARTITION p1 VALUES LESS THAN (11),
    PARTITION p2 VALUES LESS THAN (16),
    PARTITION p3 VALUES LESS THAN (21)
);

INSERT INTO employees 
VALUES 
    (1,'Ivan','Ivanov','1970-01-01',1),
    (2,'Petr','Petrov','1980-01-01',7),
    (3,'Andrey','Andreev','1990-01-01',15),
    (4,'Vasili','Vasilev','2000-01-01',18);

SELECT * FROM employees PARTITION (p3);

sudo ls -lh /var/lib/mysql/otus_db/

SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'employees';

INSERT INTO employees VALUES (5,'Maxim','Maximov','2010-01-01',22);

ALTER TABLE employees ADD PARTITION (PARTITION p4 VALUES LESS THAN (26));

INSERT INTO employees VALUES (5,'Maxim','Maximov','2010-01-01',22);

SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'employees';
ANALYZE TABLE otus_db.employees;
SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'employees';

SELECT * FROM employees;
DROP TABLE employees;


CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY RANGE (store_id) (
    PARTITION p0 VALUES LESS THAN (6),
    PARTITION p1 VALUES LESS THAN (11),
    PARTITION p2 VALUES LESS THAN (16),
    PARTITION p3 VALUES LESS THAN (21),
    PARTITION p4 VALUES LESS THAN MAXVALUE
);

INSERT INTO employees VALUES 
    (1,'Ivan','Ivanov','1970-01-01',1),
    (2,'Petr','Petrov','1980-01-01',7),
    (3,'Andrey','Andreev','1990-01-01',15),
    (4,'Vasili','Vasilev','2000-01-01',18),
    (5,'Maxim','Maximov','2010-01-01',22);

SELECT * FROM employees;

SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'employees';

SELECT * FROM employees PARTITION (p4);
   
ALTER TABLE employees DROP PARTITION p4;
SELECT * FROM employees;


CREATE TABLE employees_tmp LIKE employees;
sudo ls -lh /var/lib/mysql/otus_db/
SHOW CREATE TABLE employees_tmp \G;
SELECT * FROM employees_tmp;
DROP TABLE employees_tmp;


SELECT * FROM employees;
ALTER TABLE employees REMOVE PARTITIONING;
sudo ls -lh /var/lib/mysql/otus_db/

SELECT * FROM employees;

SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'employees';

DROP TABLE employees;

CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY RANGE ( YEAR(hired) ) (
    PARTITION p0 VALUES LESS THAN (1971),
    PARTITION p1 VALUES LESS THAN (1981),
    PARTITION p2 VALUES LESS THAN (1991),
    PARTITION p3 VALUES LESS THAN MAXVALUE
);

INSERT INTO employees VALUES 
    (1,'Ivan','Ivanov','1970-01-01',1),
    (2,'Petr','Petrov','1980-01-01',7),
    (3,'Andrey','Andreev','1990-01-01',15),
    (4,'Vasili','Vasilev','2000-01-01',18),
    (5,'Maxim','Maximov','2010-01-01',22);

SELECT * FROM employees;

SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'employees';

SELECT * FROM employees PARTITION (p3);

DROP TABLE employees;


## PARTITION BY RANGE COLUMNS
CREATE TABLE r1 (
    a INT,
    b INT
)
PARTITION BY RANGE (a)  (
    PARTITION p0 VALUES LESS THAN (5),
    PARTITION p1 VALUES LESS THAN (MAXVALUE)
);

INSERT INTO r1 VALUES (5,10), (5,11), (5,12);

SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'r1';

SELECT * FROM r1;
SELECT * FROM r1 PARTITION (p1);

INSERT INTO r1 VALUES (null,10);
SELECT * FROM r1 PARTITION (p0);

DROP TABLE r1;


CREATE TABLE rc1 (
    a INT,
    b INT
)
PARTITION BY RANGE COLUMNS(a, b) (
    PARTITION p0 VALUES LESS THAN (5, 12),
    PARTITION p3 VALUES LESS THAN (MAXVALUE, MAXVALUE)
);

INSERT INTO rc1 VALUES (5,10), (5,11), (5,12);

SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'rc1';

SELECT * FROM rc1 PARTITION (p0);
SELECT (5,10) < (5,12), (5,11) < (5,12), (5,12) < (5,12);
SELECT ROW(5,10) < ROW(5,12), ROW(5,11) < ROW(5,12), ROW(5,12) < ROW(5,12);

Это потому, что мы сравниваем строки, а не скалярные значения.
https://dev.mysql.com/doc/refman/8.0/en/partitioning-columns-range.html

DROP TABLE rc1;


### Valid
CREATE TABLE rc4 (
    a INT,
    b INT,
    c INT
)
PARTITION BY RANGE COLUMNS(a,b,c) (
    PARTITION p0 VALUES LESS THAN (0,25,50),
    PARTITION p1 VALUES LESS THAN (10,20,100),
    PARTITION p2 VALUES LESS THAN (10,30,50),
    PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE,MAXVALUE)
);

SELECT (0,25,50) < (10,20,100), (10,20,100) < (10,30,50);

### Not Valid
CREATE TABLE rcf (
    a INT,
    b INT,
    c INT
)
PARTITION BY RANGE COLUMNS(a,b,c) (
    PARTITION p0 VALUES LESS THAN (0,25,50),
    PARTITION p1 VALUES LESS THAN (20,20,100),
    PARTITION p2 VALUES LESS THAN (10,30,50),
    PARTITION p3 VALUES LESS THAN (MAXVALUE,MAXVALUE,MAXVALUE)
);

SELECT (0,25,50) < (20,20,100), (20,20,100) < (10,30,50);

DROP TABLE rc4;


### alter table
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
);

SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'employees';

ALTER TABLE employees PARTITION BY RANGE COLUMNS (lname) (
    PARTITION p0 VALUES LESS THAN ('B'),
    PARTITION p1 VALUES LESS THAN ('K'),
    PARTITION p2 VALUES LESS THAN ('O'),
    PARTITION p3 VALUES LESS THAN (MAXVALUE)
);

INSERT INTO employees VALUES 
    (1,'Ivan','Ivanov','1970-01-01',1),
    (2,'Petr','Petrov','1980-01-01',7),
    (3,'Andrey','Andreev','1990-01-01',15),
    (4,'Vasili','Vasilev','2000-01-01',18),
    (5,'Maxim','Maximov','2010-01-01',22);

SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'employees';

SELECT * FROM employees PARTITION (p3);

ALTER TABLE employees REMOVE PARTITIONING;
SELECT * FROM employees;

ALTER TABLE employees PARTITION BY RANGE COLUMNS (lname) (
    PARTITION p0 VALUES LESS THAN ('А'),
    PARTITION p1 VALUES LESS THAN ('К'),
    PARTITION p2 VALUES LESS THAN ('О'),
    PARTITION p3 VALUES LESS THAN (MAXVALUE)
);

INSERT INTO employees VALUES 
    (1,'Борис','Борисов','1970-01-01',1),
    (2,'Петр','Петров','1980-01-01',7),
    (3,'Андрей','Андреев','1990-01-01',15),
    (4,'Василий','Васильев','2000-01-01',18),
    (5,'Максим','Максимов','2010-01-01',22);

ANALYZE TABLE otus_db.employees;
SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'employees';

SELECT * FROM employees PARTITION (p3);

DROP TABLE employees;


## PARTITION BY LIST

CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY LIST(store_id) (
    PARTITION pNorth VALUES IN (3,5,6,9,17),
    PARTITION pEast VALUES IN (1,2,10,11,19,20),
    PARTITION pWest VALUES IN (4,12,13,14,18),
    PARTITION pCentral VALUES IN (7,8,15,16)
);

INSERT INTO employees VALUES 
    (1,'Ivan','Ivanov','1970-01-01',3),
    (2,'Petr','Petrov','1980-01-01',2),
    (3,'Andrey','Andreev','1990-01-01',15),
    (4,'Vasili','Vasilev','2000-01-01',18),
    (5,'Maxim','Maximov','2010-01-01',22);

SELECT * FROM employees;

INSERT IGNORE INTO employees VALUES 
    (1,'Ivan','Ivanov','1970-01-01',3),
    (2,'Petr','Petrov','1980-01-01',2),
    (3,'Andrey','Andreev','1990-01-01',15),
    (4,'Vasili','Vasilev','2000-01-01',18),
    (5,'Maxim','Maximov','2010-01-01',22);

ANALYZE TABLE otus_db.employees;
SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'employees';

SELECT * FROM employees PARTITION (pCentral);

SELECT * FROM employees;
explain SELECT * FROM employees where store_id = 15;
explain SELECT * FROM employees where store_id in (15, 18);

DROP TABLE employees;


## PARTITION BY HASH

CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY HASH(store_id)
PARTITIONS 4;

INSERT INTO employees VALUES 
    (1,'Ivan','Ivanov','1970-01-01',3),
    (2,'Petr','Petrov','1980-01-01',2),
    (3,'Andrey','Andreev','1990-01-01',15),
    (4,'Vasili','Vasilev','2000-01-01',18),
    (5,'Maxim','Maximov','2010-01-01',22);

SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'employees';

SELECT * FROM employees PARTITION (p2);

SELECT * FROM employees;
DROP TABLE employees;


## PARTITION BY KEY
CREATE TABLE employees (
    id INT NOT NULL PRIMARY KEY,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY KEY()
PARTITIONS 3;

INSERT INTO employees VALUES 
    (1,'Ivan','Ivanov','1970-01-01',3),
    (2,'Petr','Petrov','1980-01-01',2),
    (3,'Andrey','Andreev','1990-01-01',15),
    (4,'Vasili','Vasilev','2000-01-01',18),
    (5,'Maxim','Maximov','2010-01-01',22);

SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'employees';

SELECT * FROM employees;
explain SELECT * FROM employees WHERE id = 3;
SELECT * FROM employees PARTITION (p2);

DROP TABLE employees;

### Valid
CREATE TABLE employees (
    id INT PRIMARY KEY,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY KEY()
PARTITIONS 2;
DROP TABLE employees;

CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL,
    UNIQUE KEY (id)
)
PARTITION BY KEY()
PARTITIONS 3;

ALTER TABLE employees PARTITION BY KEY() PARTITIONS 4;
ANALYZE TABLE otus_db.employees;
SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 'employees';
DROP TABLE employees;

### Not Valid
CREATE TABLE employees (
    id INT NOT NULL,
    fname VARCHAR(30),
    lname VARCHAR(30),
    hired DATE NOT NULL,
    store_id INT NOT NULL
)
PARTITION BY KEY()
PARTITIONS 3;

-- https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations.html
-- https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations-partitioning-keys-unique-keys.html


## PARTITION IN ANOTHER DIR
CREATE TABLE IF NOT EXISTS t1 (
  `hashpart` SMALLINT UNSIGNED NOT NULL
)
PARTITION BY RANGE COLUMNS(`hashpart`) (
    PARTITION `p_123` VALUES LESS THAN(123) DATA DIRECTORY = '/tmp',
    PARTITION `p_MAXVALUE` VALUES LESS THAN(MAXVALUE) DATA DIRECTORY = '/tmp'
);

<!-- SHOW VARIABLES LIKE 'innodb_directories'; -->
sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
--------------------------------------------
innodb_directories = "/tmp"
--------------------------------------------
sudo systemctl restart mysql

USE otus_db;
CREATE TABLE IF NOT EXISTS t1 (
  `hashpart` SMALLINT UNSIGNED NOT NULL)
PARTITION BY RANGE COLUMNS(`hashpart`) (
    PARTITION `p_123` VALUES LESS THAN(123) DATA DIRECTORY = '/tmp',
    PARTITION `p_MAXVALUE` VALUES LESS THAN(MAXVALUE) DATA DIRECTORY = '/tmp'
);

sudo ls -lh /tmp/otus_db/


===============================================================================
CREATE TABLE otus_db.test (
    id INTEGER NOT NULL AUTO_INCREMENT,
    create_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

ALTER TABLE otus_db.test PARTITION BY RANGE (MONTH(create_date)) (
    PARTITION p202206 VALUES LESS THAN (7),
    PARTITION p202207 VALUES LESS THAN (8),
    PARTITION p202208 VALUES LESS THAN (9)
);

ALTER TABLE otus_db.test DROP PRIMARY KEY, add PRIMARY KEY (id, create_date);

ALTER TABLE otus_db.test PARTITION BY RANGE (MONTH(create_date)) (
    PARTITION p202206 VALUES LESS THAN (7),
    PARTITION p202207 VALUES LESS THAN (8),
    PARTITION p202208 VALUES LESS THAN (9)
);

<!-- TRUNCATE TABLE otus_db.test;
ALTER TABLE otus_db.test REMOVE PARTITIONING; -->

SHOW CREATE TABLE otus_db.test \G;
SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'test';

USE otus_db;
DROP PROCEDURE IF EXISTS batch_insert;
DELIMITER ;;
CREATE PROCEDURE batch_insert()
BEGIN
  DECLARE v INT DEFAULT 0;
  WHILE v < 1000 DO  
    INSERT INTO test (create_date) 
    VALUES    
      (FROM_UNIXTIME(UNIX_TIMESTAMP('2022-06-01 00:00:01') + FLOOR(0 + (RAND() * 7884000))));
    SET v = v + 1;
  END WHILE;
END;;
DELIMITER ;

CALL batch_insert();

SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'test';

SELECT * FROM otus_db.test LIMIT 10;

SELECT * FROM otus_db.test WHERE create_date = '2022-06-07 15:09:31';
EXPLAIN SELECT * FROM otus_db.test WHERE create_date = '2022-06-07 15:09:31';

SELECT * FROM otus_db.test WHERE DATE(create_date) = '2022-08-22';
EXPLAIN SELECT * FROM otus_db.test WHERE DATE(create_date) = '2022-08-22';
EXPLAIN SELECT * FROM otus_db.test WHERE create_date BETWEEN '2022-08-22 00:00:00' AND '2022-08-22 23:59:59';
EXPLAIN SELECT * FROM otus_db.test WHERE MONTH(create_date) = 7;
SELECT * FROM otus_db.test PARTITION (p202207);

ALTER TABLE otus_db.test REMOVE PARTITIONING;

ALTER TABLE otus_db.test PARTITION BY RANGE (TO_DAYS(create_date)) (
    PARTITION p202206 VALUES LESS THAN (TO_DAYS('2022-07-01')),
    PARTITION p202207 VALUES LESS THAN (TO_DAYS('2022-08-01')),
    PARTITION p202208 VALUES LESS THAN (TO_DAYS('2022-09-01'))
);

ANALYZE TABLE otus_db.test;
SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'test';

EXPLAIN SELECT * FROM otus_db.test WHERE create_date = '2022-06-07 15:09:31';
EXPLAIN SELECT * FROM otus_db.test WHERE create_date BETWEEN '2022-08-22 00:00:00' AND '2022-08-22 23:59:59';
EXPLAIN SELECT * FROM otus_db.test WHERE create_date BETWEEN '2022-06-22 00:00:00' AND '2022-07-22 23:59:59';
EXPLAIN SELECT * FROM otus_db.test WHERE DATE(create_date) = '2022-08-22';

<!-- ALTER TABLE otus_db.test TRUNCATE PARTITION p202206;
ALTER TABLE otus_db.test DROP PARTITION p202206; -->

ALTER TABLE otus_db.test REMOVE PARTITIONING;
ANALYZE TABLE otus_db.test;
SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'test';

ALTER TABLE otus_db.test PARTITION BY RANGE (TO_DAYS(create_date)) (
    PARTITION p202206 VALUES LESS THAN (TO_DAYS('2022-07-01')),
    PARTITION p202208 VALUES LESS THAN (TO_DAYS('2022-09-01'))
);
ANALYZE TABLE otus_db.test;
SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'test';

ALTER TABLE otus_db.test PARTITION BY RANGE (TO_DAYS(create_date)) (
    PARTITION p202207 VALUES LESS THAN (TO_DAYS('2022-08-01'))
);
SELECT from_days(738734);

ALTER TABLE otus_db.test REORGANIZE PARTITION p202208 INTO (
    PARTITION p202207 VALUES LESS THAN (TO_DAYS('2022-08-01')),
    PARTITION p202208 VALUES LESS THAN (TO_DAYS('2022-09-01'))
);

ANALYZE TABLE otus_db.test;
<!-- OPTIMIZE TABLE otus_db.test; -->
SELECT p.PARTITION_NAME, p.TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS p WHERE TABLE_NAME = 'test';


SELECT table_schema, table_name,
    ROUND(((data_length + index_length)/1024/1024), 2) AS "Data+Index size in MB",
    ROUND((index_length/1024/1024), 2) AS "Index size in MB",
    ROUND((data_free/1024/1024), 2) AS "Data Free size in MB",
    create_options
FROM INFORMATION_SCHEMA.TABLES
WHERE table_name = 'test'
ORDER BY table_schema, (data_length + index_length) DESC;
===============================================================================


===============================================================================
https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html

DROP TABLE IF EXISTS otus_db.test;
CREATE TABLE IF NOT EXISTS otus_db.test (c1 INT);

<!-- SELECT * FROM information_schema.innodb_tables WHERE name = 'otus_db/test';
SELECT table_id, name, has_default, default_value FROM information_schema.innodb_columns WHERE table_id = 1088; -->

set cte_max_recursion_depth = 1000000;
select @@cte_max_recursion_depth;

INSERT INTO otus_db.test
    select n from (
        WITH RECURSIVE seq (n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM seq WHERE n < 1000000
        )
        SELECT n FROM seq
    ) as seq;
SELECT count(*) FROM test;

### Algorithm Copy
alter table test add c2 int, algorithm=copy;
alter table test drop c2, algorithm=copy;

### Algorithm Inplace
alter table test add c2 int, algorithm=inplace;
alter table test drop c2, algorithm=inplace;

### Algorithm Instant
alter table test add c2 int, algorithm=instant;
SELECT table_id, name, instant_cols FROM information_schema.innodb_tables WHERE name LIKE '%test%';
SELECT table_id, name, has_default, default_value FROM information_schema.innodb_columns WHERE table_id = 1145;

alter table test drop c2, algorithm=instant;
ERROR 1845 (0A000): ALGORITHM=INSTANT is not supported for this operation. Try ALGORITHM=COPY/INPLACE

alter table test drop c2, algorithm=inplace;
Query OK, 0 rows affected (54.01 sec)

### Algorithm Default
alter table test add c2 int;
SELECT * FROM information_schema.innodb_tables WHERE name = 'otus_db/test';

alter table test drop c2;

===============================================================================