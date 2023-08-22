**Запуск MySQL Docker container**

`docker run --name mysql -e MYSQL_ROOT_PASSWORD=password -d mysql/mysql-server:latest`

**Подключение к MySQL Docker container**

`docker exec -it mysql mysql -uroot -p`

 **Создание БД в MySQL**

`CREATE DATABASE online_store;`

 **Настроить innodb_buffer_pool**
 
`SET GLOBAL innodb_buffer_pool_size = 536870912;`

**Проверка**

`SHOW DATABASES`

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/MySQL/MySQL%20Docker/db.png)


 `SHOW VARIABLES LIKE 'innodb_buffer_pool_size';`

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/MySQL/MySQL%20Docker/buffer.png)