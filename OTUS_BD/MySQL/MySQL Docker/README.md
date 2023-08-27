**Запуск MySQL Docker container**

`docker run --name mysql-container -e MYSQL_ROOT_PASSWORD=password -p 3306:3306 -d mysql/mysql-server:latest`

**Подключение к MySQL Docker container**

`docker exec -it mysql-container mysql -uroot -ppassword`

 **Создание БД в MySQL**

`CREATE DATABASE online_store;`

 **Настроить innodb_buffer_pool**
 
`SET GLOBAL innodb_buffer_pool_size = 536870912;`

**Проверка**

`SHOW DATABASES`

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/MySQL/MySQL%20Docker/db.png)


 `SHOW VARIABLES LIKE 'innodb_buffer_pool_size';`

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/MySQL/MySQL%20Docker/buffer.png)

**Для использования в клиентских приложениях**

`CREATE USER 'root'@'172.17.0.1' IDENTIFIED BY 'password';`

`GRANT ALL PRIVILEGES ON *.* TO 'root'@'172.17.0.1';`

`FLUSH PRIVILEGES;`