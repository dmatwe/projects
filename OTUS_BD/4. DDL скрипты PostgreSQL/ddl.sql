-- Создание базы данных t_online_store по шаблону
CREATE DATABASE t_online_store is_template = true;
-- Создание базы данных online_store из шаблонной t_online_store
CREATE DATABASE online_store template = t_online_store;


-- создание схем в бд online_store
CREATE SCHEMA orders;
CREATE SCHEMA clients;
CREATE SCHEMA items;


-- создание таблиц проекта интернет магазина
CREATE TABLE orders.orders_info (
           order_id int NOT NULL,
           client_id INTEGER NOT NULL,
           status_id INTEGER NOT NULL,
           session_id INTEGER NOT NULL,
           amount numeric(100, 10) NOT NULL CHECK (amount > 0) NOT NULL,
           discount_flg INTEGER NOT NULL,
           order_dttm date NOT NULL,
           start_dttm date NOT NULL,
           end_dttm date NOT NULL,
           PRIMARY KEY(order_id, start_dttm));
           
          
CREATE TABLE orders.order_shipments (
			order_id int NOT null,
			track_number varchar(255) NOT null,
			delivery_type_id int NOT null,
			shipment_date date NOT null,
			status_id int NOT null,
			delivery_company_id int NOT null,
			start_dttm date NOT null,
			end_dttm date NOT null,
			PRIMARY KEY(order_id, start_dttm));         
          
		
CREATE TABLE clients.sessions_info (
			session_id int NOT null,
			client_id int NOT null,
			src_id int NOT null,
			Purchase_flg int NOT null,
			visit_dttm date NOT null,
			duration numeric(100, 10) NOT null,
			page_count int NOT null,	
			last_page_id int NOT null,
			PRIMARY KEY(session_id));
		
		
CREATE TABLE orders.order_items (
			order_id int NOT null,
			item_id int NOT null,
			item_count int NOT null,
			price int NOT null,
			discount int,
			PRIMARY KEY(order_id));
		

CREATE TABLE items.items_count (
			item_id int NOT null,
			location_id int NOT null,
			item_count int NOT null,
			start_dttm date NOT null,
			end_dttm date NOT null,
			PRIMARY KEY(item_id, location_id, start_dttm));

		
		
CREATE TABLE clients.clients_info (
			client_id int NOT NULL,
			client_name varchar(50) NOT null,
			src_id int NOT null,
			status_id int NOT null,
			city varchar(255) NOT null,
			address varchar(255) NOT null,
			registration_dttm date NOT null,
			start_dttm date NOT null,
			end_dttm date NOT null,
			PRIMARY KEY(client_id));
		
CREATE TABLE clients.pages (
			page_id int NOT null,
			page_name varchar(50) NOT null,
			created_dttm date NOT null,
			PRIMARY KEY(page_id));
		
				
CREATE TABLE clients.client_status (
			status_id int NOT null,
			status_name varchar(50) NOT null,
			Discount_percent numeric(3, 2) NOT null,
			PRIMARY KEY(status_id));

		
CREATE TABLE clients.client_src (
			src_id int NOT null,
			src_name varchar(50) NOT null,
			PRIMARY KEY(src_id));

CREATE TABLE orders.delivery_type (
			delivery_type_id int NOT null,
			delivery_type varchar(50) NOT null,
			PRIMARY KEY(delivery_type_id));

		
CREATE TABLE orders.delivery_status (
			status_id int NOT null,
			status_name varchar(50) NOT null,
			PRIMARY KEY(status_id));

		
CREATE TABLE orders.delivery_company (
			delivery_company_id int NOT null,
			company_name varchar(50) NOT null,
			PRIMARY KEY(delivery_company_id));
		

CREATE TABLE orders.order_status (
			status_id int NOT null,
			status_name varchar(50) NOT null,
			PRIMARY KEY(status_id));
		

CREATE TABLE items.item_producer (
			producer_id  int NOT null,
			producer_name int NOT null,
			contact_info int NOT null,
			start_dttm date NOT null,
			end_dttm date NOT null,
			PRIMARY KEY(producer_id));  
			

CREATE TABLE items.items_location (
			location_id int NOT null,
			city varchar(255) NOT null,
			address varchar(255) NOT null,
			PRIMARY KEY(location_id));
		

CREATE TABLE items.items_info (
			item_id int NOT null,
			item_name int NOT null,
			category_id int NOT null,
			producer_id int NOT null,
			price int NOT null,
			start_dttm date NOT null,
			end_dttm date NOT null,
			PRIMARY KEY(item_id, producer_id));
			
CREATE TABLE items.item_category (
			category_id int NOT null,
			category_name int NOT null,
			PRIMARY KEY(category_id));

-- создание суперюзера для бд online_store
CREATE USER superuser with password 'superuser_password';
grant all privileges on database online_store to superuser;

-- создание обычного юзера 
CREATE USER user1 with password 'user1_password';

-- Дать доступ ко всем таблицам в схеме clients для user1
grant all privileges on all tables in schema clients to user1;

-- создание роли для superuser
create role online_store_super_user;
grant online_store_super_user to superuser;

-- создание роли для user1
create role clients_only;
grant clients_only to user1;





		