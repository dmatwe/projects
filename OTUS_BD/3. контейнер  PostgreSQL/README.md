1. Создать yaml с содержимым:
version: '3.5'

services:
  postgres:
    container_name: otus_bd_postgres_container
    image: postgres:15.3-alpine3.18
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: root
      PGDATA: /data/postgres
    volumes:
      - postgres:/data/postgres
    ports:
      - "5433:5432"
    networks:
      - postgres
    restart: unless-stopped
  
  pgadmin:
    container_name: otus_bd_pgadmin_container
    image: dpage/pgadmin4
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@mymail.ru
      PGADMIN_DEFAULT_PASSWORD: adminpassword
      PGADMIN_CONFIG_SERVER_MODE: 'False'
    volumes:
      - pgadmin:/var/lib/pgadmin

    ports:
      - 80:80
    networks:
      - postgres
    restart: unless-stopped

networks:
  postgres:
    driver: bridge

volumes:
    postgres:
    pgadmin:
2. находясь в дирриктории с docker-compose файлом выполнить команду docker-compose up 
3. авторизоваться по адрессу http://localhost
4. остановить контейнер можно командой docker-compose down

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/3.%20контейнер%20%20PostgreSQL/Screenshot%20.png)