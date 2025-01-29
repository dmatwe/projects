**Установка Linux**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_SA_BASIC/9.%20Диаграмма%20классов/ооп.png)

```json
sudo apt update && sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y && sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - && sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt -y install postgresql-17 unzip atop htop
```

**Установка Postgres 17**


***sudo apt update*** — обновляет список доступных пакетов и их версий, но не устанавливает их. Это позволяет системе узнать, какие пакеты доступны для установки или обновления.

***sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y*** — обновляет все установленные пакеты до последних версий. Параметр -y автоматически подтверждает все запросы на установку, что делает процесс неинтерактивным. Переменная окружения DEBIAN_FRONTEND=noninteractive позволяет избежать появления интерактивных окон, что полезно в скриптах.

***sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt (lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'*** — добавляет репозиторий PostgreSQL в список источников пакетов. Здесь используется команда lsb_release -cs, которая возвращает кодовое имя текущего дистрибутива (например, focal для Ubuntu 20.04). Это добавляет новую строку в файл /etc/apt/sources.list.d/pgdg.list, что позволяет системе получать пакеты PostgreSQL из этого репозитория.

***wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - ***— загружает ключи для аутентификации пакетов из репозитория PostgreSQL и добавляет их в систему. Это необходимо для того, чтобы система могла доверять пакетам, загружаемым из этого репозитория.

***sudo apt-get update*** — снова обновляет список пакетов, теперь уже с учетом нового репозитория PostgreSQL.

***sudo DEBIAN_FRONTEND=noninteractive apt -y install postgresql-17 unzip atop htop*** — устанавливает PostgreSQL версии 17 и несколько утилит: unzip (для распаковки zip-архивов), atop (для мониторинга системы) и htop (для интерактивного мониторинга процессов).