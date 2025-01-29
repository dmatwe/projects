**1. Установка Linux**

Яндекс облако: Ubuntu 24.04 

***Ресурсы:***

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2014.13.02.png)

**2. Установка Postgres 17**

```json
sudo apt update && sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y && sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - && sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt -y install postgresql-17 unzip atop htop
```

***Пояснения:***


***sudo apt update*** — обновляет список доступных пакетов и их версий, но не устанавливает их. Это позволяет системе узнать, какие пакеты доступны для установки или обновления.

***sudo DEBIAN_FRONTEND=noninteractive apt upgrade -y*** — обновляет все установленные пакеты до последних версий. Параметр -y автоматически подтверждает все запросы на установку, что делает процесс неинтерактивным. Переменная окружения DEBIAN_FRONTEND=noninteractive позволяет избежать появления интерактивных окон, что полезно в скриптах.

***sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt (lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'*** — добавляет репозиторий PostgreSQL в список источников пакетов. Здесь используется команда lsb_release -cs, которая возвращает кодовое имя текущего дистрибутива (например, focal для Ubuntu 20.04). Это добавляет новую строку в файл /etc/apt/sources.list.d/pgdg.list, что позволяет системе получать пакеты PostgreSQL из этого репозитория.

***wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - ***— загружает ключи для аутентификации пакетов из репозитория PostgreSQL и добавляет их в систему. Это необходимо для того, чтобы система могла доверять пакетам, загружаемым из этого репозитория.

***sudo apt-get update*** — снова обновляет список пакетов, теперь уже с учетом нового репозитория PostgreSQL.

***sudo DEBIAN_FRONTEND=noninteractive apt -y install postgresql-17 unzip atop htop*** — устанавливает PostgreSQL версии 17 и несколько утилит: unzip (для распаковки zip-архивов), atop (для мониторинга системы) и htop (для интерактивного мониторинга процессов).


**3. Тюнинг Линукса**

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

***Конфиг калькулятор***

https://pgconfigurator.cybertec.at/

![Image alt](https://github.com/dmatwe/projects/blob/main/postgres17/01deploy/Screenshot%202025-01-29%20at%2015.14.41.png)

```json
nano /etc/postgresql/17/main/postgresql.conf

pg_ctlcluster 17 main stop && pg_ctlcluster 17 main start
```