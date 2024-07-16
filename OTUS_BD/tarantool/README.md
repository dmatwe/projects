[1. Глоссарий](#t1)
<br/>
[2. Устройство](#t2)
<br/>
[3. Репликация](#t3)
<br/>
[4. Запуск](#t4)
<br/>
[5. LauJIT](#t5)
<br/>
[6. Файбер](#t6)
<br/>
[7. Сервер приложений](#t7)
<br/>
[8. Tarantool платформа](#t8)
<br/>
[9. Типы данных](#t9)
<br/>
[10. Запросы](#t10)
<br/>
[11. Как создать таблицы](#t11)
<br/>
[12. Типы индексов](#t12)
<br/>



**Глоссарий**
<a name="t1"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t9.png)



**Устройство Tarantool**
<a name="t2"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t1.png)


1. Строки хранятся в оперативной памяти (но есть 2 вида движков, дисковый и inmemory)
2. Однопоточный в части работы с данными в памяти (тк при многопоточных режимах тратится много времени на блокировки), системных потоков много. 1 поток - ***1 млн RPS MAX***
3. Тарантул персистентный, данные не теряются, тк кроме оперативки данные пишутся в лог на жестком диске
4. Есть индексы
5. Snapshot для быстрого восстановления состояния памяти
6. Можно поднимать реплики и реплицировать данные на соседний узел

***Tarantool*** - NoSQL бд (язык Lua), но есть ограниченный функционал SQL, на языке Lua в tarantool можно реализовать приложение и код будет исполняться в виде процесса работы приложения.
<br/>
***Lua*** (функции, процедуры)


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t2.png)


***TX*** - поток операционной системы

***Arena*** - аллоцированная оперативная память, после запуска TX (область памяти выделенная для потока)

***Сущности (таблицы)*** - хранятся в Arena, в таблицах ***строки*** (таплы), к таплам построены ***индексы*** разных типов (дерево, хэш)

***EV*** - событийный цикл и ***f файберы*** - многопоточность, файберы ходят в арену и у них есть каналы.

***IPROTO*** - еще один поток бинарного протокола, принимает запросы по сети (в этом потоке происходит парсинг запроса и перенаправление на файбер для исполнения), файбер отправляет запрос в арену и возвращает ответ с данными

***WAL*** - отвечает за репликацию данных, данные из арены пишутся на жесткий диск в xlog

***Snapdaemon*** - создает снапшоты по заданному интервалу (сжатое состояние арены)


**Репликация Tarantool**
<a name="t3"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t3.png)


В настройках при репликации указываем хосты узлов

***Relay*** - системный поток репликации, который вычитывает Xlog (update, insert и тд)

***Applier*** - файбер принимает данные и записывает в арену


**Старт Tarantool**
<a name="t4"></a>


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t4.png)


***memtx*** - движок оперативной памяти


**LauJIT**
<a name="t5"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t5.png)


**Файбер и многозадачность**
<a name="t6"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t6.png)

**Сервер приложений**
<a name="t7"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t7.png)

**Tarantool платформа**
<a name="t8"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t8.png)

**Типы данных**
<a name="t9"></a>

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t10.png)

**Запросы**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t11.png)

**Как создать таблицы**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t12.png)







***Start a Tarantool instance***

```bash 

docker run \
  --name app-instance-001 \
  -p 3301:3301 -d \
  tarantool/tarantool

```

***Connect to a running Tarantool instance***

```bash 

docker exec -t -i app-instance-001 console

```


***Создать space***

```bash 
box.schema.space.create('products')
```


***Посмотреть созданный space***
```bash 
 box.space.products
 ```

***Создать primary индекс для space***

***Задает ограничение на уникальность первого атрибута для тапла в space***

 ```bash 
box.space.products:create_index("pri")
 ```

***Вставить данные в space***

 ```bash 
box.space.products:insert{1,2,3}

box.space.products:insert{2,2,3, 'abc'}

 ```

 ***Select из space***


```bash 
 box.space.products:select({}, {limit= 10})
```

***удалить тапл из space по первичному ключу (индексу)***

```bash 
box.space.products:delete({1})
```


***Типы индексов***

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t14.png)

***Индексы по 2 полям***

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t15.png)

***Select индекса по итератору (эквивалент)***

```bash 
box.space.products:select({2}, {iterator="EQ"})
```
***Select индекса по итератору (больше чем)***


```bash 
box.space.products:select({1}, {iterator="GT"})
```

***Select индекса по итератору (меньше чем)***


```bash 
box.space.products:select({1}, {iterator="LT"})
```


***Select индекса по итератору (по убыванию)***
```bash 
box.space.products:select({1}, {iterator="REO"})
```


***Файловая архитектура***

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t17.png)

```bash 
box.snapshot()
```

***Журнал транзакции***

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t18.png)

***Snapshot***

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t19.png)

Тарантул по умолчанию хрнаит 2 последних снапшота, filecollector чистит

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t20.png)


**Обработка запросов**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t21.png)

1. Приложение подключается к Tarantool и отправляет запрос через IProto 
2. Запросы складываются в буфер между потоками IProto (парсинг, валидация запросов) и TX (работа с данными в арене)
3. TX забирает и исполняет запросы и передает в буфер TX / WAL (загрузка транзакций на жесткий диск, записи файла XLOG)
4. SNAPD - вычитывает все таблички и делает SNAPSHOT

**Применение Tarantool**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t22.png)

1. Key-value хранилище (кеш, персистентность)
2. Замена Kafka, RabbitMQ
3. Keychain
4. Профили пользователей
5. Промежуточное состояние тасков, статусы
6. Система лояльности

Можно использовать вместо Postgres (основная СУБД), но необходимо позаботиться об оперативной памяти на сервере 

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t23.png)



![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t24.png)



![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t25.png)


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t26.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t27.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t28.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t29.png)



**Репликация**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r1.png)

**Асинхронная репликация**

Данные, которые попали на мастер реплики получают не сразу

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r2.png)

**Ограничения репликации**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r3.png)

**Устройство репликации**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r4.png)

**Топологии репликации**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r5.png)

1. Однонаправленная - мастер отправляет данные на реплику, при смене мастера репликация сломается
2. Двунаправленная - можно переключать мастера между узлами, но мастер не знает только про соседний узел
3. Фуллмеш - часто используемая, каждый узел знает о всех узлах

**Настройка топологии**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r6.png)

***настройка каналов репликации***

```bash 
box.cfg{replication = {"localhost:3302", "localhost:3301"}}
```

**Удаление реплики**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r7.png)

**LSN XLOG**

Порядковый номер записи в XLOG

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r8.png)


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r9.png)


Смена мастера на узел Б


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r10.png)


**Векторные часы**

***совокупность всех последних LSN - актуальное состояние***

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r11.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r12.png)

**Переключение лидера**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r13.png)

**Нереплицируемые спейсы**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r14.png)


**Топология leader-leader**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r15.png)

**Проблема асинхронной репликации**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r16.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r18.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r17.png)

**Синхронной репликация**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r19.png)


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r20.png)

***Топология звезда***

Мастер должен знать про каждую из реплик

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r21.png)

Только DDL (Insert/update/Delete)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r22.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r23.png)

**Выбор лидера**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r24.png)

**Шардинг (горизонтальное масштабирование)**

***Storages*** - репликасеты, оди и те же спейсы, но данные разные (части)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r30.png)


***Routes*** - перенаправляют запросы в Storages

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r28.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r25.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r26.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r27.png)

**Выполнение запроса из Routes**

Взаимодействие через API
![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r29.png)


**Добавление репликасета, ребалансер**

При добавлении бакеты старых репликасетов переходят на новый репликасет, за переправку бакето (пользовательские данные)в отвечает ребалансер бакетов

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r31.png)

1. При переправке бакетов, ставится ограничение на запись в бакет
2. После усешной преправки, ставится ограничение на чтение бакета на старом репликасете 
3. Удаление бакета на старом репликасете происходит фоновым процессом 

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r32.png)


**Ограничения для ребалансер**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r33.png)

**Нагрузка на PROD**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r34.png)

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/r35.png)