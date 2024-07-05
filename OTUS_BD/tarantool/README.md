
**Устройства Tarantool**


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


**Термины**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t9.png)


***Устройство Tarantool***


![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t2.png)


***TX*** - поток операционной системы

***Arena*** - аллоцированная оперативная память, после запуска TX (область памяти выделенная для потока)

***Сущности (таблицы)*** - хранятся в Arena, в таблицах ***строки*** (таплы), к таплам построены ***индексы*** разных типов (дерево, хэш)

***EV*** - событийный цикл и ***f файберы*** - многопоточность, файберы ходят в арену и у них есть каналы.

***IPROTO*** - еще один поток бинарного протокола, принимает запросы по сети (в этом потоке происходит парсинг запроса и перенаправление на файбер для исполнения), файбер отправляет запрос в арену и возвращает ответ с данными

***WAL*** - отвечает за репликацию данных, данные из арены пишутся на жесткий диск в xlog

***Snapdaemon*** - создает снапшоты по заданному интервалу (сжатое состояние арены)


**Репликация Tarantool**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t3.png)


В настройках при репликации указываем хосты узлов

***Relay*** - системный поток репликации, который вычитывает Xlog (update, insert и тд)

***Applier*** - файбер принимает данные и записывает в арену


**Старт Tarantool**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t4.png)


```bash 
box.cfg{}
```

***memtx*** - движок оперативной памяти


**LauJIT**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t5.png)


**Файбер и многозадачность**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t6.png)

**Сервер приложений**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t7.png)

**Tarantool платформа**
![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/tarantool/png/t8.png)

**Типы данных**

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