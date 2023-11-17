

**Установка MongoDB в Docker и заливка данными**

https://github.com/neelabalan/mongodb-sample-dataset

`docker pull mvertes/alpine-mongo`

`docker run -d --name mongo -p 2717:27017 -v ~/mongodb:/data/db mvertes/alpine-mongo`

`brew install mongodb/brew/mongodb-database-tools`

`./script.sh localhost 2717`

**Подключиться к контейнеру**
`docker exec -it mongo mongo`



**Список БД**
`show databases`


**Выбрать БД**
`use sample_supplies`


**Показать коллекции БД**
`show collections`

**Выбрать коллекцию**
`db.sales`

**SELECT из коллекции**
`db.getCollection('sales').find({})`


**Исключить атрибут при Select**
`db.sales.find({}, {items : 0})`

**Поиск c фильтром**
`db.getCollection('sales').find({ "purchaseMethod" : "Online" })`

**Поиск c фильтром по вложенным коллекциям**
`db.getCollection('sales').find({ 'customer.age': {$gt: 30}})`

**фильтр and**
`db.getCollection('sales').find({ $and: [“purchaseMethod" : "Online" }, { 'customer.age': {$gt: 30}]})`

**Создать коллекцию accounts**
`db.accounts`

**Вставить данные в коллекцию accounts**
`db.accounts.insertOne({email: 'email@mail.ru', date: '2022-12-12'})`

**Изменить запись**
`db.accounts.updateOne({ email: 'email@mail.ru' }, { $set: { date: '2023-12-12' } })`
