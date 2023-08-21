*Сотрудник склада*

**Просмотр входящих запросов от СТО**

`GET /warehouse/requests`

***Код 200***

```json
[
  {

    "request_id": 'integer',
    "status": 'string',
    "timestamp": integer
  },
  {
    "request_id": 'integer',
    "status": 'string',
    "status": 'string',
    "timestamp": integer
  }
  ...
]
```


***Код 400***

Некорректный запрос

{

  "code": string,

  "message": string

}

**Просмотр информации по конкретному запросу от СТО**

`GET /warehouse/requests/{request_id}`

***Код 200***

{

  "request_id": integer,
  "sto_id": integer,
  "items": 

  {

    "item_id": integer,
    "quantity": integer

    },

  "status": 'string',
  "timestamp": integer

  }

***Код 400***

Некорректный запрос
{
  "code": string,
  "message": string
}

***Код 404***

Заявка не найдена
{
  "code": string,
  "message": string
}

**Изменить статус заяаки**

`PATCH /warehouse/requests/{request_id}`

***Тело запроса***

{
  "request_id": string,
  "status": 'string',
  "timestamp": integer
}

***Код 200***

Ок

***Код 400***

Некорректный запрос
{
  "code": string,
  "message": string
}

***Код 404***

Заявка не найдена
{
  "code": string,
  "message": string
}

**Оформить доставку ЗЧ в СТО**

`POST /warehouse/shipping/requests/{request_id}`

***Код 200***

Ок

***Код 400***

Некорректный запрос
{
  "code": string,
  "message": string
}

***Код 404***

Заявка не найдена
{
  "code": string,
  "message": string
}


**Узнать статус отправления (обновить статус)**

`GET /warehouse/shipping/requests/{request_id}`

***Код 200***

{
  "request_id": string,
  "status": 'string',
  "timestamp": integer
}


***Код 400***

Некорректный запрос
{
  "code": string,
  "message": string
}

***Код 404***

Заявка не найдена
{
  "code": string,
  "message": string
}

*CRM*

**Создание заявки**

`POST b2b.taxi.yandex.net/b2b/cargo/integration/v2/claims/create\?request_id={string}`

- [Документация Яндекс](https://github.com/dmatwe/projects/tree/main/ВКР_бизнес_аналитика/ВКР_бакалавриат)

**Подтверждение заявки**

`POST b2b.taxi.yandex.net/b2b/cargo/integration/v2/claims/accept\ ?claim_id={string}`

- [Документация Яндекс](https://github.com/dmatwe/projects/tree/main/ВКР_бизнес_аналитика/ВКР_бакалавриат)

**Получение информации по заявке**

`POST b2b.taxi.yandex.net/b2b/cargo/integration/v2/claims/info\?claim_id={string}`

- [Документация Яндекс](https://github.com/dmatwe/projects/tree/main/ВКР_бизнес_аналитика/ВКР_бакалавриат)



*Я Доставка*

**Изменения статуса заявки в CRM**

`PATCH /warehouse/requests/{request_id}`

***Код 200***

{
  "request_id": string,
  "status": 'string',
  "timestamp": integer
}


***Код 400***

Некорректный запрос
{
  "code": string,
  "message": string
}

***Код 404***

Заявка не найдена
{
  "code": string,
  "message": string
}