﻿**Схема БД интернет магазина:**

![Image alt](https://github.com/dmatwe/projects/blob/main/OTUS_BD/PostgreSQL/1.%20Проектирование%20БД%20/схема_бд.png)
**Описание сущностей, атрибутов и бизнес-задач:**

| №                    | 1                                                                      |
| -------------------- | ---------------------------------------------------------------------- |
| Сущность             | orders_info                                                            |
| Схема                | orders                                                                 |
| Тип хранения истории | scd_2                                                                  |
| Описание             | Фактовая сущность заказов клиентов                                     |
| Атрибут сущности     | Описание атрибута                                                      |
| order_id             | Идентификатор заказа                                                   |
| client_id            | Идентификатор клиента                                                  |
| status_id            | Идентификатор статусу заказа                                           |
| session_id           | Идентификатор сессии                                                   |
| amount               | Сумма заказа                                                           |
| discount_flg         | Применялась ли скидка в заказе                                         |
| order_dttm           | Дата заказа                                                            |
| start_dttm           | Дата внесения записи                                                   |
| end_dttm             | Дата актуальности записи                                               |
| Бизнес-задачи        | Мониторинг статуса заказа;<br>Мониторинг всех заказов клиента.         |
| №                    | 2                                                                      |
| Сущность             | sessions_info                                                          |
| Схема                | sessions                                                               |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Фактовая сущность сессий пользователей                                 |
| Атрибут сущности     | Описание атрибута                                                      |
| session_id           | Идентификатор сессии                                                   |
| client_id            | Идентификатор клиента                                                  |
| src_id               | Идентификатор источника<br>с которого пользователь перешел             |
| Purchase_flg         | Совершалась ли покупка                                                 |
| visit_dttm           | Дата сессии                                                            |
| duration             | Время сессии                                                           |
| page_count           | Кол-во просмотренных страниц                                           |
| last_page_id         | Идентификатор последней страницы сессии                                |
| Бизнес-задачи        | Анализ поведения пользователей;<br>Анализ трафика с разных источников. |
| №                    | 3                                                                      |
| Сущность             | order_shipments                                                        |
| Схема                | orders                                                                 |
| Тип хранения истории | scd_2                                                                  |
| Описание             | Фактовая сущность трекинга заказов                                     |
| Атрибут сущности     | Описание атрибута                                                      |
| order_id             | Идентификатор заказа                                                   |
| track_number         | Трек-номер заказа                                                      |
| delivery_type_id     | Идентификатор типа доставки                                            |
| shipment_date        | Дата отправки                                                          |
| status_id            | Идентификатор статусу отправки                                         |
| delivery_company_id  | Идентификатор почтовой компании                                        |
| start_dttm           | Дата внесения записи                                                   |
| end_dttm             | Дата актуальности записи                                               |
| Бизнес-задачи        | Мониторинг статуса отправки.                                           |
| №                    | 4                                                                      |
| Сущность             | order_items                                                            |
| Схема                | orders                                                                 |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Фактовая сущность списка товаров в заказе                              |
| Атрибут сущности     | Описание атрибута                                                      |
| order_id             | Идентификатор заказа                                                   |
| item_id              | Идентификатор товара                                                   |
| item_count           | Кол-во товара одной позиции                                            |
| price                | Цена                                                                   |
| discount             | Размер скидки                                                          |
| Бизнес-задачи        | Анализ продаж                                                          |
| №                    | 5                                                                      |
| Сущность             | items_count                                                            |
| Схема                | items                                                                  |
| Тип хранения истории | scd_2                                                                  |
| Описание             | Фактовая сущность учета товаров на складе                              |
| Атрибут сущности     | Описание атрибута                                                      |
| item_id              | Идентификатор товара                                                   |
| location_id          | Идентификатор склада                                                   |
| item_count           | Кол-во товара на складе                                                |
| start_dttm           | Дата внесения записи                                                   |
| end_dttm             | Дата актуальности записи                                               |
| Бизнес-задачи        | Товароучет.                                                            |
| №                    | 6                                                                      |
| Сущность             | clients_info                                                           |
| Схема                | clients                                                                |
| Тип хранения истории | scd_2                                                                  |
| Описание             | Справочник пользователей                                               |
| Атрибут сущности     | Описание атрибута                                                      |
| client_id            | Идентификатор клиента                                                  |
| user_name            | ФИО                                                                    |
| src_id               | Идентификатор источника<br>с которого клиент перешел первый раз        |
| status_id            | Идентификатор cтатуса клиента (приоритет)                              |
| city                 | Город                                                                  |
| address              | Адрес                                                                  |
| registration_dttm    | Дата регистрации                                                       |
| start_dttm           | Дата внесения записи                                                   |
| end_dttm             | Дата актуальности записи                                               |
| Бизнес-задачи        | Мониторинг польователей;<br>Сегментация пользователей.                 |
| №                    | 7                                                                      |
| Сущность             | client_status                                                          |
| Схема                | clients                                                                |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник статусов для клиентов                                       |
| Атрибут сущности     | Описание атрибута                                                      |
| status_id            | Идентификатор cтатуса клиента (приоритет)                              |
| status_name          | Наименование статуса                                                   |
| Discount_percent     | Размер скидки в %                                                      |
| Бизнес-задачи        | Мониторинг доступных статусов для польователя.                         |
| №                    | 8                                                                      |
| Сущность             | client_src                                                             |
| Схема                | clients                                                                |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник источников                                                  |
| Атрибут сущности     | Описание атрибута                                                      |
| src_id               | Идентификатор источника                                                |
| src_name             | Наименование источника                                                 |
| Бизнес-задачи        | Мониторинг доступных источников.                                       |
| №                    | 9                                                                      |
| Сущность             | pages                                                                  |
| Схема                | clients                                                                |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник страниц сайта                                               |
| Атрибут сущности     | Описание атрибута                                                      |
| page_id              | Идентификатор страницы сайта                                           |
| page_name            | Наименование страницы сайта                                            |
| created_dttm         | Дата создания страницы                                                 |
| Бизнес-задачи        | Мониторинг существующих страниц сайта.                                 |
| №                    | 10                                                                     |
| Сущность             | delivery_type                                                          |
| Схема                | orders                                                                 |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник типов доставки                                              |
| Атрибут сущности     | Описание атрибута                                                      |
| delivery_type_id     | Идентификатор типа доставки                                            |
| delivery_type        | Наименование типа доставки                                             |
| Бизнес-задачи        | Мониторинг существующих типов доставки.                                |
| №                    | 11                                                                     |
| Сущность             | delivery_status                                                        |
| Схема                | orders                                                                 |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник статусов отправления                                        |
| Атрибут сущности     | Описание атрибута                                                      |
| status_id            | Идентификатор статуса отправления                                      |
| status_name          | Наименование статуса отправления                                       |
| Бизнес-задачи        | Мониторинг существующих статусов отправления.                          |
| №                    | 12                                                                     |
| Сущность             | order_status                                                           |
| Схема                | orders                                                                 |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник статусов заказа                                             |
| Атрибут сущности     | Описание атрибута                                                      |
| status_id            | Идентификатор статуса заказа                                           |
| status_name          | Наименование статуса заказа                                            |
| Бизнес-задачи        | Мониторинг существующих статусов заказа.                               |
| №                    | 13                                                                     |
| Сущность             | delivery_company                                                       |
| Схема                | orders                                                                 |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник почтовых компаний                                           |
| Атрибут сущности     | Описание атрибута                                                      |
| delivery_company_id  | Идентификатор почтовой компании                                        |
| status_name          | Наименование почтовой компании                                         |
| Бизнес-задачи        | Мониторинг существующих почтовых компаний.                             |
| №                    | 14                                                                     |
| Сущность             | item_producer                                                          |
| Схема                | items                                                                  |
| Тип хранения истории | scd_2                                                                  |
| Описание             | Справочник поставщиков                                                 |
| Атрибут сущности     | Описание атрибута                                                      |
| producer_id          | Идентификатор поставщика                                               |
| producer_name        | Наименование поставщика                                                |
| contact_info         | Контактная информация                                                  |
| start_dttm           | Дата внесения записи                                                   |
| end_dttm             | Дата актуальности записи                                               |
| Бизнес-задачи        | Мониторинг существующих поставщиков.                                   |
| №                    | 15                                                                     |
| Сущность             | items_location                                                         |
| Схема                | items                                                                  |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник складов                                                     |
| Атрибут сущности     | Описание атрибута                                                      |
| location_id          | Идентификатор склада                                                   |
| city                 | Город                                                                  |
| address              | Адрес склада                                                           |
| Бизнес-задачи        | Мониторинг существующих складов.                                       |
| №                    | 16                                                                     |
| Сущность             | items_info                                                             |
| Схема                | items                                                                  |
| Тип хранения истории | scd_2                                                                  |
| Описание             | Справочник товаров                                                     |
| Атрибут сущности     | Описание атрибута                                                      |
| item_id              | Идентификатор товара                                                   |
| item_name            | Наименование товара                                                    |
| category_id          | Идентификатор категории товара                                         |
| producer_id          | Идентификатор поставщика                                               |
| price                | Цена товара                                                            |
| start_dttm           | Дата внесения записи                                                   |
| end_dttm             | Дата актуальности записи                                               |
| Бизнес-задачи        | Анализ и мониторинг существующих товаров.                              |
| №                    | 17                                                                     |
| Сущность             | item_category                                                          |
| Схема                | items                                                                  |
| Тип хранения истории | scd_1                                                                  |
| Описание             | Справочник категорий товара                                            |
| Атрибут сущности     | Описание атрибута                                                      |
| category_id          | Идентификатор категории товара                                         |
| category_name        | Наименование категории товара                                          |
| Бизнес-задачи        | Мониторинг существующих категорий товара.                              |