**Описание сущностей**

| Сущность          | Описание сущности              | Атрибут           | Описание атрибута                   |
| ----------------- | ------------------------------ | ----------------- | ----------------------------------- |
| order             | Заказ-наряд                    | order_id          | Идентификатор заказа-наряда         |
|                   |                                | client_id         | Идентификатор клиента               |
|                   |                                | car_id            | Идентификатор авто                  |
|                   |                                | workshop_id       | Идентификатор мастерской/СТО        |
|                   |                                | status            | Статус заказа-наряда                |
|                   |                                | date              | Дата оформления                     |
| order_items_count | Список запчастей в заказ-наряд | order_id          | Идентификатор заказа-наряда         |
|                   |                                | item_id           | Идентификатор запчастей             |
|                   |                                | count             | Кол-во запчастей                    |
| items_count       | Кол-во запчастей на складах    | item_id           | Идентификатор запчасти              |
|                   |                                | storage_id        | Идентификатор склада                |
|                   |                                | count             | Кол-во запчастей                    |
|                   |                                | date_start        | Дата начала записи                  |
|                   |                                | date_end          | Дата конца записи                   |
| dim_items         | Справочник запчастей           | item_id           | Идентификатор детали                |
|                   |                                | item_name         | Наименование детали                 |
| dim_storage       | Справочник складов             | storage_id        | Идентификатор склада                |
|                   |                                | type              | Тип склада                          |
|                   |                                | storage_location  | Адрес склада                        |
| dim_workshop      | Справочник Мастерских/СТО      | workshop_id       | Идентификатор мастерской/СТО        |
|                   |                                | type              | Тип (мастерская/СТО)                |
|                   |                                | workshop_location | Адрес мастерской/СТО                |
|                   |                                | storage_id        | Идентификатор склада                |
| dim_client        | Справочник клиентов            | client_id         | Идентификатор клиента               |
|                   |                                | first_name        | Имя                                 |
|                   |                                | last_name         | Фамилия                             |
| dim_car           | Справочник авто                | car_id            | Идентификатор авто                  |
|                   |                                | client_id         | Идентификатор клиента               |
|                   |                                | mark              | Марка авто                          |
|                   |                                | model             | Модель авто                         |
|                   |                                | year              | Год выпуска авто                    |
|                   |                                | gos_number        | Гос-номер авто                      |
| order_work_time   | Учет времени работ сотрудников | order_id          | Идентификатор заказа-наряда         |
|                   |                                | employee_id       | Идентификатор сотрудника            |
|                   |                                | work_type_id      | Идентификатор типа работ            |
|                   |                                | timestamp_start   | Время начала работ                  |
|                   |                                | timestamp_end     | Время окончания работ               |
|                   |                                | status            | Статус работ                        |
| dim_work_type     | Справочник типов работ         | work_type_id      | Идентификатор типа работ            |
|                   |                                | work_type         | Наименование типа работ             |
| order_work_type   | Типы работ в заказ-наряд       | work_type_id      | Идентификатор типа работ            |
|                   |                                | order_id          | Идентификатор заказа-наряда         |
| order_employee    | Сотрудники заказа-наряда       | order_id          | Идентификатор заказа-наряда         |
|                   |                                | employee_id       | Идентификатор сотрудника            |
| dim_employee      | Справочник сотрудников         | employee_id       | Идентификатор сотрудника            |
|                   |                                | work_location_id  | Идентификатор места работы          |
|                   |                                | rank_id           | Идентификатор ранга (специальность) |
|                   |                                | date_start        | Дата зачисления в штат              |
|                   |                                | date_end          | Дата увольнения                     |
|                   |                                | first_name        | Имя                                 |
|                   |                                | last_name         | Фамилия                             |
| dim_rank          | Справочник специальностей      | rank_id           | Идентификатор ранга (специальность) |
|                   |                                | rank_name         | Наименование специальности          |


**Связи**

| Сущность_1        | Ключ связи                   | Сущность_2        | Тип связи |
| ----------------- | ---------------------------- | ----------------- | --------- |
| order             | order_id                     | order_items_count | 1:M       |
| order             | order_id                     | order_work_time   | 1:M       |
| order             | order_id                     | order_work_type   | 1:M       |
| order             | order_id                     | order_employee    | 1:M       |
| order_work_time   | employee_id                  | dim_employee      | M:1       |
| order_work_time   | work_type_id                 | dim_work_type     | M:1       |
| dim_work_type     | work_type_id                 | order_work_type   | 1:M       |
| order_employee    | employee_id                  | dim_employee      | M:1       |
| dim_employee      | rank_id                      | dim_rank          | M:1       |
| order_items_count | item_id                      | items_count       | M:M       |
| items_count       | item_id                      | dim_items         | M:1       |
| items_count       | storage_id                   | dim_storage       | M:1       |
| dim_storage       | storage_id                   | dim_workshop      | 1:1       |
| dim_workshop      | workshop_id                  | order             | 1:M       |
| dim_client        | client_id                    | order             | 1:M       |
| dim_client        | client_id                    | dim_car           | 1:M       |
| dim_car           | car_id                       | order             | 1:M       |
| dim_employee      | work_location_id:storage_id  | dim_storage       | 1:1       |
| dim_employee      | work_location_id:workshop_id | dim_workshop      | 1:1       |
