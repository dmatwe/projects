﻿| Чеклист для проверки продукта                                                                                                   |
| ------------------------------------------------------------------------------------------------------------------------------- |
| 1\. Проверить наличие меню на сайте или в приложении.                                                                           |
| 2\. Попробовать добавить блюдо в корзину и убедиться, что процесс проходит без ошибок.                                          |
| 3\. Проверить различные способы оплаты заказа (карта, наличные, онлайн-платежи).                                                |
| 4\. Убедиться, что после оплаты заказа клиент получает чек с номером заказа.                                                    |
| 5\. Проверить возможность редактирования позиций в корзине (добавление, удаление).                                              |
| 6\. Попробовать выбрать способ получения заказа (доставка, самовывоз) и удостовериться, что это работает корректно.             |
| 7\. Проверить различные способы оплаты заказа (карта, наличные, онлайн-платежи).                                                |
| 8\. Протестировать процесс регистрации в программе лояльности и проверить функционал по копированию и списанию бонусных баллов. |
| 9\. Проверить возможность обратиться к менеджеру через чат или звонок и получить ответы на вопросы.                             |
| 10\. Протестировать функционал отмены заказа и убедиться, что клиент может вернуть свои деньги без проблем.                     |


**Тест-кейсы** 

US01
Я как клиент
ХОЧУ ознакомиться с меню
ЧТОБЫ выбрать блюдо


US02
Я как клиент
ХОЧУ добавить блюда в корзину
ЧТОБЫ его приготовили


| ID                                                                         | HP_US1/2                                                               |
| -------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Название                                                                   | Успешное добавление блюда в корзину                                    |
| Краткое описание                                                           | Отображение информации о позиции и успешное добавление блюда в корзину |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                                 |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                                      |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 01/User Story 02                                            |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент перешел на вкладку главного меню                                |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android                           |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                                |

| Шаги воспроизведения | дейстивие пользователя                           | Ожидаемый результат                                                                                                    |
| -------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------- |
| шаг 1                | Выбрать вкладку главного меню ресторана          | Отобразить все категории меню                                                                                          |
| шаг 2                | Выбрать категорию бургеры                        | Отобразить список бургеров из категории бургеры<br>с кратким описанием и ценой                                         |
| шаг 3                | Выбрать бургер №1                                | Отобразить подробное описание бургера №1<br>(Состав, калории)                                                          |
| шаг 4                | Добавить бургер №1 в корзирну (нажать на кнопку) | Отобразить подтверждение об успешном добавлении блюда<br>Отобразить счетчик кол-ва позиций и их общую стоимость заказа |
| Постусловие          | Очистить корзину и завершить сессию           |

| ID                                                                         | UNHP_US1/2                                                             |
| -------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Название                                                                   | Неуспешное добавление блюда в корзину                                  |
| Краткое описание                                                           | Отображение информации о позиции и успешное добавление блюда в корзину |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                                 |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                                      |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 01/User Story 02                                            |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент перешел на вкладку главного меню                                |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android                           |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                                |

| Шаги воспроизведения | дейстивие пользователя                           | Ожидаемый результат                                                            |
| -------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------ |
| шаг 1                | Выбрать вкладку главного меню ресторана          | Отобразить все категории меню                                                  |
| шаг 2                | Выбрать категорию бургеры                        | Отобразить список бургеров из категории бургеры<br>с кратким описанием и ценой |
| шаг 3                | Выбрать бургер №1                                | Отобразить подробное описание бургера №1<br>(Состав, калории)                  |
| шаг 4                | Добавить бургер №1 в корзирну (нажать на кнопку) | Кнопка добавить в корзину неактивна                                            |
| Постусловие          | Завершить сессию                                 |


US 03 Я как клиент
ХОЧУ оплатить заказа
ЧТОБЫ получить еду

US 04 Я как клиент
ХОЧУ получить чек с номером заказа
ЧТОБЫ узнать готовность заказа и получить его после готовности показав чек

US 07 Я как клиент
ХОЧУ выбрать способ оплаты заказа
ЧТОБЫ оплатить заказ удобным способом

| ID                                                                         | HP_US3/4/7_1                                              |
| -------------------------------------------------------------------------- | --------------------------------------------------------- |
| Название                                                                   | Успешная оплата заказа картой и получение чека            |
| Краткое описание                                                           | Осуществить оплату и получить чек с информацией по заказу |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                    |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                         |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 03/User Story 04/ User Story 07                |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент сформировал заказ и перешел на страницу оплаты     |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android              |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                   |

| Шаги воспроизведения | дейстивие пользователя                                                                                                                | Ожидаемый результат                                                                                                         |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| шаг 1                | Перейти на страницу оплаты                                                                                                            | Отобразить выбор способа оплаты                                                                                             |
| шаг 2                | Выбрать оплату картой                                                                                                                 | Отобразить поля ввода для оплаты картой<br>(номер карты, месяц/год, cvv, фамилию и имя)                                     |
| шаг 3                | Ввести реквизиты банковской карты<br><br>Номер карты: 5555 5555 5555 5555<br>месяц/год: 12/29<br>cvv: 555<br>фамилию и имя: TEST TEST | Отобразить тип карты (МИР)                                                                                                  |
| шаг 4                | Оплатить заказ (нажать на кнопку)                                                                                                     | Запросить код подтверждения через СМС                                                                                       |
| шаг 5                | Подтвердить оплату (ввести код)                                                                                                       | Отобразить уведомление об успешной оплате<br>Отправить чек на email<br>Распечатать чек если заказ был сделан через терминал |
| Постусловие          | Завершить сессию                                                                                                                      |


| ID                                                                         | HP_US3/4/7_2                                                        |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Название                                                                   | Успешная оплата заказа наличными и получение чека                   |
| Краткое описание                                                           | Осуществить оплату наличными и получить чек с информацией по заказу |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                              |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                                   |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 03/User Story 04/ User Story 07                          |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент сформировал заказ и перешел на страницу оплаты               |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android                        |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                             |

| Шаги воспроизведения | дейстивие пользователя                       | Ожидаемый результат                                                               |
| -------------------- | -------------------------------------------- | --------------------------------------------------------------------------------- |
| шаг 1                | Перейти на страницу оплаты                   | Отобразить выбор способа оплаты                                                   |
| шаг 2                | Выбрать оплату наличными                     | Получить QR-код для оплаты заказа<br>Отобразить номер кассы для внесения наличных |
| шаг 3                | Предъявить QR-код на кассе и внести наличные | Получить чек с информацией по заказу                                              |
| Постусловие          | Завершить сессию                             |

| ID                                                                         | UNHP_US3/7                                            |
| -------------------------------------------------------------------------- | ----------------------------------------------------- |
| Название                                                                   | Неуспешная оплата заказа картой                       |
| Краткое описание                                                           | Неудачная попытка оплаты картой                       |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                     |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 03 / User Story 07                         |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент сформировал заказ и перешел на страницу оплаты |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android          |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                               |

| Шаги воспроизведения | дейстивие пользователя                                                                                                                | Ожидаемый результат                                                                     |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| шаг 1                | Перейти на страницу оплаты                                                                                                            | Отобразить выбор способа оплаты                                                         |
| шаг 2                | Выбрать оплату картой                                                                                                                 | Отобразить поля ввода для оплаты картой<br>(номер карты, месяц/год, cvv, фамилию и имя) |
| шаг 3                | Ввести реквизиты банковской карты<br><br>Номер карты: 5555 5555 5555 5555<br>месяц/год: 12/29<br>cvv: 555<br>фамилию и имя: TEST TEST | Отобразить тип карты (МИР)                                                              |
| шаг 4                | Оплатить заказ (нажать на кнопку)                                                                                                     | Запросить код подтверждения через СМС                                                   |
| шаг 5                | Подтвердить оплату (ввести некорректный код)                                                                                          | Отобразить уведомление об ошибке<br>Предложить вернуться к выбору способа оплаты        |
| Постусловие          | Завершить сессию                                                                                                                      |


US 05 Я как клиент
ХОЧУ редактировать позиции в корзине
ЧТОБЫ скорректировать заказ


| ID                                                                         | HP_US5                                                                |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Название                                                                   | Успешное редактирование корзины                                       |
| Краткое описание                                                           | Отредактировать корзину, удалить и добавить позиции                   |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                                |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                                     |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 05                                                         |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент добавил позиции в корзину (Бургер №1 - 1 шт, Бургер №2 - 2 шт) |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android                          |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                               |

| Шаги воспроизведения | дейстивие пользователя                            | Ожидаемый результат                                                                                                                                       |
| -------------------- | ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| шаг 1                | Перейти в корзину                                 | Отобразить список позиций в корзине<br>Бургер №1 - 1 шт<br>Бургер №2 - 2 шт                                                                               |
| шаг 2                | Удалить Бургер №1 (-1)<br>Добавить Бургер №2 (+1) | Удалить позицию Бургер №1 из заказа<br>Добавить позицию Бургер №2, отобразить кол-во = 3 шт<br>Отобразить новую стоимость заказа                          |
| шаг 3                | Добавить максимальное кол-во Бургер №2 (+7)       | Добавить позиции Бургер №2, отобразить кол-во = 10 шт<br>Кнопка добавления позиции в корзину для Бургер №2 неактивна<br>Отобразить новую стоимость заказа |
| Постусловие          | Отчистить корзину, завершить сессию               |


US 06 Я как клиент
ХОЧУ выбрать способ получения заказа
ЧТОБЫ забрать заказ удобным способом

| ID                                                                         | HP_US6                                                                |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Название                                                                   | Успешный выбор способа получения заказа - в ресторане                 |
| Краткое описание                                                           | Выбрать способ получения заказа                                       |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                                |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                                     |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 06                                                         |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент сформировал заказ и перешел на вкладку выбора получения заказа |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android                          |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                               |

| Шаги воспроизведения | дейстивие пользователя                        | Ожидаемый результат                                           |
| -------------------- | --------------------------------------------- | ------------------------------------------------------------- |
| шаг 1                | Перейти на вкладку выбора получения заказа    | Отобразить варианты получения заказа<br>с собой / в ресторане |
| шаг 2                | Выбрать способ получения заказа - в ресторане | Перейти на страницу оплаты заказа                             |
| Постусловие          | Отчистить корзину, завершить сессию           |


| ID                                                                         | UNHP_US6                                                                                                |
| -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Название                                                                   | Неуспешный выбор способа получения заказа - с собой                                                     |
| Краткое описание                                                           | Выбрать способ получения заказа с собой                                                                 |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                                                                  |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                                                                       |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 06                                                                                           |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент сформировал заказ (добавил в корзину XXL ICE CREAM) и перешел на вкладку выбора получения заказа |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android                                                            |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                                                                 |

| Шаги воспроизведения | дейстивие пользователя                     | Ожидаемый результат                                                                                                             |
| -------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| шаг 1                | Перейти на вкладку выбора получения заказа | Отобразить варианты получения заказа<br>с собой / в ресторане                                                                   |
| шаг 2                | Выбрать способ получения заказа - с собой  | Отобразить уведомление, что<br>блюдо XXL ICE CREAM подается только в ресторане<br>и предложить изменить способ получения заказа |
| Постусловие          | Отчистить корзину, завершить сессию        |


US 08 Я как клиент
ХОЧУ зарегистрироваться в программе лояльности
ЧТОБЫ копить и списывать бонусные баллы при оплате заказа

| ID                                                                         | HP_US8                                                                                  |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Название                                                                   | Успешная регистрация клиента в программе лояльности, проверка введенных данных пройдена |
| Краткое описание                                                           | Зарегистрировать в программе лояльности                                                 |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                                                  |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                                                       |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 08                                                                           |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент не зарегистрирован в системе                                                     |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android                                            |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                                                 |

| Шаги воспроизведения | дейстивие пользователя                                                                                                          | Ожидаемый результат                                                                                                                                                                            |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| шаг 1                | Перейти на вкладку регистрации в программе лояльности                                                                           | Отобразить поля ввода для регистрации<br><br>ФИО <br>Телефон<br>EMAIL<br>Дата рождения                                                                                                         |
| шаг 2                | Ввести корректные данные<br><br>ФИО: TEST TEST TEST<br>Телефон: 88005553555<br>EMAIL: test@mail.ru<br>Дата рождения: 1900-01-01 | Введены корректные данные                                                                                                                                                                      |
| шаг 3                | Подтвердить ввод (нажать кнопку Зарегистрировать)                                                                               | Направить код верификации на почту и номер телефона<br>Отобразить окно для ввода кода                                                                                                          |
| шаг 4                | Ввести код верификации                                                                                                          | Успешная регистрация<br>Клиент зарегистрирован в системе:<br>1) Появилась запись о клиенте в списке<br>зарегистрированных клиентов<br>2) На экране появилось сообщение об успешной регистрации |
| Постусловие          | Удалить регистрацию клиента: удалить запись о<br>регистрации клиента в таблице зарегистрированных                               |

| ID                                                                         | UNHP_US8                                                                          |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Название                                                                   | Неуспешная регистрация клиента в программе лояльности, клиент уже зарегистрирован |
| Краткое описание                                                           | При регистрации использовать номер, который уже есть в системе                    |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                                            |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                                                 |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 08                                                                     |
| Предусловия (что и как подготовлено к воспроизведению)                     | Номер зарегистрирован в системе                                                   |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android                                      |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                                                           |

| Шаги воспроизведения | дейстивие пользователя                                                                                                          | Ожидаемый результат                                                                                    |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| шаг 1                | Перейти на вкладку регистрации в программе лояльности                                                                           | Отобразить поля ввода для регистрации<br><br>ФИО <br>Телефон<br>EMAIL<br>Дата рождения                 |
| шаг 2                | Ввести корректные данные<br><br>ФИО: TEST TEST TEST<br>Телефон: 88888888888<br>EMAIL: test@mail.ru<br>Дата рождения: 1900-01-01 | Введены корректные данные                                                                              |
| шаг 3                | Подтвердить ввод (нажать кнопку Зарегистрировать)                                                                               | система отражает сообщение о том, что пользователь<br>уже зарегистрирован и предлагает сбросить пароль |
| Постусловие          | Завершить сессию                                                                                                                |


US 09 Я как клиент
ХОЧУ обратиться к менеджеру
ЧТОБЫ получить ответы на вопросы

| ID                                                                         | HP_US9                                       |
| -------------------------------------------------------------------------- | -------------------------------------------- |
| Название                                                                   | Успешная отправка обращения менеджеру        |
| Краткое описание                                                           | Отправка обращения менеджеру                 |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент       |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)            |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 09                                |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент зарегистрирован в системе             |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                      |

| Шаги воспроизведения | дейстивие пользователя                                                                 | Ожидаемый результат                                                                         |
| -------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| шаг 1                | Перейти на вкладку создания обращений                                                  | Отобразить список категорий обращений<br><br>Вопрос по оплате<br>Вопрос по заказу<br>Другое |
| шаг 2                | Выбрать "другое",<br>написать комментарий: Test<br>Отправить обращение нажав на кнопку | система отображает номер обращения и примерное время ответа менеджера                       |
| Постусловие          | Удалить обращение клиента из системы                                                   |


US 10 Я как клиент
ХОЧУ отменить заказ
ЧТОБЫ вернуть свои деньги

| ID                                                                         | HP_US10                                               |
| -------------------------------------------------------------------------- | ----------------------------------------------------- |
| Название                                                                   | Успешная отправка заявки на возврат средств           |
| Краткое описание                                                           | Отправка заявки на возврат средств                    |
| Вышестоящая фича / доработка / задача                                      | Основная задача: MVP User Story Клиент                |
| Автор                                                                      | Системный аналитик (Матвеев Д.О.)                     |
| Где описана постановка (ссылка, версия, название документа/раздела)        | User Story 10                                         |
| Предусловия (что и как подготовлено к воспроизведению)                     | Клиент зарегистрирован в системе и совершил заказ<br> |
| Окружение (ОС, браузер, разрешение экрана, скорость сети, доступность GPS) | Терминал в ресторане, приложение IOS/Android          |
| Приоритет (влияет на порядок обработки)                                    | Высокий                                               |

| Шаги воспроизведения | дейстивие пользователя                                                                                                        | Ожидаемый результат                                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| шаг 1                | Перейти на вкладку возврата средств                                                                                           | Система предлагает выбрать причины возврата<br><br>Перепутали заказ<br>Не получил заказ<br>Что-то не так с блюдом |
| шаг 2                | Выбрать "Что-то не так с блюдом"<br>Указать id заказа: 0001<br>написать комментарий: Test<br>Отправить заявку нажав на кнопку | Система отображает уведомление об успешном возврате и сроки возврата средств                                      |
| Постусловие          | Удалить заявку клиента из системы                                                                                             |