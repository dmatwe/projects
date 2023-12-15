 **Проектирование ресурсной модели Open API**
 
 - [Спецификация](https://app.swaggerhub.com/apis/DenisMatveev/order/1.0.0-oas3)

**Контракт метода POST /orders/{orderId}/repeat:**

 | Краткое описание        | Повторение заказа                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Тип запроса             | post                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| Адрес                   | /orders/{orderId}/repeat                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Структура запроса       | Заголовки:<br>content: application/json<br>Пример:<br>POST /orders/id8282/repeat                                                                                                                                                                                                                                                                                                                                                                               |
| Структура ответа        | Структура при успехе:<br>При успешном оформлении заказа возвращается:<br>\- order_id: ID заказа (string 250)<br>\- message: сообщение о статусе заказа (string 250)<br>\- time: время оформления заказа (string 250)<br><br>Структура при ошибке:<br>\- HTTP-код ответа integer<br>\- краткое описание причины ошибки - string(250)<br>\- пример:<br><br>{<br>"code": 400,<br>"message":<br>"Неверный запрос, например, неполные или некорректные данные"<br>} |
| Список возможных ошибок | \- 400 : Неверный запрос, например, неполные или некорректные данные<br>\- 401: Пользователь не авторизован<br>\- 403: Пользователь не имеет права на оформление заказа<br>\- 404: Заказ не найден<br>\- 500: Внутренняя ошибка сервера                                                                                                                                                                                                                        |

```json
---
openapi: 3.0.0
info:
  title: Dot Eats
  description: API для заказа блюд через приложение
  version: 1.0.0-oas3
servers:
- url: https://virtserver.swaggerhub.com/cottoffeus/Dot_Eats/1
  description: SwaggerHub API Auto Mocking
paths:
  /auth/login:
    post:
      tags:
      - Технические методы
      summary: Аутентификация пользователя
      description: "Позволяет пользователю войти в систему, используя логин и пароль."
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/auth_login_body'
        required: true
      responses:
        "200":
          description: Успешная аутентификация
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/inline_response_200'
        "401":
          description: Неверные учетные данные
        "500":
          description: Внутренняя ошибка сервера
  /dishes:
    get:
      tags:
      - Просмотр блюд
      summary: Получение списка блюд
      parameters:
      - name: cusine
        in: query
        description: Фильтр по типу кухни
        required: false
        style: form
        explode: true
        schema:
          type: string
          enum:
          - russian
          - italian
          - georgian
          - asian
      - name: category
        in: query
        description: Фильтр по категории блюда
        required: false
        style: form
        explode: true
        schema:
          type: string
          enum:
          - starter
          - mainCourse
          - dessert
          - soup
          - salad
          - sideDish
          - beverage
      - name: raiting
        in: query
        description: Фильтр по рейтингу блюда
        required: false
        style: form
        explode: true
        schema:
          type: string
          enum:
          - Превосходно
          - Выше Ожидаемого
          - Удовлетворительно
          - Слабо
          - Отвратительно
          - Тролль
      - name: price_min
        in: query
        description: Минимальная цена блюда
        required: false
        style: form
        explode: true
        schema:
          type: number
      - name: price_max
        in: query
        description: Максимальная цена блюда
        required: false
        style: form
        explode: true
        schema:
          type: number
      - name: page
        in: query
        description: Номер страницы для пагинации
        required: false
        style: form
        explode: true
        schema:
          type: integer
          default: 1
      - name: pageSize
        in: query
        description: Количество блюд на странице
        required: false
        style: form
        explode: true
        schema:
          type: integer
          default: 10
      responses:
        "200":
          description: Список блюд
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/inline_response_200_1'
      security:
      - bearerAuth: []
  /dishes/{dishId}:
    get:
      tags:
      - Просмотр блюд
      summary: Получение деталей блюда
      parameters:
      - name: dishId
        in: path
        required: true
        style: simple
        explode: false
        schema:
          type: string
      responses:
        "200":
          description: Детали блюда
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/DishExtended'
      security:
      - bearerAuth: []
  /cart/items:
    get:
      tags:
      - Работа с заказом и корзиной
      summary: Получение товаров в корзине
      description: "Возвращает список товаров, находящихся в корзине пользователя."
      operationId: getCartItems
      responses:
        "200":
          description: Список товаров в корзине
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/CartItemsResponse'
        "401":
          description: "Неавторизован, пользователь не аутентифицирован"
        "404":
          description: Корзина не найдена
        "500":
          description: Внутренняя ошибка сервера
    post:
      tags:
      - Работа с заказом и корзиной
      summary: Добавление блюда в корзину
      description: Добавляет указанное блюдо в корзину пользователя.
      operationId: addItemToCart
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/AddItemRequest'
        required: true
      responses:
        "201":
          description: Блюдо успешно добавлено в корзину
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/AddItemResponse'
        "400":
          description: "Неверный запрос, например, отсутствует идентификатор блюда или количество"
        "401":
          description: Пользователь не авторизован
        "500":
          description: Внутренняя ошибка сервера
  /cart/items/{itemId}:
    put:
      tags:
      - Работа с заказом и корзиной
      summary: Редактирование блюда в корзине
      description: Обновляет количество указанного блюда в корзине пользователя.
      operationId: updateCartItem
      parameters:
      - name: itemId
        in: path
        description: Уникальный идентификатор блюда
        required: true
        style: simple
        explode: false
        schema:
          type: string
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/UpdateItemRequest'
        required: true
      responses:
        "200":
          description: Информация о блюде успешно обновлена
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UpdateItemResponse'
        "400":
          description: "Неверный запрос, например, отсутствует количество"
        "401":
          description: Пользователь не авторизован
        "404":
          description: Блюдо не найдено в корзине
        "500":
          description: Внутренняя ошибка сервера
  /cart/checkout:
    post:
      tags:
      - Работа с заказом и корзиной
      summary: Оформление заказа
      description: Оформляет заказ на основе текущего содержимого корзины пользователя.
      operationId: checkoutCart
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/CheckoutRequest'
        required: true
      responses:
        "200":
          description: Заказ успешно оформлен
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/CheckoutResponse'
        "400":
          description: "Неверный запрос, например, неполные или некорректные данные"
        "401":
          description: Пользователь не авторизован
        "403":
          description: Пользователь не имеет права на оформление заказа
        "404":
          description: Корзина не найдена
        "500":
          description: Внутренняя ошибка сервера
      security:
      - bearerAuth: []
  /orders/{orderId}/payment:
    post:
      tags:
      - Работа с заказом и корзиной
      summary: Инициация оплаты заказа
      description: Инициирует процесс оплаты для указанного заказа и возвращает ссылку для перенаправления на платежный шлюз.
      operationId: initiatePayment
      parameters:
      - name: orderId
        in: path
        description: Уникальный идентификатор заказа
        required: true
        style: simple
        explode: false
        schema:
          type: string
      responses:
        "303":
          description: Перенаправление на платежный шлюз
          headers:
            Location:
              description: URL платежного шлюза
              style: simple
              explode: false
              schema:
                type: string
              example: https://help.reg.ru/support/hosting/redirekty/kak-sdelat-redirekt-na-html
        "400":
          description: "Неверный запрос, например, неполные или некорректные данные"
        "401":
          description: Пользователь не авторизован
        "403":
          description: Пользователь не имеет права на оплату данного заказа
        "404":
          description: Заказ не найден
        "500":
          description: Внутренняя ошибка сервера
      security:
      - bearerAuth: []
  /orders/{orderID}/status:
    get:
      tags:
      - Получение статуса заказа
      parameters:
      - name: orderID
        in: path
        description: Уникальный идентификатор заказа
        required: true
        style: simple
        explode: false
        schema:
          type: string
      responses:
        "200":
          description: Статус получен
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/inline_response_200_2'
        "400":
          description: "Неверный запрос, например, неполные или некорректные данные"
        "401":
          description: Пользователь не авторизован
        "403":
          description: Пользователь не имеет права на оформление заказа
        "404":
          description: Корзина не найдена
        "500":
          description: Внутренняя ошибка сервера
  /dishes/{dishId}/reviews:
    post:
      tags:
      - Добавление отзыва о заказанном блюде
      parameters:
      - name: dishId
        in: path
        description: Уникальный идентификатор блюда
        required: true
        style: simple
        explode: false
        schema:
          type: string
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/dishId_reviews_body'
        required: true
      responses:
        "200":
          description: Отзыв успешно добавлен
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/AddReviews'
        "400":
          description: "Неверный запрос, например, отсутствует идентификатор блюда или количество"
        "401":
          description: Пользователь не авторизован
        "500":
          description: Внутренняя ошибка сервера
  /orders/{orderId}/repeat:
    post:
      tags:
      - Работа с заказом и корзиной
      summary: Повторение заказа
      operationId: repeatOrder
      parameters:
      - name: orderId
        in: path
        description: Уникальный идентификатор заказа
        required: true
        style: simple
        explode: false
        schema:
          type: string
      responses:
        "200":
          description: Заказ успешно оформлен
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/CheckoutResponse'
        "400":
          description: "Неверный запрос, например, неполные или некорректные данные"
        "401":
          description: Пользователь не авторизован
        "403":
          description: Пользователь не имеет права на оформление заказа
        "404":
          description: Заказ не найден
        "500":
          description: Внутренняя ошибка сервера
      security:
      - bearerAuth: []
  /orders:
    get:
      tags:
      - Работа с заказом и корзиной
      summary: Метод получения истории заказов
      description: "Метод предназначен для получения списка заказов, сохраненных в БД."
      operationId: getOrders
      responses:
        "200":
          description: Список заказов
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/CheckoutResponse'
components:
  schemas:
    AddItemRequest:
      required:
      - itemId
      - quantity
      type: object
      properties:
        itemId:
          type: string
          description: Уникальный идентификатор блюда
        quantity:
          type: integer
          description: Количество добавляемых блюд
    AddItemResponse:
      type: object
      properties:
        message:
          type: string
          description: Сообщение о результате добавления
        cartTotal:
          type: integer
          description: Общее количество блюд в корзине
    CartItemsResponse:
      type: object
      properties:
        userId:
          type: string
          description: Уникальный идентификатор пользователя
        items:
          type: array
          items:
            $ref: '#/components/schemas/CartItem'
        totalPrice:
          type: number
          description: Общая стоимость всех товаров в корзине
          format: float
    CartItem:
      type: object
      properties:
        itemId:
          type: string
          description: Уникальный идентификатор товара
        name:
          type: string
          description: Название товара
        quantity:
          type: integer
          description: Количество товара в корзине
        price:
          type: number
          description: Цена за единицу товара
          format: float
    UpdateItemRequest:
      required:
      - quantity
      type: object
      properties:
        quantity:
          type: integer
          description: Новое количество блюда
    UpdateItemResponse:
      type: object
      properties:
        message:
          type: string
          description: Сообщение о результате обновления
        updatedItem:
          $ref: '#/components/schemas/CartItem'
    CheckoutRequest:
      type: object
      properties:
        address:
          type: string
          description: Адрес доставки
        paymentMethod:
          type: string
          description: Способ оплаты
          enum:
          - card
          - ewallet
          - sbp
          - tpay
          - spay
          - cash
    CheckoutResponse:
      type: object
      properties:
        orderId:
          type: string
          description: Уникальный идентификатор заказа
        message:
          type: string
          description: Сообщение о статусе заказа
        time:
          type: string
          description: Время заказа с точностью до миллисекунд
          example: 1996-12-20T00:39:57.870Z
    Dish:
      type: object
      properties:
        id:
          type: string
        name:
          type: string
        imageLink:
          type: string
          format: url
          example: https://static.wikia.nocookie.net/harrypotter/images/c/c5/Pumpkin_Juice_WWHP.jpg/revision/latest/scale-to-width-down/1000?cb=20110529053444
    DishExtended:
      allOf:
      - $ref: '#/components/schemas/Dish'
      - type: object
        properties:
          description:
            type: string
          cusine:
            type: string
            enum:
            - russian
            - italian
            - georgian
            - asian
          category:
            type: string
            enum:
            - starter
            - mainCourse
            - dessert
            - soup
            - salad
            - sideDish
            - beverage
          raiting:
            type: string
            enum:
            - Превосходно
            - Выше Ожидаемого
            - Удовлетворительно
            - Слабо
            - Отвратительно
            - Тролль
          price:
            type: number
    AddReviews:
      required:
      - reviewId
      - status
      - textReview
      type: object
      properties:
        textReview:
          type: string
        reviewId:
          type: string
        status:
          type: string
    auth_login_body:
      required:
      - password
      - username
      type: object
      properties:
        username:
          type: string
          description: Имя пользователя
        password:
          type: string
          description: Пароль пользователя
          format: password
    inline_response_200:
      type: object
      properties:
        token:
          type: string
          description: Токен доступа
          format: JWT
          example: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkhhcnJ5IFBvdHRlciIsImlhdCI6MTUxNjIzOTAyMn0.Jkr1iBrO9-cr7uZRYhvE8BuSXIfuPnQWuZBNBE60t14
    inline_response_200_1:
      type: object
      properties:
        total:
          type: integer
          description: "Общее количество блюд, соответствующих фильтрам"
        dishes:
          type: array
          items:
            $ref: '#/components/schemas/Dish'
        page:
          type: integer
          description: Текущая страница
        pageSize:
          type: integer
          description: Количество блюд на странице
    inline_response_200_2:
      type: object
      properties:
        status:
          type: string
    dishId_reviews_body:
      type: object
      properties:
        textReview:
          type: string
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT

```
