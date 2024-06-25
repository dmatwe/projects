# Сохраните файл и запустите приложение с помощью `uvicorn` (в консоли): uvicorn main:app --reload

# Документацию можете посмотреть по ссылкам http://127.0.0.1:8000/docs и http://127.0.0.1:8000/redoc (альтернативная).

***GET Hello World***

```python 

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

```


***GET HTML***


```python 

from fastapi import FastAPI
from fastapi.responses import FileResponse
app = FastAPI()

@app.get('/')
def root():
    return FileResponse("index.html")

```

```html 

<!DOCTYPE html>

<html lang="ru">
<head>

<meta charset="UTF-8">

<title> Пример простой страницы html</title>
</head>

<body>

Я НЕРЕАЛЬНО КРУТ И МОЙ РЕСПЕКТ БЕЗ МЕРЫ :)
</body>

</html>

```

***Добавление дополнительных маршрутов / ***

***http://127.0.0.1:8000/custom***

```python 

# новый роут
@app.get("/custom")
def read_custom_message():
    return {"message": "This is a custom message!"}
```