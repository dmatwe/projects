# Сохраните файл и запустите приложение с помощью `uvicorn` (в консоли): uvicorn main:app --reload


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