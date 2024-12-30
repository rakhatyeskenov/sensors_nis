import asyncio
import logging
import json
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import aiomysql
from fastapi.staticfiles import StaticFiles
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "img")

# Конфигурация FastAPI
app = FastAPI()

app.mount("/img", StaticFiles(directory=STATIC_DIR), name="img")

print(os.getcwd())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    'host': "10.13.0.93",
    'user': 'kt_user',
    'password': "ktnis2009",
    'db': "kt_temp_sensor"
}

# Главная страница
@app.get("/", response_class=HTMLResponse)
async def get_index():
    template_path = os.path.join(BASE_DIR, "templates", "index.html")
    try:
        with open(template_path, "r", encoding="utf-8") as file:
            html_content = file.read()
        return HTMLResponse(content=html_content, status_code=200)
    except FileNotFoundError:
        logging.error(f"index.html not found at {template_path}")
        return HTMLResponse(content="<h1>index.html not found</h1>", status_code=404)


# WebSocket Endpoint
@app.websocket("/ws/data")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logging.info("WebSocket соединение установлено.")
    try:
        while True:
            # Получение данных из базы данных
            data = await fetch_latest_sensor_data()
            if data:
                # Логирование данных
                logging.info(f"Отправка данных клиенту WebSocket: {data}")

                # Отправка данных клиенту
                await websocket.send_text(json.dumps(data))
            await asyncio.sleep(0.5)  # Обновление каждые 2 секунды
    except Exception as e:
        logging.error(f"Ошибка WebSocket: {e}")
    finally:
        logging.info("WebSocket соединение закрыто.")

async def fetch_latest_sensor_data():
    """
    Извлечение последних данных с датчиков из базы данных.
    """
    data = {}
    try:
        # Подключение к базе данных
        pool = await aiomysql.create_pool(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            db=DB_CONFIG['db'],
            minsize=1,
            maxsize=5
        )
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                # Запрос для получения последних данных с датчиков
                # query = """
                #     SELECT sensor_id, temperature, humidity
                #     FROM sensor_readings
                #     ORDER BY timestamp DESC
                #     LIMIT 100;
                # """
                query = """
                    SELECT sensor_id, temperature, humidity
                    FROM sensor_readings
                    WHERE timestamp = (
                        SELECT MAX(timestamp)
                        FROM sensor_readings sr
                        WHERE sr.sensor_id = sensor_readings.sensor_id
                    )
                """
                await cursor.execute(query)
                rows = await cursor.fetchall()

                # Форматирование данных для отправки клиенту
                for row in rows:
                    sensor_id, temperature, humidity = row
                    data[sensor_id] = {
                        "temperature": temperature,
                        "humidity": humidity
                    }

        pool.close()
        await pool.wait_closed()
    except Exception as e:
        logging.error(f"Ошибка при извлечении данных из базы данных: {e}")
    return data

# Добавьте функцию process_sensor_data
def process_sensor_data(data):
    """
    Фильтрует данные для активных датчиков.
    """
    processed_data = {}
    for sensor_id, values in data.items():
        # Проверяем наличие данных и статус "активен"
        if values and values.get('temperature') is not None and values.get('humidity') is not None:
            processed_data[sensor_id] = values
    return processed_data


