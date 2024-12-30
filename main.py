import asyncio
import aiomysql
import logging
import time  # Для измерения времени обработки
from sensors_config import sensors
import tinytuya

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
    datefmt="%H:%M:%S"
)

# Словарь для отслеживания ошибок
error_count = {}
MAX_ERRORS = 3  # Максимальное количество ошибок до пометки как проблемный датчик

DB_CONFIG = {
    'host': "10.13.0.93",
    'user': 'kt_user',
    'password': "ktnis2009",
    'db': "kt_temp_sensor"
}

# Время ожидания ответа от датчика (в секундах)
TIMEOUT = 10

async def get_sensor_data_with_timeout(sensor_config):
    """
    Получить данные с датчика с ограничением по времени.
    """
    device = tinytuya.OutletDevice(
        sensor_config['device_id'],
        sensor_config['ip_address'],
        sensor_config['local_key']
    )
    device.set_version(3.3)
    try:
        data = await asyncio.wait_for(
            asyncio.to_thread(device.status),
            timeout=TIMEOUT
        )
        return data
    except asyncio.TimeoutError:
        logging.warning(f"[Датчик {sensor_config['sensor_id']:3}] - Не ответил за {TIMEOUT} сек.")
        return None
    except Exception as e:
        logging.error(f"[Датчик {sensor_config['sensor_id']:3}] - Ошибка: {e}")
        # Увеличить счетчик ошибок
        error_count[sensor_config['sensor_id']] = error_count.get(sensor_config['sensor_id'], 0) + 1
        if error_count[sensor_config['sensor_id']] >= MAX_ERRORS:
            logging.warning(f"[Датчик {sensor_config['sensor_id']:3}] - Проблемный датчик (ошибок: {error_count[sensor_config['sensor_id']]})")
        return None, None


async def handle_sensor(sensor_config):
    """
    Обработать данные одного датчика.
    """
    start_time = time.time()  # Запуск таймера
    data = await get_sensor_data_with_timeout(sensor_config)
    elapsed_time = time.time() - start_time  # Измерение времени
    if data:
        temperature = data['dps'].get('27') / 10
        humidity = data['dps'].get('46')
        logging.info(
            f"[Датчик {sensor_config['sensor_id']:3}] - Данные получены: T={temperature:.1f}, H={humidity:.1f}. "
            f"Время обработки: {elapsed_time:.2f} сек."
        )
        return sensor_config['sensor_id'], temperature, humidity, elapsed_time
    logging.info(f"[Датчик {sensor_config['sensor_id']:3}] - Пропущен. Время: {elapsed_time:.2f} сек.")
    logging.debug(f"[Датчик {sensor_config['sensor_id']:3}] - Запрос отправлен.")
    return None


async def process_all_sensors(pool):
    """
    Обработать данные со всех датчиков одновременно.
    """
    logging.info("=== Запрос для всех датчиков начат ===")
    start_time = time.time() 
    tasks = [handle_sensor(sensor) for sensor in sensors]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Фильтрация и сортировка результатов
    valid_results = [result for result in results if result and not isinstance(result, Exception)]
    valid_results.sort(key=lambda x: x[0])  # Сортировка по sensor_id

    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            for result in valid_results:
                sensor_id, temperature, humidity, elapsed_time = result
                query = "INSERT INTO sensor_readings (sensor_id, temperature, humidity) VALUES (%s, %s, %s)"
                await cursor.execute(query, (sensor_id, temperature, humidity))
                logging.info(
                    f"[Сохранение] - Данные для sensor_id={sensor_id} сохранены в БД. "
                    f"Время обработки: {elapsed_time:.2f} сек."
                )
            await connection.commit()
    logging.info("=== Все данные успешно сохранены в базу данных ===")


async def main():
    """
    Главная функция обработки всех датчиков.
    """
    pool = await aiomysql.create_pool(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        db=DB_CONFIG['db'],
        minsize=5,
        maxsize=10
    )
    try:
        while True:
            start_time = time.time()  # Запуск таймера перед обработкой всех датчиков
            logging.info("=== Начало обработки всех датчиков ===")
            await process_all_sensors(pool)
            total_time = time.time() - start_time  # Общее время обработки всех датчиков
            logging.info(f"=== Обработка завершена. Время: {total_time:.2f} секунд ===")
            logging.info("Ожидание 10 секунд перед следующим опросом.\n")
            await asyncio.sleep(5)
    finally:
        pool.close()
        await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
