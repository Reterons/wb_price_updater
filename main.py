import schedule
import time
import logging
from filelock import FileLock
from app import main as app_main

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)

def run_script():
    """Запускает основной скрипт напрямую через импорт"""
    logging.info("Запуск основного скрипта...")
    try:
        app_main()  # Вызываем функцию main() из app.py
        logging.info("Скрипт успешно завершен")
    except Exception as e:
        logging.error(f"Ошибка при выполнении скрипта: {str(e)}")

def main():
    schedule.every(8).hours.do(run_script)
    
    # Первый запуск сразу
    run_script()
    
    logging.info("Планировщик запущен. Скрипт будет выполняться каждые 8 часов.")
    
    while True:
        schedule.run_pending()
        time.sleep(14400)

if __name__ == "__main__":
    main()
