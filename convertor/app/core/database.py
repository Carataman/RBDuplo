from typing import Dict, List
import psycopg2
from psycopg2.extras import DictCursor
import logging

logger = logging.getLogger(__name__)


class DatabaseConnect:
    def __init__(self, config: Dict):
        """
        Args:
            config: Полный загруженный конфиг (должен содержать 'database' и другие параметры)
        """
        self.config = config  # Сохраняем весь конфиг
        self.connection = self._connect()

    def _connect(self):
        """Подключение к PostgreSQL"""
        try:
            if 'database' not in self.config:
                raise ValueError("Отсутствует конфигурация БД")

            conn = psycopg2.connect(**self.config['database'])
            logger.info("Подключение к PostgreSQL успешно!")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise

    def fetch_data(self) -> List[Dict]:
        """Получает данные из БД с учётом start_date из конфига"""
        if not self.connection:
            logger.error("Нет подключения к БД")
            return []

        try:
            # Получаем start_date из общего конфига
            start_date = self.config.get('start_date', '1970-01-01')

            with self.connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("""
                    SELECT id, file_path, timestamp 
                    FROM main.materials 
                    WHERE timestamp >= %s 
                    ORDER BY timestamp ASC 
                    LIMIT 1;
                """, (start_date,))
                return cursor.fetchall()
        except psycopg2.Error as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            return []
    def close(self):
        """Закрытие соединения с базой данных"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Соединение с PostgreSQL закрыто")
            except Exception as e:
                logger.error(f"Ошибка при закрытии соединения: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


