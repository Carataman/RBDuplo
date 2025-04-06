from typing import Dict, List
import psycopg2
from psycopg2.extras import DictCursor
import logging

logger = logging.getLogger(__name__)


class DatabaseConnect:
    def __init__(self, config: Dict):
        """
        Args:
            config: Должен содержать ключи:
                   dbname, user, password, host, port
        """
        if not config:
            raise ValueError("Не передана конфигурация БД")

        self.config = config
        self.connection = self._connect()

    def _connect(self):
        """Подключение к PostgreSQL с проверкой параметров"""
        required_keys = {'dbname', 'user', 'password', 'host', 'port'}
        missing = required_keys - set(self.config.keys())

        if missing:
            raise ValueError(f"В конфиге БД отсутствуют ключи: {missing}")

        try:
            conn = psycopg2.connect(**self.config)
            logger.info("Успешное подключение к PostgreSQL")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Ошибка подключения: {e}")
            raise

    def get_new_violations(self) -> List[Dict]:
        """Получает данные из БД с учётом start_date из конфига"""
        if not self.connection:
            logger.error("Нет подключения к БД")
            return []

        try:
            # Правильное получение start_date из вложенной структуры
            start_date = self.config.get('processing', {}).get('start_date', '1970-01-01 00:00:00')

            logger.info(f"Используется start_date: {start_date}")

            with self.connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("""
                    SELECT id, file_path, timestamp 
                    FROM main.materials 
                    WHERE timestamp >= %s 
                    ORDER BY timestamp ASC 
                    LIMIT 1;
                """, (start_date,))

                result = cursor.fetchall()
                logger.info(f"Найдено нарушений: {len(result)}")
                return result

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


