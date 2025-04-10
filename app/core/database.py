from typing import Dict, List
import psycopg2
from psycopg2.extras import DictCursor
import logging

logger = logging.getLogger(__name__)


class DatabaseConnect:
    def __init__(self, config: Dict, start_date: str = None):
        """
        Args:
            config: Должен содержать ключи:
                   dbname, user, password, host, port
        """
        if not config:
            raise ValueError("Не передана конфигурация БД")

        self.config = config
        self.connection = self._connect()
        self.start_date = start_date
        logger.info(start_date)
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

    # def get_new_violations(self) -> List[Dict]:
    #
    #     """Получает данные из БД с учётом start_date из конфига"""
    #     if not self.connection:
    #         logger.error("Нет подключения к БД")
    #         return []
    #
    #     try:
    #
    #         # Правильное получение start_date из вложенной структуры
    #
    #
    #         logger.info(f"Используется start_date: {self.start_date}")
    #
    #         with self.connection.cursor(cursor_factory=DictCursor) as cursor:
    #             cursor.execute("""
    #                 SELECT id, file_path, timestamp
    #                 FROM main.materials
    #                 WHERE (timestamp >= %s
    #                 ) AND (file_type = 1 OR file_type = 2) ORDER BY timestamp ASC
    #                 LIMIT 1;
    #             """, (self.start_date,))
    #
    #             result = cursor.fetchall()
    #             logger.info(f"Найдено нарушений: {len(result)}")
    #             return result
    #
    #     except psycopg2.Error as e:
    #         logger.error(f"Ошибка выполнения запроса: {e}")
    #         return []
    def get_new_violations(self) -> List[Dict]:
        """
        Получает данные из БД с учётом start_date из конфига.
        Возвращает список словарей с данными о нарушениях, включая оба файла.
        """
        if not self.connection:
            logger.error("Нет подключения к БД")
            return []

        try:
            logger.info(f"Используется start_date: {self.start_date}")

            with self.connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        m1.id as original_id,
                        m1.file_path as original_path,
                        m1.timestamp,
                        m1.target_id,
                        m1.event_id,
                        m2.id as fr_id,
                        m2.file_path as fr_path
                    FROM main.materials m1
                    LEFT JOIN main.materials m2 
                        ON m1.target_id = m2.target_id 
                        AND m1.event_id = m2.event_id
                        AND m2.file_type = 2
                    WHERE m1.timestamp >= %s
                        AND m1.file_type = 1
                    ORDER BY m1.timestamp ASC
                    LIMIT 1;
                """, (self.start_date,))

                rows = cursor.fetchall()
                violations = []

                for row in rows:
                    violation = {
                        'id': row['original_id'],
                        'timestamp': row['timestamp'],
                        'target_id': row['target_id'],
                        'event_id': row['event_id'],
                        'files': {
                            'original': {
                                'id': row['original_id'],
                                'path': row['original_path']
                            },
                            'fr': {
                                'id': row['fr_id'],
                                'path': row['fr_path']
                            } if row['fr_id'] else None
                        }
                    }
                    violations.append(violation)

                logger.info(f"Найдено нарушений: {len(violations)}")
                return violations

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


