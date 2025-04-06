from typing import Dict, Any
import requests
import logging
# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SendToServer:
    def __init__(self, config: Dict):
        """Инициализация клиента для отправки данных на сервер

        Args:
            config: Должен содержать:
                   - api_url: Базовый URL API
                   - timeout: Таймаут соединения (опционально)
                   - endpoint: Конечная точка API (опционально)
        """
        required_keys = ['api_url']
        missing = [k for k in required_keys if k not in config]

        if missing:
            raise ValueError(f"В конфиге API отсутствуют обязательные ключи: {missing}")

        self.base_url = config['api_url'].rstrip('/')
        self.timeout = config.get('timeout', 30)
        self.endpoint = config.get('endpoint', '/api/violations')
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def check_connection(self) -> bool:
        """Проверка доступности сервера"""
        try:
            response = requests.get(
                f"{self.base_url}/ping",
                timeout=5,
                headers=self.headers
            )
            return response.status_code == 200
        except RequestException as e:
            logger.error(f"Ошибка подключения к серверу: {str(e)}")
            return False

    def send_violation(self, data: Dict[str, Any]) -> bool:
        """Отправка данных о нарушении на сервер"""
        try:
            full_url = f"{self.base_url}{self.endpoint}"

            response = requests.post(
                full_url,
                json=data,
                headers=self.headers,
                timeout=self.timeout
            )

            if response.ok:
                logger.info(f"Данные успешно отправлены. Ответ сервера: {response.text}")
                return True
            else:
                logger.error(f"Ошибка сервера. Статус: {response.status_code}, Ответ: {response.text}")
                return False

        except RequestException as e:
            logger.error(f"Ошибка при отправке данных: {str(e)}")
            return False