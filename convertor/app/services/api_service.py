from typing import Dict, Any
import requests
import logging

from requests import RequestException

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SendToServer:
    def __init__(self, config: Dict):
        required_keys = ['api_url']
        missing = [k for k in required_keys if k not in config]

        if missing:
            raise ValueError(f"В конфиге API отсутствуют обязательные ключи: {missing}")

        self.base_url = config['api_url'].rstrip('/')
        self.timeout = config.get('timeout', 30)
        self.endpoint = config.get('endpoint', '/api/violations')
        self.session = requests.Session()
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

    def send_violation(self, json_data: str) -> bool:
        """Отправка уже сериализованного JSON"""
        try:
            response = requests.post(
                self.endpoint,
                data=json_data,
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout
            )
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"API request failed: {e}")
            return False
    # def send_violation(self, data: dict) -> bool:
    #     """Отправка данных на сервер"""
    #     try:
    #         # Проверка минимально необходимых полей
    #         required_fields = ['id', 'timestamp']  # Пример базовых требований
    #         for field in required_fields:
    #             if field not in data:
    #                 raise ValueError(f"Отсутствует обязательное поле: {field}")
    #
    #         response = self.session.post(
    #             f"{self.base_url}{self.endpoint}",
    #             json=data,  # Отправляем как есть
    #             headers=self.headers,
    #             timeout=self.timeout
    #         )
    #
    #         # 5. Обработка специфичных кодов ответа
    #         if response.status_code == 200:
    #             logger.info(f"Успешная отправка. Ответ: {response.text}")
    #             return True
    #         elif response.status_code == 400:
    #             logger.error(f"Ошибка 400: Некорректный запрос. Подробности: {response.text}")
    #
    #
    #
    #         elif 500 <= response.status_code < 600:
    #             logger.error(f"Ошибка {response.status_code}: Проблема на сервере")
    #         else:
    #             logger.error(f"Неизвестная ошибка. Статус: {response.status_code}, Ответ: {response.text}")
    #
    #         return False
    #
    #     except requests.exceptions.Timeout:
    #         logger.error("Таймаут при подключении к серверу")
    #         return False
    #     except requests.exceptions.TooManyRedirects:
    #         logger.error("Слишком много редиректов")
    #         return False
    #     except requests.exceptions.RequestException as e:
    #         logger.error(f"Критическая ошибка при отправке: {str(e)}", exc_info=True)
    #         return False