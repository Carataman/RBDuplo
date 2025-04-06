from typing import Dict, Any
import requests
import logging
# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SendToServer:
    def _send_to_server(self, data: Dict[str, Any],) -> bool:
        """Отправляет данные на сервер"""
        try:
            response = requests.get(f"{self.api_url}/ping", timeout=5)
            if response.status_code != 200:
                logger.error(f"Сервер недоступен. Статус: {response.status_code}")
                return False

            response = requests.post(
                f"{self.api_url}/send",
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            if response.status_code == 200:
                try:
                    logger.info(f"Успешная отправка. Ответ: {response.json()}")

                except ValueError:
                    logger.info(f"Успешная отправка. Ответ: {response.text}")
                return True
            else:
                logger.error(f"Ошибка отправки. Статус: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Ошибка соединения с сервером: {e}")
            return False