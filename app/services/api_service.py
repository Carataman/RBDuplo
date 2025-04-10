from typing import Dict, Any, Optional
import requests
import logging
import json
from pathlib import Path
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class SendToServer:
    def __init__(self, config: Dict):
        required_keys = ['api_url']
        if missing := [k for k in required_keys if k not in config]:
            raise ValueError(f"Missing required config keys: {missing}")

        self.base_url = config['api_url'].rstrip('/')
        self.timeout = config.get('timeout', 30)
        self.endpoint = config.get('endpoint', '/api/violations')
        self.session = requests.Session()
        self.default_headers = {
            'Accept': 'application/json'
        }

    def send_violation(
            self,
            data: dict,
            file_path: Optional[str] = None
    ) -> bool:
        """Отправка нарушения с возможностью прикрепления файла"""
        try:
            url = f"{self.base_url}{self.endpoint}"
            files = {}
            headers = self.default_headers.copy()

            # Подготовка файла если есть
            if file_path:
                if not Path(file_path).exists():
                    raise FileNotFoundError(f"File {file_path} not found")

                files['file'] = (
                    Path(file_path).name,
                    open(file_path, 'rb'),
                    'image/jpeg'
                )
                # Для multipart запросов НЕ устанавливаем Content-Type вручную
                headers.pop('Content-Type', None)
            else:
                headers['Content-Type'] = 'application/json'

            # Логирование перед отправкой
            logger.debug(f"Sending to {url}")
            logger.debug(f"Headers: {headers}")
            logger.debug(f"Data: {json.dumps(data, indent=2, ensure_ascii=False)}")
            if files:
                logger.debug(f"Attaching file: {file_path}")

            response = self.session.post(
                url=url,
                #data=data if files else None,
                json=data if not files else None,
                files=files,
                headers=headers,
                timeout=self.timeout
            )

            return self._handle_response(response)

        except FileNotFoundError as e:
            logger.error(f"File error: {str(e)}")
            return False
        except RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}", exc_info=True)
            return False
        finally:
            # Закрываем файловый дескриптор если был открыт
            if files.get('file'):
                files['file'][1].close()

    def _handle_response(self, response: requests.Response) -> bool:
        """Обработка ответа сервера"""
        try:
            response_data = response.json()
        except json.JSONDecodeError:
            response_data = {'raw_response': response.text}

        log_msg = (
            f"Status: {response.status_code}\n"
            f"Headers: {response.headers}\n"
            f"Body: {json.dumps(response_data, indent=2)}"
        )

        if response.ok:
            logger.info(f"Successfully sent\n{log_msg}")
            return True

        error_msg = f"Server error {response.status_code}"
        if isinstance(response_data, dict):
            error_msg += f": {response_data.get('error', 'Unknown error')}"
            if 'details' in response_data:
                error_msg += f"\nDetails: {response_data['details']}"

        logger.error(f"{error_msg}\n{log_msg}")
        return False

    def check_connection(self) -> bool:
        """Проверка доступности сервера"""
        try:
            response = self.session.get(
                f"{self.base_url}/health",
                timeout=5,
                headers=self.default_headers
            )
            return response.status_code == 200
        except RequestException as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False