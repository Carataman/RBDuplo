import json
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def load_config() -> Dict[str, Any]:
    """Загрузка и объединение конфигурационных файлов"""
    config = {}
    base_dir = Path(__file__).parent.parent.parent
    conf_dir = base_dir  / "conf"

    if not conf_dir.exists():
        raise FileNotFoundError(f"Директория с конфигами не найдена: {conf_dir}")

    # 1. Загрузка connection.json (обязательный)
    conn_path = conf_dir / "connection.json"
    try:
        with open(conn_path, 'r', encoding='utf-8') as f:
            config.update(json.load(f))
        logger.info(f"Успешно загружен {conn_path}")
    except Exception as e:
        logger.critical(f"Ошибка загрузки connection.json: {e}")
        raise

    # 2. Загрузка configuration.json
    conf_path = conf_dir / "configuration.json"
    try:
        with open(conf_path, 'r', encoding='utf-8') as f:
            configuration = json.load(f)


            # Правильное объединение вложенных структур
            for key, value in configuration.items():
                if key in config:
                    if isinstance(value, dict) and isinstance(config[key], dict):
                        config[key].update(value)
                    else:
                        config[key] = value
                else:
                    config[key] = value

        logger.info(f"Успешно загружен {conf_path}")

    except FileNotFoundError:
        logger.warning("Файл configuration.json не найден, используются значения по умолчанию")
    except Exception as e:
        logger.error(f"Ошибка загрузки configuration.json: {e}")

    # Установка значений по умолчанию
    config.setdefault('processing', {}).setdefault('start_date', '1970-01-01 00:00:00')

    # Валидация обязательных параметров
    required_sections = ['database', ]
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Отсутствует обязательная секция '{section}' в конфигурации")

    logger.debug(f"Полная загруженная конфигурация:\n{json.dumps(config, indent=2)}")
    return config