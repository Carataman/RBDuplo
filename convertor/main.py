import logging

from app.services.data_service import DataProcessingService
from app.utils.config_loader import load_config
from app.utils.logger import setup_logging


def main():
    # Инициализация
    setup_logging()
    config = load_config()

    try:
        # Создаем сервис обработки данных
        service = DataProcessingService(config)

        # Основной workflow
        service.run_processing_flow()

    except Exception as e:
        logging.critical(f"Application failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()