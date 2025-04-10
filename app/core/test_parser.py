from jpeg_parser import JpegParser
from pathlib import Path


def test_parser():
    # 1. Загружаем тестовый файл
    test_file = Path("2309021_2025_01_30_16_17_06_31_01_00.jpg")
    file_data = test_file.read_bytes()

    # 2. Парсим данные
    parser = JpegParser()
    parsed_objects = parser.parse(file_data)

    #print("Результат парсинга:")
    print(json.dumps(parsed_objects, indent=2, ensure_ascii=False))

    # 3. Проверяем структурированные данные
    structured_data = parser._parse_json()
    print("\nСтруктурированные данные:")
    print(json.dumps(structured_data, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    import json

    test_parser()