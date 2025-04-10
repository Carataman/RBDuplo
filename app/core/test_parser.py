from jpeg_parser import JpegParser
from pathlib import Path


def test_parser():
    # 1. Загружаем тестовый файл
    test_file = Path("2309021_2025_01_30_16_17_06_31_01_00.jpg")
    file_data = test_file.read_bytes()

    parser = JpegParser()

    # Загружаем файл
    with open(test_file, 'rb') as f:
        data = f.read()

    # Получаем сегменты в виде байтов (по умолчанию)
    byte_segments = parser._extract_jpeg_segments(data)
    print(f"Найдено {len(byte_segments)} сегментов в бинарном формате")



    segments = parser._extract_jpeg_segments(data)
    base64_segments = parser.segments_to_base64(segments)

    print(base64_segments[1])

if __name__ == "__main__":
    import json

    test_parser()