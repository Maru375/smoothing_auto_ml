import logging


def setup_logging():
    # 로거 생성
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # 로깅 레벨 설정

    # 파일 핸들러 설정
    file_handler = logging.FileHandler('processing.log')
    file_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # 콘솔 (터미널) 핸들러 설정
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
