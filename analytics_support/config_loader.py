import yaml


def load_config(yaml_file_path: str) -> dict:
    """
    YAML 파일을 읽고 설정을 로드 합니다.
    :param yaml_file_path: YAML 파일 경로
    :return: YAML 에 설정된 Config
    """
    try:
        with open(yaml_file_path, "r") as file:
            print("설정 파일 로드 성공")
            return yaml.safe_load(file)
    except FileNotFoundError as e:
        print(f"설정 파일을 찾을 수 없습니다: {e.filename}")
        raise
