import sys
import subprocess
import importlib.metadata


def install_package(package):
    try:
        # 패키지가 이미 설치되어 있는지 확인
        importlib.metadata.version(package)
        print(f"{package}이(가) 이미 설치되어 있습니다.")
    except importlib.metadata.PackageNotFoundError:
        # 설치되어 있지 않다면 pip를 사용하여 설치
        try:
            print(f"{package}을(를) 설치 중입니다...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            print(f"{package}이(가) 성공적으로 설치되었습니다.")
        except subprocess.CalledProcessError:
            print(f"{package} 설치에 실패했습니다.")
        except Exception as e:
            print(f"예기치 못한 오류가 발생했습니다: {str(e)}.")


install_package('influxdb_client')
install_package('pycaret')
install_package('pyyaml')