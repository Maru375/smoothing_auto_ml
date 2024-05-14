import sys
import pytz
import pandas as pd
from pathlib import Path
from datetime import datetime
from analytics_support.config_loader import load_config
from analytics_support.database import InfluxDBManager
from analytics_support.data_processing import resampling_data, replace_iqr_outliers, merge_dataframes


# 상수 정의
CONFIG_PATH = "resources/influxdb_config.yaml"
CSV_PATH = "resources/training_data/sensor_data.csv"
DEFAULT_START_DATE = "2024-04-17"
KOREA_TZ = pytz.timezone("Asia/Seoul")


def check_to_start_date(file_path: str) -> datetime:
    """
    쿼리 조회 시작 날짜를 구하기 위해 CSV의 time컬럼을 읽어
    마지막 날짜를 UTC로 변환하여 반환합니다.

    만약 CSV가 없다면 기본 시작 날짜를 사용합니다.
    :param file_path: CSV 파일 경로
    :return: 쿼리 조회 시작 날짜
    """
    path = Path(file_path)

    # 파일 존재 여부 확인
    if not path.exists():
        print(f" {file_path} 에서 CSV를 찾을 수 없습니다.")
        print("시작 날짜를 Default로 설정합니다.")
        # 문자열에서 datetime 객체로 변환하고, 시간대를 지정
        start_time_kst = KOREA_TZ.localize(datetime.strptime(DEFAULT_START_DATE, "%Y-%m-%d"))
        return start_time_kst.astimezone(pytz.utc)

    try:
        df = pd.read_csv(file_path)
        # 'time' 컬럼의 마지막 행의 데이터를 추출
        last_date = df['time'].iloc[-1]
        last_date_kst = pd.to_datetime(last_date).tz_localize(KOREA_TZ)
        return last_date_kst.astimezone(pytz.utc)
    except Exception as e:
        print(f"에러 발생: {e}")
        return sys.exit(1)


def check_to_end_date(timezone: pytz.timezone) -> datetime:
    """
    실행 시점의 날짜를 기준으로 전날의 날짜를 계산 하여 UTC로 변환합니다.
    :param timezone: 타임존 정보를 나타내는 pytz의 타임존 객체
    :return: UTC 시간대로 변환한 datetime
    """
    today_kst = datetime.now(timezone)

    end_time_kst = KOREA_TZ.localize(datetime(today_kst.year, today_kst.month, today_kst.day, 0, 0, 0))

    end_time_utc = end_time_kst.astimezone(pytz.utc)

    return end_time_utc


def update_csv(csv_path, new_data):
    """
    기존 CSV 파일을 업데이트합니다.
    :param csv_path: 업데이트할 CSV 파일의 경로
    :param new_data: 새로 추가할 데이터 (DataFrame 형식)
    """
    # 기존 데이터 로드, `_time`을 인덱스로 설정
    if Path(csv_path).exists():
        existing_data = pd.read_csv(csv_path, index_col='time', parse_dates=True)
    else:
        existing_data = pd.DataFrame()

    # 새 데이터와 기존 데이터 병합
    updated_data = pd.concat([existing_data, new_data], axis=0)

    # 중복 인덱스 제거, 최신 데이터 유지
    updated_data = updated_data[~updated_data.index.duplicated(keep='last')]

    # 파일에 저장
    updated_data.to_csv(csv_path)
    print("CSV 파일이 업데이트 되었습니다.")


start_time_utc = check_to_start_date(CSV_PATH)
end_time_utc = check_to_end_date(KOREA_TZ)

print("조회 날짜 확인 (UTC):", start_time_utc.date(), "~", end_time_utc.date())

if start_time_utc.date() == end_time_utc.date():
    print("CSV가 최신버전 입니다.")
    sys.exit(1)

print("CSV를 최신화 합니다.")

# 콘센트 전력(W) 조회
query_power_socket_data = f'''
import "experimental"
from(bucket: "powermetrics_data")
  |> range(start: {start_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["phase"] == "total")
  |> filter(fn: (r) => r["description"] == "w")
  |> filter(fn: (r) => r["place"] == "office")
  |> filter(fn: (r) => r["location"] == "class_a_floor_heating_1" or r["location"] == "class_a_floor_heating_2")
  |> aggregateWindow(every: 1m, fn: last, createEmpty: false)
  |> map(fn: (r) => ({{r with _time: experimental.addDuration(d: 9h, to: r._time)}}))
  |> keep(columns: ["_time", "_value"])
'''

# 이산화탄소 조회 Flux 쿼리
query_co2_data = f'''
import "experimental"
from(bucket: "environmentalsensors_data")
  |> range(start: {start_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["place"] == "class_a")
  |> filter(fn: (r) => r["measurement"] == "co2")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _time: experimental.addDuration(d: 9h, to: r._time)}}))
  |> keep(columns: ["_time", "_value"])
'''

# 조도 조회 Flux 쿼리
query_illumination_data = f'''
import "experimental"
from(bucket: "environmentalsensors_data")
  |> range(start: {start_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["place"] == "class_a")
  |> filter(fn: (r) => r["measurement"] == "illumination")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _time: experimental.addDuration(d: 9h, to: r._time)}}))
  |> keep(columns: ["_time", "_value"])
'''

flux_queries = {
    "PowerSocketData": query_power_socket_data,
    "CO2Data": query_co2_data,
    "IlluminationData": query_illumination_data
}


def main():
    try:
        # 설정 로드
        config = load_config(CONFIG_PATH)

        # DB 클라이언트 생성
        db_manager = InfluxDBManager(config["smoothing_influxdb"])

        # Flux 쿼리 실행
        dataframes = db_manager.queries_to_dataframes(flux_queries)
        db_manager.close()

        # 데이터 전처리(집계)
        power_socket_df = resampling_data(dataframes["PowerSocketData"], "socket_power(Wh)", "sum")
        co2_df = resampling_data(dataframes["CO2Data"], "average_co2(ppm)")
        illumination_df = resampling_data(dataframes["IlluminationData"], "average_illumination(lux)")
        print("데이터 전처리 완료")

        # 이상치 제거
        power_socket_df = replace_iqr_outliers(power_socket_df)
        co2_df = replace_iqr_outliers(co2_df)
        illumination_df = replace_iqr_outliers(illumination_df)
        print("데이터 이상치 제거 완료")

        # 데이터 프레임 병합
        training_set_df = merge_dataframes([power_socket_df, co2_df, illumination_df])

        # CSV 최신화
        update_csv(CSV_PATH, training_set_df)

    except Exception as e:
        print(f"데이터 처리 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
