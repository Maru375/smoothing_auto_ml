import logging
import pandas as pd
from typing import Dict
from influxdb_client import InfluxDBClient


class InfluxDBManager:
    """
    InfluxDB 클라이언트 인스턴스를 생성합니다.
    :param config: InfluxDB 접속 정보
    :return: InfluxDB 클라이언트
    """
    def __init__(self, config: dict):
        self.client = InfluxDBClient(
            url=config["url"],
            token=config["token"],
            org=config["org"]
        )

    def queries_to_dataframes(self, queries_dict: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """
        Flux 쿼리를 사용하여 데이터를 조회하여 데이터프레임으로 만듭니다.
        :param queries_dict: Flux 쿼리 Dict[str, str]
        :return: Dict[str, pd.DataFrame]
        """
        dataframes = {}
        for key, query in queries_dict.items():
            result = self.client.query_api().query(query=query)
            results = []

            for table in result:
                for record in table.records:
                    results.append({
                        "time": record.get_time(),
                        "value": record.get_value()
                    })

            df = pd.DataFrame(results)
            df["time"] = df["time"].astype(str).str.replace(r"\+00:00$", "", regex=True)
            dataframes[key] = df

        logging.info("데이터 조회 완료")

        return dataframes

    def close(self):
        logging.info("InfluxDBClient 정상 종료")
        self.client.close()
