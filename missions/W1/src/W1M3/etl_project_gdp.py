import pandas as pd
import sqlite3
from abc import ABC, abstractmethod

import requests
import json

class StoregeLoader(ABC):
    """저장 단계 디자인 패턴을 위한 추상 클래스"""
    @abstractmethod
    def load(self):
        pass

class JsonLoader(StoregeLoader):
    """Json 형식 저장 클래스"""
    def load(self, df: pd.DataFrame):
        """데이터 저장 함수"""
        df.to_json(
            "Countries_by_GDP.json",
            orient="records",
            force_ascii=False,
            indent = 4
        )

class SqliteLoader(StoregeLoader):
    """Sqlite DB 저장 함수"""
    def __init__(self, db_name: str='World_Economies'):
        self.conn = sqlite3.connect(db_name)

    def load(self, df: pd.DataFrame):
        df.to_sql("cleaned_data", con=self.conn, if_exists='replace', index=False)
        self.conn.commit()
        self.conn.close()

class ETLManager:
    def __init__(self, loader: StoregeLoader):
        self.BASE_URL = "https://www.imf.org/external/datamapper/api/v1"
        self.GDP_URL = f"{self.BASE_URL}/NGDPD"
        self.COUNTRIES_URL = f"{self.BASE_URL}/countries"
        self.loader = loader

    def extract_gdp(self):
        """
        IMF api를 활용해 GCP 데이터를 추출하는 함수
        :return: 나라 별 GCP 값 JSON
        """
        try:
            gdp_data = requests.get(self.GDP_URL).json()
            gdp_values = gdp_data.get("values", {}).get("NGDPD", {})
        except Exception as e:
            gdp_values = {}
            #TODO: log 처리

        return gdp_values

    def extract_countries(self):
        """
        IMF api를 활용해 나라 코드 별 나라의 풀네임을 추출하는 함수
        :return:JSON
        """
        try:
            countries_data = requests.get(self.COUNTRIES_URL).json().get("countries", {})
        except Exception as e:
            countries_data = {}
            #TODO: log 처리
        return countries_data

    def transform_GDP(self, gdp_values, countries_data) -> pd.DataFrame:
        """
        추출된 데이터를 바타아으로 나라별 GCP df를 요구사항에 맞게 리턴하는 함수
        :return: 데이터 프레임
        """
        #가장 최근 연도 찾기
        try:
            all_years = [y for c_vals in gdp_values.values() for y in c_vals.keys()]
            if not all_years:
                #print("데이터를 찾을 수 없습니다.")
                return
            latest_year = max(all_years)
        except Exception as e:
            pass

        try:
            #데이터 정제
            clean_list = []
            try:
                # 190여 개 개별 국가 정보를 기준으로 루프 실행
                for iso_code, meta in countries_data.items():
                    #해당 국가의 GDP 수치가 최신 연도에 존재하는 경우
                    if iso_code in gdp_values and latest_year in gdp_values[iso_code]:
                        country_name = meta.get("label")

                        gdp_val = float(gdp_values[iso_code][latest_year])

                        if gdp_val > 0.0:
                            clean_list.append({
                                'Country': country_name,
                                'GDP (1B USd)': round(gdp_val, 2)
                            })
            except Exception as e:
                pass

            try:
                #데이터 프레임 생성 및 정렬
                df = pd.DataFrame(clean_list)
                if df.empty:
                    print(f"{latest_year}년도에 해당하는 국가 데이터가 없습니다.")
                    return None

                df = df.sort_values(by='GDP (1B USD)', ascending=False).reset_index(drop=True)

            except Exception as e:
                pass
            return df

        except Exception as e:
            pass

    def run(self):
        gdp_values = self.extract_gdp()
        countries_data = self.extract_countries()

        df = self.transform_GDP(gdp_values, countries_data)

        self.loader.load(df)

if __name__ == "__main__":
    loader = JsonLoader()
    etl_manager = ETLManager(loader)
    etl_manager.run()



