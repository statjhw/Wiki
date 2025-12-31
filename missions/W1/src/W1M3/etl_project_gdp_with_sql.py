import pandas as pd
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime
import requests
import json


class ETLLogger:
    """ETL 프로세스 로깅을 위한 클래스"""

    def __init__(self, log_file='etl_project_log.txt'):
        """
        로거 초기화
        """
        self.log_file = log_file

    def _format_time(self):
        """
        현재 시간을 'Year-Monthname-Day-Hour-Minute-Second' 형식으로 반환
        예: 2025-December-31-14-30-45
        """
        now = datetime.now()
        return now.strftime("%Y-%B-%d-%H-%M-%S")

    def _write_log(self, message):
        """
        로그 메시지를 파일에 append하고 화면에 출력
        """
        timestamp = self._format_time()
        log_entry = f"{timestamp}, {message}"

        # 파일에 저장
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry + "\n")

        # 화면에 출력
        print(log_entry)

    def log_start(self, stage_name):
        """
        ETL 단계 시작 로그
        """
        self._write_log(f"{stage_name} Started")

    def log_end(self, stage_name, status='success', error=None):
        """
        ETL 단계 종료 로그
        """
        if status == 'success':
            self._write_log(f"{stage_name} Completed")
        else:
            error_msg = f"{stage_name} Failed"
            if error:
                error_msg += f": {str(error)}"
            self._write_log(error_msg)

    def log_message(self, message):
        """
        커스텀 메시지 로그
        :param message: 로그 메시지
        """
        self._write_log(message)


class StoregeLoader(ABC):
    """저장 단계 디자인 패턴을 위한 추상 클래스"""
    @abstractmethod
    def load(self):
        pass

class SqliteLoader(StoregeLoader):
    """Sqlite DB 저장 함수"""
    def __init__(self, db_name: str='World_Economies', logger=None):
        self.conn = sqlite3.connect(db_name)
        self.logger = logger or ETLLogger()

    def load(self, df: pd.DataFrame, metadata):
        self.logger.log_start("Load Data to SQLite")

        try:
            df['year'] = metadata['year']
            df['update_time'] = metadata['updated_time']

            df.to_sql("cleaned_data", con=self.conn, if_exists='append', index=False)
            self.conn.commit()
            self.conn.close()

            self.logger.log_end("Load Data to SQLite", status='success')
        except Exception as e:
            self.logger.log_end("Load Data to SQLite", status='error', error=e)
            raise

class SqliteReader:
    """sqlite DB 읽기 함수"""
    def __init__(self, db_name: str='World_Economies', logger=None):
        self.conn = sqlite3.connect(db_name)
        self.logger = logger or ETLLogger()

    def query(self, query):
        """SQL 쿼리 실행 및 결과 반환"""
        try:
            df = pd.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            self.logger.log_end("Query Execution", status='error', error=e)
            return None

    def pretty_print(self, df):
        """DataFrame을 보기 좋게 출력"""
        if df is None or df.empty:
            print("데이터가 없습니다.")
            return

        print("\n[GDP 100B USD 이상 국가]")
        print("=" * 70)
        print(f"총 {len(df)}개국")
        print("-" * 70)

        # 인덱스를 1부터 시작
        df_display = df.copy()
        df_display.index = range(1, len(df_display) + 1)
        print(df_display.to_string())
        print("=" * 70)

class ETLManager:
    def __init__(self, loader: StoregeLoader, logger=None):
        self.BASE_URL = "https://www.imf.org/external/datamapper/api/v1"
        self.GDP_URL = f"{self.BASE_URL}/NGDPD"
        self.COUNTRIES_URL = f"{self.BASE_URL}/countries"
        self.loader = loader
        self.logger = logger or ETLLogger()

    def extract_gdp(self):
        """
        IMF api를 활용해 GCP 데이터를 추출하는 함수
        :return: 나라 별 GCP 값 JSON
        """
        self.logger.log_start("Extract GDP Data")

        try:
            gdp_data = requests.get(self.GDP_URL).json()
            gdp_values = gdp_data.get("values", {}).get("NGDPD", {})
            self.logger.log_end("Extract GDP Data", status='success')
        except Exception as e:
            gdp_values = {}
            self.logger.log_end("Extract GDP Data", status='error', error=e)

        return gdp_values

    def extract_countries(self):
        """
        IMF api를 활용해 나라 코드 별 나라의 풀네임을 추출하는 함수
        :return:JSON
        """
        self.logger.log_start("Extract Countries Data")

        try:
            countries_data = requests.get(self.COUNTRIES_URL).json().get("countries", {})
            self.logger.log_end("Extract Countries Data", status='success')
        except Exception as e:
            countries_data = {}
            self.logger.log_end("Extract Countries Data", status='error', error=e)

        return countries_data

    def transform_GDP(self, gdp_values, countries_data) -> pd.DataFrame:
        """
        추출된 데이터를 바탕으로 나라별 GCP df를 요구사항에 맞게 리턴하는 함수
        :return: 데이터 프레임
        """
        self.logger.log_start("Transform Data")

        #가장 최근 연도 찾기
        try:
            all_years = [y for c_vals in gdp_values.values() for y in c_vals.keys()]
            if not all_years:
                self.logger.log_end("Transform Data", status='error', error="No data found")
                return None
            latest_year = max(all_years)
        except Exception as e:
            self.logger.log_end("Transform Data", status='error', error=e)
            return None

        #iso_code to region 매칭을 위한 데이터 로드
        try:
            iso_to_region = pd.read_csv("iso_code_region.csv", encoding="cp949")
        except Exception as e:
            self.logger.log_end("Load Region Data", status='error', error=e)
            return None

        try:
            #데이터 정제
            clean_list = []
            try:
                # 190여 개 개별 국가 정보를 기준으로 루프 실행
                for iso_code, meta in countries_data.items():
                    #해당 국가의 GDP 수치가 최신 연도에 존재하는 경우
                    region_row = iso_to_region[iso_to_region['국제표준화기구(ISO)(alpha3)'] == iso_code]["대륙명_공통 대륙코드"]
                    region = region_row.values[0] if len(region_row) > 0 else "Unknown"

                    if iso_code in gdp_values and latest_year in gdp_values[iso_code]:
                        country_name = meta.get("label")

                        gdp_val = float(gdp_values[iso_code][latest_year])

                        if gdp_val > 0.0:
                            clean_list.append({
                                'Country': country_name,
                                'GDP (1B USD)': round(gdp_val, 2),
                                'Region': region
                            })
            except Exception as e:
                self.logger.log_end("Transform Data", status='error', error=e)
                return None

            try:
                #데이터 프레임 생성 및 정렬
                df = pd.DataFrame(clean_list)
                if df.empty:
                    print(f"{latest_year}년도에 해당하는 국가 데이터가 없습니다.")
                    self.logger.log_end("Transform Data", status='error', error="Empty dataframe")
                    return None

                df = df.sort_values(by='GDP (1B USD)', ascending=False).reset_index(drop=True)

            except Exception as e:
                self.logger.log_end("Transform Data", status='error', error=e)
                return None

            meta = {
                'api_url': self.GDP_URL,
                'year': latest_year,
                'updated_time': datetime.now().strftime("%Y-%B-%d-%H-%M-%S")
            }

            self.logger.log_end("Transform Data", status='success')
            return df, meta

        except Exception as e:
            self.logger.log_end("Transform Data", status='error', error=e)
            return None

    def monitor(self):
        """GDP 100B USD 이상 국가 조회 및 출력"""
        self.logger.log_start("Monitor GDP Data")

        try:
            # SqliteReader 인스턴스 생성
            reader = SqliteReader(logger=self.logger)

            print(f"\n[IMF GDP 분석 보고서]")
            print("=" * 70)

            # 1. GDP 100B USD 이상 국가 조회 (최신 update_time만)
            print(f"\n1. 시장 규모 100B USD 이상 국가")
            print("-" * 70)

            query1 = """
                SELECT Country, "GDP (1B USD)", Region, year, update_time
                FROM cleaned_data
                WHERE "GDP (1B USD)" >= 100
                  AND update_time = (SELECT MAX(update_time) FROM cleaned_data)
                ORDER BY "GDP (1B USD)" DESC
            """

            df1 = reader.query(query1)

            if df1 is not None and not df1.empty:
                print(f"총 {len(df1)}개국")
                print("-" * 70)
                df1_display = df1.copy()
                df1_display.index = range(1, len(df1_display) + 1)
                print(df1_display[['Country', 'GDP (1B USD)', 'Region']].to_string())
            else:
                print("데이터가 없습니다.")

            # 2. Region별 Top5 국가의 GDP 평균
            print(f"\n2. Region별 Top5 국가의 GDP 평균")
            print("=" * 70)

            query2 = """
                WITH RankedData AS (
                    SELECT
                        Country,
                        "GDP (1B USD)",
                        Region,
                        ROW_NUMBER() OVER (PARTITION BY Region ORDER BY "GDP (1B USD)" DESC) as rank
                    FROM cleaned_data
                    WHERE update_time = (SELECT MAX(update_time) FROM cleaned_data)
                      AND Region != 'Unknown'
                )
                SELECT
                    Region,
                    COUNT(*) as "Top5 국가 수",
                    ROUND(AVG("GDP (1B USD)"), 2) as "Top5 평균 GDP (1B USD)"
                FROM RankedData
                WHERE rank <= 5
                GROUP BY Region
                ORDER BY "Top5 평균 GDP (1B USD)" DESC
            """

            df2 = reader.query(query2)

            if df2 is not None and not df2.empty:
                df2_display = df2.copy()
                df2_display.index = range(1, len(df2_display) + 1)
                print(df2_display.to_string())
            else:
                print("데이터가 없습니다.")

            print("=" * 70)

            self.logger.log_end("Monitor GDP Data", status='success')

        except Exception as e:
            self.logger.log_end("Monitor GDP Data", status='error', error=e)

    def run(self):
        self.logger.log_start("ETL Process")

        gdp_values = self.extract_gdp()
        countries_data = self.extract_countries()

        result = self.transform_GDP(gdp_values, countries_data)

        if result is not None:
            df, metadata = result
            self.loader.load(df, metadata)
            self.logger.log_end("ETL Process", status='success')
        else:
            self.logger.log_end("ETL Process", status='error', error="Transform failed")

        self.monitor()

if __name__ == "__main__":
    # 공유 Logger 인스턴스 생성
    logger = ETLLogger()

    # Loader에 logger 주입
    loader = SqliteLoader(logger=logger)

    # ETLManager 실행
    etl_manager = ETLManager(loader, logger=logger)
    etl_manager.run()