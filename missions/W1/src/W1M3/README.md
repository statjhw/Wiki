# ETL Project - GDP Data Analysis

IMF API를 활용한 국가별 GDP 데이터 ETL 파이프라인

## 사용 방법

### 1. 환경 설정

```bash
# 필요한 패키지 설치
pip install -r requirements.txt
```

### 2. 필수 파일 확인

- `iso_code_region.csv`: 국가 코드와 Region 매핑 파일 (필수)

### 3. 실행

#### JSON 버전 (etl_project_gdp.py)

```bash
python etl_project_gdp.py
```

**출력:**
- `Countries_by_GDP.jsonl`: GDP 데이터 (메타데이터 포함, append 모드)
- `etl_project_log.txt`: ETL 프로세스 로그
- 화면 출력:
  - GDP 100B USD 이상 국가 목록
  - Region별 Top5 국가의 GDP 평균

#### SQLite 버전 (etl_project_gdp_with_sql.py)

```bash
python etl_project_gdp_with_sql.py
```

**출력:**
- `World_Economies`: SQLite 데이터베이스 (append 모드)
- `etl_project_log.txt`: ETL 프로세스 로그
- 화면 출력:
  - GDP 100B USD 이상 국가 목록 (SQL 쿼리)
  - Region별 Top5 국가의 GDP 평균 (SQL 쿼리)

### 4. ETL 프로세스

1. **Extract**: IMF API에서 GDP 및 국가 데이터 추출
2. **Transform**:
   - 최신 연도 데이터 필터링
   - Region 정보 매핑 (iso_code_region.csv)
   - GDP 100B USD 이상 국가 선별
3. **Load**: JSON 파일 또는 SQLite DB에 저장
4. **Monitor**: 분석 결과 화면 출력

### 5. 로그 확인

모든 ETL 단계는 `etl_project_log.txt`에 기록됩니다:

```
Year-Monthname-Day-Hour-Minute-Second, [단계명] Started/Completed
```

### 6. 주요 기능

- **로깅**: 파일 및 화면 동시 출력
- **Region 분석**: 대륙별 GDP 통계
- **Append 모드**: 실행 시마다 데이터 누적 저장
- **최신 데이터 조회**: update_time 기준 최신 데이터만 분석

### 7. 데이터 구조

#### JSON 출력 (Countries_by_GDP.jsonl)
```json
{
    "metadata": {
        "api_url": "...",
        "year": "2024",
        "updated_time": "2025-December-31-15-30-45"
    },
    "data": [
        {
            "Country": "United States",
            "GDP (1B USD)": 26854.60,
            "Region": "Americas"
        },
        ...
    ]
}
```

#### SQLite 테이블 (cleaned_data)
| Country | GDP (1B USD) | Region | year | update_time |
|---------|-------------|--------|------|-------------|
| United States | 26854.60 | Americas | 2024 | 2025-December-31-15-30-45 |

## 참고사항

- IMF API 연결 필요 (인터넷 연결 확인)
- `iso_code_region.csv`는 CP949 인코딩 사용
- 실행할 때마다 최신 데이터 추가 (기존 데이터 유지)
