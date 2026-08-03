# final_day1_report

Python 데이터 분석 과정의 Day 1 종합 실습입니다.

## 주요 내용

- 세 API를 `asyncio.gather()`로 동시에 수집
- Pydantic v2 모델로 타입과 범위 검증
- 검증 결과를 CSV와 Parquet으로 저장
- 읽기·쓰기 시간과 파일 크기 비교
- pytest 테스트와 Ruff 코드 검사

## 실행 방법

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python 판교_1반_장현진_day1종합실습.py
```

## 검사 방법

```bash
python -m pytest -v
python -m ruff check .
```

실행 결과는 기본적으로 `data/day1_report.csv`와
`data/day1_report.parquet`에 저장됩니다.
