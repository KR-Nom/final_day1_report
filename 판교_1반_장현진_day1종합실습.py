"""
====================================================================
 작성자 : P023_장현진_Day1 종합 실습
 작성일 : 2026.08.03
 GitHub : https://github.com/KR-Nom/final_day1_report
 버전   : v1.0.0
--------------------------------------------------------------------
 변경사항
   v1.0.0 (2026.08.03)
   - asyncio.gather()를 이용한 날씨·국가·IP API 동시 수집
   - Pydantic v2 모델을 이용한 응답 데이터 검증
   - CSV·Parquet 저장 및 읽기·쓰기 성능 비교
   - API 재시도, 예외 처리, pytest·Ruff 검사 구성
--------------------------------------------------------------------
 프로그램 설명
   세 개의 공개 API를 비동기로 동시에 호출하고, 필요한 데이터를
   Pydantic v2 모델로 검증합니다. 검증된 결과는 하나의 표로 정리한 뒤
   CSV와 Parquet으로 저장하고 읽기·쓰기 시간과 파일 크기를 비교합니다.

 실행 및 검사
   python 판교_1반_장현진_day1종합실습.py
   python -m pytest -v
   python -m ruff check .
====================================================================
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

import httpx
import pandas as pd
from pydantic import BaseModel, Field, ValidationError

WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=37.5665&longitude=126.9780"
    "&hourly=temperature_2m,precipitation_probability"
    "&forecast_days=3&timezone=Asia%2FSeoul"
)
COUNTRY_URL = "https://countries.dev/alpha/KOR"
IP_URL = "http://ip-api.com/json/8.8.8.8"
MAX_API_ATTEMPTS = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

LOGGER = logging.getLogger(__name__)
P = ParamSpec("P")
R = TypeVar("R")


class PipelineError(RuntimeError):
    """수집 데이터가 파이프라인 규칙을 위반할 때 발생합니다."""


#================================================================
# 1) Pydantic v2 데이터 검증 모델
class WeatherRecord(BaseModel):
    """서울의 시간대별 날씨 한 건."""

    time: datetime
    temperature_2m: float = Field(ge=-100, le=60)
    precipitation_probability: int = Field(ge=0, le=100)


class CountryRecord(BaseModel):
    """한국 국가 정보 중 필요한 값."""

    name: str = Field(min_length=1)
    capital: str = Field(min_length=1)
    region: str = Field(min_length=1)
    population: int = Field(gt=0)


class IpRecord(BaseModel):
    """IP 주소 기반 위치 정보."""

    query: str = Field(min_length=1)
    country: str = Field(min_length=1)
    city: str = Field(min_length=1)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)


class StorageResult(BaseModel):
    """CSV와 Parquet의 성능 측정 결과."""

    csv_write: float = Field(ge=0)
    csv_read: float = Field(ge=0)
    parquet_write: float = Field(ge=0)
    parquet_read: float = Field(ge=0)
    csv_size: int = Field(ge=0)
    parquet_size: int = Field(ge=0)
#================================================================


#================================================================
# 2) 비동기 함수 실행 시간 측정 데코레이터
def async_timer(function: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """비동기 함수의 실행 시간을 로그로 기록합니다."""

    @wraps(function)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        start = time.perf_counter()
        try:
            return await function(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            LOGGER.info("%s 실행 시간: %.6f초", function.__name__, elapsed)

    return wrapper
#================================================================


#================================================================
# 3) API JSON 데이터 수집 및 일시 오류 재시도
async def fetch_json(
    client: httpx.AsyncClient,
    api_name: str,
    url: str,
    max_attempts: int = MAX_API_ATTEMPTS,
) -> dict[str, Any]:
    """API를 호출하고 정상적인 JSON 객체를 반환합니다."""
    for attempt in range(1, max_attempts + 1):
        response = await client.get(url)
        should_retry = response.status_code in RETRYABLE_STATUS_CODES

        if not should_retry or attempt == max_attempts:
            break

        delay = 2 ** (attempt - 1)
        LOGGER.warning(
            "%s API 일시 오류(%d): %d초 후 재시도(%d/%d)",
            api_name,
            response.status_code,
            delay,
            attempt,
            max_attempts,
        )
        await asyncio.sleep(delay)

    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise PipelineError(f"{api_name} API 응답이 JSON 객체가 아닙니다")

    LOGGER.info("%s API 요청 성공: 상태 코드 %d", api_name, response.status_code)
    return payload
#================================================================


#================================================================
# 4) 세 API 비동기 동시 수집
@async_timer
async def collect_all() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """asyncio.gather()로 날씨·국가·IP API를 동시에 호출합니다."""
    headers = {"User-Agent": "final-day1-report/1.0"}
    async with httpx.AsyncClient(timeout=15.0, headers=headers) as client:
        weather, country, ip = await asyncio.gather(
            fetch_json(client, "날씨", WEATHER_URL),
            fetch_json(client, "국가", COUNTRY_URL),
            fetch_json(client, "IP", IP_URL),
        )
    return weather, country, ip
#================================================================


#================================================================
# 5) 날씨 데이터 구조 및 범위 검증
def validate_weather(payload: dict[str, Any]) -> list[WeatherRecord]:
    """날씨 배열 구조와 길이를 확인하고 모델 목록으로 변환합니다."""
    try:
        hourly = payload["hourly"]
        times = hourly["time"]
        temperatures = hourly["temperature_2m"]
        precipitation = hourly["precipitation_probability"]
    except (KeyError, TypeError) as error:
        raise PipelineError("날씨 응답 구조가 올바르지 않습니다") from error

    columns = (times, temperatures, precipitation)
    if not all(isinstance(values, list) for values in columns):
        raise PipelineError("날씨 시간대 데이터가 리스트가 아닙니다")
    if len({len(values) for values in columns}) != 1:
        raise PipelineError("날씨 데이터 길이가 일치하지 않습니다")
    if not times:
        raise PipelineError("날씨 데이터가 비어 있습니다")

    return [
        WeatherRecord(
            time=time_value,
            temperature_2m=temperature,
            precipitation_probability=probability,
        )
        for time_value, temperature, probability in zip(*columns, strict=True)
    ]
#================================================================


#================================================================
# 6) 국가 및 IP 데이터 검증
def validate_country(payload: dict[str, Any]) -> CountryRecord:
    """국가 응답에서 이름·수도·지역·인구를 추출하고 검증합니다."""
    name = payload.get("name")
    if isinstance(name, dict):
        name = name.get("common") or name.get("official")

    capital = payload.get("capital")
    if isinstance(capital, list):
        capital = capital[0] if capital else None

    try:
        return CountryRecord(
            name=name,
            capital=capital,
            region=payload["region"],
            population=payload["population"],
        )
    except KeyError as error:
        raise PipelineError("국가 응답 구조가 올바르지 않습니다") from error


def validate_ip(payload: dict[str, Any]) -> IpRecord:
    """IP API 성공 여부와 위치 데이터 범위를 검증합니다."""
    if payload.get("status") != "success":
        message = payload.get("message", "알 수 없는 오류")
        raise PipelineError(f"IP API 응답 실패: {message}")
    return IpRecord.model_validate(payload)
#================================================================


#================================================================
# 7) 검증된 세 API 데이터를 하나의 표로 구성
def build_dataframe(
    weather: list[WeatherRecord],
    country: CountryRecord,
    ip: IpRecord,
) -> pd.DataFrame:
    """시간대별 날씨에 국가와 IP 정보를 결합합니다."""
    dataframe = pd.DataFrame(
        [record.model_dump(mode="json") for record in weather]
    )
    return dataframe.assign(
        country_name=country.name,
        capital=country.capital,
        region=country.region,
        population=country.population,
        ip_query=ip.query,
        ip_country=ip.country,
        ip_city=ip.city,
        ip_lat=ip.lat,
        ip_lon=ip.lon,
    )
#================================================================


#================================================================
# 8) CSV·Parquet 저장 및 성능 비교
def measure_operation(operation: Callable[[], R]) -> tuple[R, float]:
    """일반 함수의 반환값과 실행 시간을 함께 반환합니다."""
    start = time.perf_counter()
    result = operation()
    return result, time.perf_counter() - start


def save_and_compare(dataframe: pd.DataFrame, output_dir: Path) -> StorageResult:
    """동일 데이터를 두 형식으로 저장하고 성능과 형태를 비교합니다."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "day1_report.csv"
    parquet_path = output_dir / "day1_report.parquet"

    _, csv_write = measure_operation(
        lambda: dataframe.to_csv(csv_path, index=False, encoding="utf-8")
    )
    csv_data, csv_read = measure_operation(lambda: pd.read_csv(csv_path))
    _, parquet_write = measure_operation(
        lambda: dataframe.to_parquet(parquet_path, index=False)
    )
    parquet_data, parquet_read = measure_operation(
        lambda: pd.read_parquet(parquet_path)
    )

    if csv_data.shape != dataframe.shape or parquet_data.shape != dataframe.shape:
        raise PipelineError("저장 후 데이터의 행·열 개수가 원본과 다릅니다")

    return StorageResult(
        csv_write=csv_write,
        csv_read=csv_read,
        parquet_write=parquet_write,
        parquet_read=parquet_read,
        csv_size=csv_path.stat().st_size,
        parquet_size=parquet_path.stat().st_size,
    )
#================================================================


#================================================================
# 9) 전체 수집·검증·저장 파이프라인
@async_timer
async def run_pipeline(output_dir: Path) -> tuple[pd.DataFrame, StorageResult]:
    """수집, 검증, 표 구성, 저장을 순서대로 실행합니다."""
    weather_payload, country_payload, ip_payload = await collect_all()
    weather = validate_weather(weather_payload)
    country = validate_country(country_payload)
    ip = validate_ip(ip_payload)
    dataframe = build_dataframe(weather, country, ip)
    storage_result = save_and_compare(dataframe, output_dir)
    LOGGER.info("날씨 데이터 저장 완료: %d건", len(dataframe))
    return dataframe, storage_result
#================================================================


#================================================================
# 10) 실행 결과 출력 및 오류 유형별 처리
def print_result(dataframe: pd.DataFrame, result: StorageResult) -> None:
    """수집 결과와 파일 형식별 성능을 출력합니다."""
    first_row = dataframe.iloc[0]
    print(f"날씨 데이터: {len(dataframe)}건")
    print(f"국가 정보: {first_row['country_name']} / {first_row['capital']}")
    print(f"IP 위치: {first_row['ip_country']} {first_row['ip_city']}")
    print(
        f"CSV - 쓰기:{result.csv_write:.6f}초, "
        f"읽기:{result.csv_read:.6f}초, 크기:{result.csv_size:,} bytes"
    )
    print(
        f"Parquet - 쓰기:{result.parquet_write:.6f}초, "
        f"읽기:{result.parquet_read:.6f}초, 크기:{result.parquet_size:,} bytes"
    )


async def main() -> int:
    """환경변수 출력 경로를 적용하여 전체 프로그램을 실행합니다."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    output_dir = Path(os.getenv("OUTPUT_DIR", "data"))

    try:
        dataframe, result = await run_pipeline(output_dir)
    except httpx.HTTPError as error:
        LOGGER.error("API 요청 오류: %s", error)
        return 1
    except ValidationError as error:
        LOGGER.error("스키마 검증 오류:\n%s", error)
        return 1
    except PipelineError as error:
        LOGGER.error("파이프라인 데이터 오류: %s", error)
        return 1
    except OSError:
        LOGGER.exception("파일 처리 중 운영체제 오류가 발생했습니다")
        return 1

    print_result(dataframe, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
#================================================================
