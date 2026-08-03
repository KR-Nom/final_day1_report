"""Day 1 종합 실습의 핵심 검증과 저장 기능을 테스트합니다."""

import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import httpx
import pandas as pd
import pytest
from pydantic import ValidationError

MODULE_PATH = Path(__file__).with_name("판교_1반_장현진_day1종합실습.py")
SPEC = spec_from_file_location("day1_pipeline", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("종합 실습 모듈을 불러올 수 없습니다")

pipeline = module_from_spec(SPEC)
sys.modules[SPEC.name] = pipeline
SPEC.loader.exec_module(pipeline)


class StubAsyncClient:
    """준비된 HTTP 응답을 순서대로 반환합니다."""

    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = responses
        self.call_count = 0

    async def get(self, url: str) -> httpx.Response:
        response = self.responses[self.call_count]
        self.call_count += 1
        response.request = httpx.Request("GET", url)
        return response


@pytest.mark.anyio
async def test_fetch_json_retries_429(monkeypatch: pytest.MonkeyPatch) -> None:
    """429 응답 이후 정상 응답을 받을 때까지 재시도합니다."""
    client = StubAsyncClient(
        [
            httpx.Response(429, json={"error": "rate limit"}),
            httpx.Response(200, json={"status": "ok"}),
        ]
    )

    async def skip_delay(_seconds: float) -> None:
        return None

    monkeypatch.setattr(pipeline.asyncio, "sleep", skip_delay)
    payload = await pipeline.fetch_json(
        client, "테스트", "https://example.com", max_attempts=3
    )

    assert payload == {"status": "ok"}
    assert client.call_count == 2


def test_validate_weather_creates_models() -> None:
    """정상 날씨 응답을 Pydantic 모델 목록으로 변환합니다."""
    payload = {
        "hourly": {
            "time": ["2026-08-03T09:00", "2026-08-03T10:00"],
            "temperature_2m": [27.5, 28.0],
            "precipitation_probability": [10, 20],
        }
    }

    records = pipeline.validate_weather(payload)

    assert len(records) == 2
    assert records[0].temperature_2m == 27.5


def test_weather_range_validation() -> None:
    """허용 범위를 벗어난 강수확률은 검증 오류가 발생합니다."""
    with pytest.raises(ValidationError):
        pipeline.WeatherRecord(
            time="2026-08-03T09:00",
            temperature_2m=27.5,
            precipitation_probability=101,
        )


def test_validate_weather_rejects_mismatched_lengths() -> None:
    """날씨 배열 길이가 서로 다르면 파이프라인 오류가 발생합니다."""
    payload = {
        "hourly": {
            "time": ["2026-08-03T09:00"],
            "temperature_2m": [27.5, 28.0],
            "precipitation_probability": [10],
        }
    }

    with pytest.raises(pipeline.PipelineError, match="길이가 일치하지 않습니다"):
        pipeline.validate_weather(payload)


def test_save_and_compare_creates_both_files(tmp_path: Path) -> None:
    """CSV·Parquet 파일과 성능 측정 결과가 생성됩니다."""
    dataframe = pd.DataFrame(
        {
            "time": ["2026-08-03T09:00"],
            "temperature_2m": [27.5],
            "precipitation_probability": [10],
        }
    )

    result = pipeline.save_and_compare(dataframe, tmp_path)

    assert (tmp_path / "day1_report.csv").is_file()
    assert (tmp_path / "day1_report.parquet").is_file()
    assert result.csv_size > 0
    assert result.parquet_size > 0
