import pytest
from datetime import datetime, timedelta, timezone

from etl.extract import extract_matches
from etl.transform import transform
from etl.load import load_to_clickhouse
from common.settings import get_clickhouse_settings


@pytest.fixture(scope="module")
def test_period() -> tuple[str, str]:
    today = datetime.now(timezone.utc)
    yesterday = today - timedelta(days=1)
    return (
        yesterday.strftime("%Y-%m-%dT%H:%M:%SZ"),
        today.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


@pytest.fixture
def clickhouse_test_settings():
    settings = get_clickhouse_settings()
    return settings


class TestEndToEnd:
    def test_full_pipeline(self, test_period, clickhouse_test_settings):
        since, until = test_period
        
        raw = extract_matches(begin_date=since, end_date=until, per_page=100)
        assert isinstance(raw, list)
        
        entities = transform(raw)
        assert isinstance(entities, dict)
        
        entities_dict = {
            name: [m.model_dump(mode="json", exclude_none=True) for m in models]
            for name, models in entities.items()
        }
        
        result = load_to_clickhouse(
            entities=entities_dict,
            dag_run_id="test-e2e",
            settings=clickhouse_test_settings,
        )
        
        assert sum(result.values()) >= 0
        assert result.get("matches", 0) == len(raw), "Несоответствие извлечено/загружено"
