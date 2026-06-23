import logging
from datetime import datetime, timezone
from typing import Dict, List, Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from common.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        value = value.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            try:
                return datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                return datetime.strptime(value, '%Y-%m-%d')
    return value


def convert_value(value: Any, key: str) -> Any:
    datetime_fields = {
        'begin_at', 'end_at', 'modified_at', 'scheduled_at',
        'original_scheduled_at', 'detailed_stats', 'created_at',
        'last_loaded_at', 'started_at', 'finished_at'
    }

    if key in datetime_fields:
        return parse_datetime(value)
    return value


class ClickHouseLoader:
    LOAD_ORDER = [
        "videogames",
        "leagues",
        "series",
        "tournaments",
        "teams",
        "matches",
        "match_opponents",
    ]

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str = "etl"
    ):
        logger.info(f"Подключение к ClickHouse: {host}:{port}/{database}")
        self.client: Client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )
        logger.info("Подключение установлено")

    def _insert_batch(self, table_name: str, dicts: List[Dict]) -> int:
        if not dicts:
            logger.warning(f"[{table_name}] Нет данных для загрузки")
            return 0

        column_names = list(dicts[0].keys())

        data = []
        for row in dicts:
            data.append([convert_value(row.get(col), col) for col in column_names])

        logger.info(
            f"[{table_name}] Загрузка {len(data)} записей, "
            f"колонки: {column_names}"
        )

        self.client.insert(table_name, data, column_names=column_names)

        logger.info(f"[{table_name}] Успешно загружено {len(data)} записей")
        return len(data)

    def load_entities(self, entities: Dict[str, List]) -> Dict[str, int]:
        logger.info("Начало загрузки сущностей в ClickHouse")

        loaded_counts: Dict[str, int] = {}
        total_loaded = 0

        for entity_name in self.LOAD_ORDER:
            models = entities.get(entity_name, [])
            count = self._insert_batch(entity_name, models)
            loaded_counts[entity_name] = count
            total_loaded += count

        logger.info(
            f"Загрузка завершена. Всего записей: {total_loaded}. "
            f"По таблицам: {loaded_counts}"
        )

        return loaded_counts

    def update_state(
        self,
        entity_name: str,
        last_id: int,
        loaded_at: datetime | None = None
    ) -> None:
        if loaded_at is None:
            loaded_at = utc_now()

        logger.info(
            f"Обновление etl_state: entity={entity_name}, "
            f"last_id={last_id}, last_loaded_at={loaded_at}"
        )

        column_names = ["entity_name", "last_loaded_at", "last_id"]
        data = [[entity_name, loaded_at, last_id]]

        self.client.insert("etl_state", data, column_names=column_names)

        logger.info(f"etl_state обновлён для {entity_name}")

    def log_run(
        self,
        dag_run_id: str,
        task_id: str,
        status: str,
        rows_extracted: int,
        rows_loaded: int,
        started_at: datetime,
        finished_at: datetime | None = None,
        error_message: str | None = None
    ) -> None:
        if finished_at is None:
            finished_at = utc_now()

        logger.info(
            f"Запись в etl_run_log: dag_run_id={dag_run_id}, "
            f"status={status}, extracted={rows_extracted}, "
            f"loaded={rows_loaded}"
        )

        column_names = [
            "dag_run_id", "task_id", "started_at", "finished_at",
            "status", "rows_extracted", "rows_loaded", "error_message"
        ]
        data = [[
            dag_run_id, task_id, started_at, finished_at,
            status, rows_extracted, rows_loaded, error_message
        ]]

        self.client.insert("etl_run_log", data, column_names=column_names)

        logger.info("etl_run_log обновлён")

    def close(self) -> None:
        self.client.close()
        logger.info("Соединение с ClickHouse закрыто")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def load_to_clickhouse(
    entities: Dict[str, List],
    clickhouse_host: str = "clickhouse",
    clickhouse_port: int = 8123,
    clickhouse_username: str = "etl_user",
    clickhouse_password: str = "",
    clickhouse_database: str = "etl",
    dag_run_id: str = "",
    task_id: str = "load",
    started_at: datetime | None = None,
) -> Dict[str, int]:
    if started_at is None:
        started_at = utc_now()

    status = "success"
    error_message = None
    rows_extracted = sum(len(models) for models in entities.values())
    rows_loaded = 0
    loaded_counts: Dict[str, int] = {}

    try:
        with ClickHouseLoader(
            host=clickhouse_host,
            port=clickhouse_port,
            username=clickhouse_username,
            password=clickhouse_password,
            database=clickhouse_database,
        ) as loader:
            loaded_counts = loader.load_entities(entities)
            rows_loaded = sum(loaded_counts.values())

            for entity_name, models in entities.items():
                if models:
                    try:
                        last_id = max(
                            getattr(m, "id", 0) for m in models
                        )
                        if last_id:
                            loader.update_state(
                                entity_name=entity_name,
                                last_id=last_id,
                            )
                    except (AttributeError, TypeError):
                        logger.warning(
                            f"[{entity_name}] Не удалось определить last_id, "
                            f"пропускаем обновление etl_state"
                        )

    except Exception as e:
        status = "failed"
        error_message = str(e)
        logger.exception("Ошибка при загрузке данных в ClickHouse")
        raise

    finally:
        try:
            with ClickHouseLoader(
                host=clickhouse_host,
                port=clickhouse_port,
                username=clickhouse_username,
                password=clickhouse_password,
                database=clickhouse_database,
            ) as loader:
                loader.log_run(
                    dag_run_id=dag_run_id,
                    task_id=task_id,
                    status=status,
                    rows_extracted=rows_extracted,
                    rows_loaded=rows_loaded,
                    started_at=started_at,
                    error_message=error_message,
                )
        except Exception as log_err:
            logger.error(f"Не удалось записать etl_run_log: {log_err}")

    return loaded_counts

if __name__ == "__main__":
    import json
    from etl.transform import transform

    with open("tests/content/matches.json") as file:
        matches_list = json.load(file) 

    objects = transform(matches_list)

    load_to_clickhouse(
        objects,
        "localhost",
        8123,
        "etl_user",
        "your_strong_password",
        "etl"
    )

