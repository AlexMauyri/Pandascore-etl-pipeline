import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from common.minio_client import MinIOStorage
from common.logging_config import setup_logging
from etl.extract import extract_matches
from etl.transform import transform
from etl.load import load_to_clickhouse

setup_logging()
logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_API_URL"),
    "access_key": os.getenv("MINIO_ROOT_USER"),
    "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
    "bucket": os.getenv("MINIO_BUCKET"),
    "secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
}

CLICKHOUSE_CONFIG = {
    "host": os.getenv("CLICKHOUSE_HOST"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
    "username": os.getenv("CLICKHOUSE_USER"),
    "password": os.getenv("CLICKHOUSE_PASSWORD"),
    "database": os.getenv("CLICKHOUSE_DB"),
}

def _get_minio() -> MinIOStorage:
    return MinIOStorage(**MINIO_CONFIG)


def _serialize_for_minio(obj: Any) -> Any:
    from pydantic import BaseModel

    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json", exclude_none=True)
    if isinstance(obj, list):
        return [_serialize_for_minio(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize_for_minio(v) for k, v in obj.items()}
    return obj


def task_extract(**context) -> Dict[str, Any]:
    dag_run = context["dag_run"]
    execution_date: datetime = context["data_interval_start"]
    since = (execution_date - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    until = execution_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"[extract] dag_run_id={dag_run.run_id}, период: {since} -> {until}")

    try:
        raw_matches = extract_matches(begin_date=since, end_date=until, per_page=100)
    except Exception as e:
        logger.exception("[extract] Ошибка при извлечении данных из API")
        raise AirflowFailException(f"Extract failed: {e}")

    rows_extracted = len(raw_matches)
    logger.info(f"[extract] Извлечено матчей: {rows_extracted}")

    minio = _get_minio()
    key = f"raw/{dag_run.run_id}/matches.json"
    minio.put_json(key, raw_matches)
    logger.info(f"[extract] Сырые данные сохранены в MinIO: {key}")

    return {
        "bucket": MINIO_CONFIG["bucket"],
        "key": key,
        "rows_extracted": rows_extracted,
        "since": since,
        "until": until,
    }


def task_transform(**context) -> Dict[str, Any]:
    dag_run = context["dag_run"]
    ti = context["ti"]

    extract_meta: Dict[str, Any] = ti.xcom_pull(task_ids="extract")
    if not extract_meta:
        raise AirflowFailException("[transform] Нет мета-информации от extract")

    key = extract_meta["key"]
    rows_extracted = extract_meta["rows_extracted"]

    logger.info(f"[transform] dag_run_id={dag_run.run_id}, читаем из MinIO: {key}")

    minio = _get_minio()
    raw_matches = minio.get_json(key)

    try:
        entities = transform(raw_matches)
    except Exception as e:
        logger.exception("[transform] Ошибка при трансформации")
        raise AirflowFailException(f"Transform failed: {e}")

    rows_transformed = sum(len(models) for models in entities.values())
    logger.info(f"[transform] Трансформировано сущностей: {rows_transformed}")

    entities_dict = _serialize_for_minio(entities)

    transformed_key = f"transformed/{dag_run.run_id}/entities.json"
    minio.put_json(transformed_key, entities_dict)
    logger.info(f"[transform] Сохранено в MinIO: {transformed_key}")

    return {
        "bucket": MINIO_CONFIG["bucket"],
        "key": transformed_key,
        "rows_transformed": rows_transformed,
        "rows_extracted": rows_extracted,
    }


def task_load(**context) -> Dict[str, Any]:
    dag_run = context["dag_run"]
    ti = context["ti"]
    started_at = datetime.now(timezone.utc)

    transform_meta: Dict[str, Any] = ti.xcom_pull(task_ids="transform")
    if not transform_meta:
        raise AirflowFailException("[load] Нет мета-информации от transform")

    key = transform_meta["key"]
    rows_extracted = transform_meta["rows_extracted"]
    rows_transformed = transform_meta["rows_transformed"]

    logger.info(f"[load] dag_run_id={dag_run.run_id}, читаем из MinIO: {key}")

    minio = _get_minio()
    entities_dict = minio.get_json(key)

    try:
        loaded_counts = load_to_clickhouse(
            entities=entities_dict,
            clickhouse_host=CLICKHOUSE_CONFIG["host"],
            clickhouse_port=CLICKHOUSE_CONFIG["port"],
            clickhouse_username=CLICKHOUSE_CONFIG["username"],
            clickhouse_password=CLICKHOUSE_CONFIG["password"],
            clickhouse_database=CLICKHOUSE_CONFIG["database"],
            dag_run_id=dag_run.run_id,
            task_id="load",
            started_at=started_at,
        )
    except Exception as e:
        logger.exception("[load] Ошибка при загрузке в ClickHouse")
        raise AirflowFailException(f"Load failed: {e}")

    rows_loaded = sum(loaded_counts.values())
    logger.info(f"[load] Загружено в ClickHouse: {rows_loaded} записей")

    raw_key = f"raw/{dag_run.run_id}/matches.json"
    try:
        minio.delete(key)
        minio.delete(raw_key)
        logger.info(f"[load] Временные файлы удалены из MinIO")
    except Exception as e:
        logger.warning(f"[load] Не удалось удалить временные файлы: {e}")

    return {
        "rows_loaded": rows_loaded,
        "rows_transformed": rows_transformed,
        "rows_extracted": rows_extracted,
        "loaded_counts": loaded_counts,
    }


with DAG(
    dag_id="cybersport_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="ETL-пайплайн для загрузки cybersport-данных из PandaScore в ClickHouse",
    schedule="@daily",
    start_date=datetime(2026, 6, 1, tzinfo=timezone.utc),
    catchup=True,
    max_active_runs=1,
    tags=["etl", "cybersport", "pandascore", "clickhouse"],
) as dag:

    task_extract_op = PythonOperator(task_id="extract", python_callable=task_extract)
    task_transform_op = PythonOperator(task_id="transform", python_callable=task_transform)
    task_load_op = PythonOperator(task_id="load", python_callable=task_load)

    task_extract_op >> task_transform_op >> task_load_op
