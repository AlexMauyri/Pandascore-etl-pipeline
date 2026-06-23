import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
 
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
 
from common.logging_config import setup_logging
from common.minio_client import MinIOStorage
from common.settings import get_minio_settings
from etl.extract import extract_matches
from etl.load import load_to_clickhouse
from etl.transform import transform
 
setup_logging()
logger = logging.getLogger(__name__)
 
 
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
 
TASK_EXECUTION_TIMEOUT = timedelta(minutes=30)
 
 
def _get_minio() -> MinIOStorage:
    settings = get_minio_settings()
    return MinIOStorage(
        endpoint=settings.api_url,
        access_key=settings.root_user,
        secret_key=settings.root_password,
        bucket=settings.bucket,
        secure=settings.secure,
    )
 
 
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
 
    minio = _get_minio()
    key = f"raw/{dag_run.run_id}/matches.json"
 
    if minio.exists(key):
        logger.info(f"[extract] Сырые данные уже есть в MinIO ({key}), повторный запрос к API пропущен")
        raw_matches = minio.get_json(key)
        rows_extracted = len(raw_matches)
    else:
        try:
            raw_matches = extract_matches(begin_date=since, end_date=until, per_page=100)
        except Exception as e:
            logger.exception("[extract] Ошибка при извлечении данных из API")
            raise AirflowFailException(f"Extract failed: {e}")
 
        rows_extracted = len(raw_matches)
        logger.info(f"[extract] Извлечено матчей: {rows_extracted}")
        minio.put_json(key, raw_matches)
        logger.info(f"[extract] Сырые данные сохранены в MinIO: {key}")
 
    return {
        "key": key,
        "rows_extracted": rows_extracted,
        "since": since,
        "until": until,
    }
 
 
def task_transform(**context) -> Dict[str, Any]:
    dag_run = context["dag_run"]
    ti = context["ti"]
 
    extract_meta: Dict[str, Any] | None = ti.xcom_pull(task_ids="extract")
    if not extract_meta:
        raise AirflowFailException("[transform] Нет мета-информации от extract")
 
    key = extract_meta["key"]
    rows_extracted = extract_meta["rows_extracted"]
    transformed_key = f"transformed/{dag_run.run_id}/entities.json"
 
    logger.info(f"[transform] dag_run_id={dag_run.run_id}, читаем из MinIO: {key}")
 
    minio = _get_minio()
 
    if minio.exists(transformed_key):
        logger.info(f"[transform] Результат уже есть в MinIO ({transformed_key}), пропускаем повторную трансформацию")
        entities_dict = minio.get_json(transformed_key)
        rows_transformed = sum(len(v) for v in entities_dict.values())
        return {
            "key": transformed_key,
            "rows_transformed": rows_transformed,
            "rows_extracted": rows_extracted,
        }
 
    raw_matches = minio.get_json(key)
 
    try:
        entities = transform(raw_matches)
    except Exception as e:
        logger.exception("[transform] Ошибка при трансформации")
        raise AirflowFailException(f"Transform failed: {e}")
 
    rows_transformed = sum(len(models) for models in entities.values())
    logger.info(f"[transform] Трансформировано сущностей: {rows_transformed}")
 
    entities_dict = _serialize_for_minio(entities)
    minio.put_json(transformed_key, entities_dict)
    logger.info(f"[transform] Сохранено в MinIO: {transformed_key}")
 
    return {
        "key": transformed_key,
        "rows_transformed": rows_transformed,
        "rows_extracted": rows_extracted,
    }
 
 
def task_load(**context) -> Dict[str, Any]:
    dag_run = context["dag_run"]
    ti = context["ti"]
    started_at = datetime.now(timezone.utc)
 
    transform_meta: Dict[str, Any] | None = ti.xcom_pull(task_ids="transform")
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
            dag_run_id=dag_run.run_id,
            task_id="load",
            started_at=started_at,
        )
    except Exception as e:
        logger.exception("[load] Ошибка при загрузке в ClickHouse")
        raise AirflowFailException(f"Load failed: {e}")
 
    rows_loaded = sum(loaded_counts.values())
    logger.info(f"[load] Загружено в ClickHouse: {rows_loaded} записей")
 
    return {
        "rows_loaded": rows_loaded,
        "rows_transformed": rows_transformed,
        "rows_extracted": rows_extracted,
        "loaded_counts": loaded_counts,
    }
 
 
def task_cleanup(**context) -> None:
    dag_run = context["dag_run"]
    ti = context["ti"]
 
    extract_meta: Dict[str, Any] | None = ti.xcom_pull(task_ids="extract")
    transform_meta: Dict[str, Any] | None = ti.xcom_pull(task_ids="transform")
 
    minio = _get_minio()
 
    for meta in (extract_meta, transform_meta):
        if not meta:
            continue
        key = meta["key"]
        try:
            minio.delete(key)
            logger.info(f"[cleanup] Удалён временный файл: {key}")
        except Exception as e:
            logger.warning(f"[cleanup] Не удалось удалить {key}: {e}")
 
 
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
 
    task_extract_op = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
        execution_timeout=TASK_EXECUTION_TIMEOUT,
    )
    task_transform_op = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
        execution_timeout=TASK_EXECUTION_TIMEOUT,
    )
    task_load_op = PythonOperator(
        task_id="load",
        python_callable=task_load,
        execution_timeout=TASK_EXECUTION_TIMEOUT,
    )
    task_cleanup_op = PythonOperator(
        task_id="cleanup",
        python_callable=task_cleanup,
        execution_timeout=timedelta(minutes=5),
        retries=1,
    )
 
    task_extract_op >> task_transform_op >> task_load_op >> task_cleanup_op
