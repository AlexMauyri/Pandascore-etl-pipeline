import requests
import logging
from requests import RequestException, Response
from typing import Any, Dict, List
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry_if_result,
    before_sleep_log,
)

from common.logging_config import setup_logging
from common.settings import get_pandascore_settings

setup_logging()
logger = logging.getLogger(__name__)

def _is_retryable_status(response: Response) -> bool:
    return response.status_code == 429 or 500 <= response.status_code < 600


def _build_headers(token: str) -> Dict[str, str]:
    return {"accept": "application/json", "Authorization": f"Bearer {token}"}


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=1, max=10),
    retry=(retry_if_exception_type(RequestException) | retry_if_result(_is_retryable_status)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def _fetch_page(url: str, headers: Dict[str, str], timeout: int) -> Response:
    return requests.get(url, headers=headers, timeout=timeout)


def _fetch_all_pages(
    url: str,
    per_page: int,
    headers: Dict[str, str],
    timeout: int,
) -> List[Dict[str, Any]]:
    per_page_filter = f"per_page={per_page}"
    all_data: List[Dict[str, Any]] = []
    page = 1
 
    logger.info(f"Начинаем загрузку всех страниц с {url} (per_page={per_page})")
 
    while True:
        page_filter = f"page={page}"
        paginated_url = f"{url}&{per_page_filter}&{page_filter}"
 
        logger.debug(f"Запрос страницы {page}: {paginated_url}")
 
        response = _fetch_page(paginated_url, headers, timeout)
        response.raise_for_status()
 
        data = response.json()
        received = len(data)
        logger.info(f"Страница {page}: получено {received} элементов")
 
        if not data:
            logger.info("Страница пуста, завершаем загрузку")
            break
 
        all_data.extend(data)
        logger.info(f"Всего загружено после страницы {page}: {len(all_data)}")
 
        if received < per_page:
            logger.info(f"Получено меньше ({received}) чем per_page ({per_page}), завершаем загрузку")
            break
 
        page += 1
 
    logger.info(f"Загрузка завершена. Итого загружено: {len(all_data)} элементов")
    return all_data


def extract_matches(begin_date: str, end_date: str, per_page: int = 100) -> List[Dict[str, Any]]:
    settings = get_pandascore_settings()
    headers = _build_headers(settings.token)
 
    logger.info(f"Начало извлечения матчей с {begin_date} по {end_date}, per_page={per_page}")
    range_filter = f"range[begin_at]={begin_date},{end_date}"
    matches_url = f"{settings.base_url}/matches?{range_filter}"
 
    matches_data = _fetch_all_pages(
        matches_url,
        per_page=per_page,
        headers=headers,
        timeout=settings.timeout_seconds,
    )
 
    logger.info(f"Извлечение завершено. Получено матчей: {len(matches_data)}")
    return matches_data


if __name__ == "__main__":
    matches = extract_matches("2026-06-19T00:00:00Z", "2026-06-20T00:00:00Z")
    print(f"Извлечено матчей: {len(matches)}")
    if matches:
        print("Первый матч: ", matches[0])

