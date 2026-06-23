import logging
from typing import List, Dict, Set
from datetime import datetime
from etl.models import (
    Videogame, League, Serie, Tournament, Team,
    Match, MatchOpponent
)

from common.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def _parse_datetime(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, str):
        value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    return value


def _dedup_by_id(items: List[dict]) -> List[dict]:
    seen: Set[int] = set()
    result = []
    for item in items:
        item_id = item.get("id")
        if item_id and item_id not in seen:
            seen.add(item_id)
            result.append(item)
    return result


def _log_first_object(entity_name: str, objects: List):
    if objects:
        logger.info(f"[{entity_name}] Первый объект: {objects[0]}")
    else:
        logger.warning(f"[{entity_name}] Нет распарсенных объектов!")
    logger.info(f"[{entity_name}] Всего распарсено: {len(objects)}")


def parse_videogames(raw_matches: List[dict]) -> List[Videogame]:
    raw_list = [m["videogame"] for m in raw_matches if m.get("videogame")]
    raw_list = _dedup_by_id(raw_list)
    
    result = [
        Videogame(
            id=raw["id"],
            name=raw.get("name", ""),
            slug=raw.get("slug")
        )
        for raw in raw_list
    ]
    
    _log_first_object("videogames", result)
    return result


def parse_leagues(raw_matches: List[dict]) -> List[League]:
    raw_list = [m["league"] for m in raw_matches if m.get("league")]
    raw_list = _dedup_by_id(raw_list)
    
    league_to_videogame: Dict[int, int] = {}
    for m in raw_matches:
        league_id = m.get("league", {}).get("id")
        videogame_id = m.get("videogame", {}).get("id")
        if league_id and videogame_id:
            league_to_videogame[league_id] = videogame_id
    
    result = [
        League(
            id=raw["id"],
            name=raw.get("name", ""),
            slug=raw.get("slug"),
            videogame_id=league_to_videogame.get(raw["id"])
        )
        for raw in raw_list
    ]
    
    _log_first_object("leagues", result)
    return result


def parse_series(raw_matches: List[dict]) -> List[Serie]:
    raw_list = [m["serie"] for m in raw_matches if m.get("serie")]
    raw_list = _dedup_by_id(raw_list)
    
    result = [
        Serie(
            id=raw["id"],
            name=raw.get("name", ""),
            league_id=raw.get("league_id"),
            begin_at=_parse_datetime(raw.get("begin_at")),
            end_at=_parse_datetime(raw.get("end_at"))
        )
        for raw in raw_list
    ]
    
    _log_first_object("series", result)
    return result


def parse_tournaments(raw_matches: List[dict]) -> List[Tournament]:
    raw_list = [m["tournament"] for m in raw_matches if m.get("tournament")]
    raw_list = _dedup_by_id(raw_list)
    
    result = [
        Tournament(
            id=raw["id"],
            name=raw.get("name", ""),
            serie_id=raw.get("serie_id"),
            begin_at=_parse_datetime(raw.get("begin_at")),
            end_at=_parse_datetime(raw.get("end_at"))
        )
        for raw in raw_list
    ]
    
    _log_first_object("tournaments", result)
    return result


def parse_teams(raw_matches: List[dict]) -> List[Team]:
    raw_list = []
    for match in raw_matches:
        for opp in match.get("opponents", []):
            team = opp.get("opponent")
            if team:
                raw_list.append(team)
    
    raw_list = _dedup_by_id(raw_list)
    
    team_to_videogame: Dict[int, int] = {}
    for m in raw_matches:
        videogame_id = m.get("videogame", {}).get("id")
        for opp in m.get("opponents", []):
            team_id = opp.get("opponent", {}).get("id")
            if team_id and videogame_id:
                team_to_videogame[team_id] = videogame_id
    
    result = [
        Team(
            id=raw["id"],
            name=raw.get("name", ""),
            slug=raw.get("slug"),
            acronym=raw.get("acronym"),
            videogame_id=team_to_videogame.get(raw["id"])
        )
        for raw in raw_list
    ]
    
    _log_first_object("teams", result)
    return result


def parse_matches(raw_matches: List[dict]) -> List[Match]:
    result = []
    
    for raw in raw_matches:
        winner = raw.get("winner")
        winner_id = winner.get("id") if winner else None
        
        result.append(Match(
            id=raw["id"],
            name=raw.get("name", ""),
            status=raw.get("status", ""),
            begin_at=_parse_datetime(raw.get("begin_at")),
            end_at=_parse_datetime(raw.get("end_at")),
            tournament_id=raw.get("tournament", {}).get("id"),
            videogame_id=raw.get("videogame", {}).get("id"),
            winner_id=winner_id,
            number_of_games=len(raw.get("games", []))
        ))
    
    _log_first_object("matches", result)
    return result


def parse_opponents(raw_matches: List[dict]) -> List[MatchOpponent]:
    result = []
    
    for raw in raw_matches:
        match_id = raw["id"]
        winner = raw.get("winner")
        winner_id = winner.get("id") if winner else None
        
        for opp in raw.get("opponents", []):
            team = opp.get("opponent", {})
            team_id = team.get("id")
            
            if not team_id:
                continue
            
            result.append(MatchOpponent(
                match_id=match_id,
                team_id=team_id,
                score=opp.get("score", 0),
                is_winner=1 if team_id == winner_id else 0
            ))
    
    _log_first_object("match_opponents", result)
    return result


def transform(raw_matches: List[dict]) -> Dict[str, List]:
    logger.info(f"Начало трансформации {len(raw_matches)} сырых матчей")
    
    result = {
        "videogames": parse_videogames(raw_matches),
        "leagues": parse_leagues(raw_matches),
        "series": parse_series(raw_matches),
        "tournaments": parse_tournaments(raw_matches),
        "teams": parse_teams(raw_matches),
        "matches": parse_matches(raw_matches),
        "match_opponents": parse_opponents(raw_matches),
    }
    
    total_objects = sum(len(v) for v in result.values())
    logger.info(f"Трансформация завершена. Всего объектов: {total_objects}")
    
    return result


if __name__ == "__main__":
    import json

    with open("tests/content/matches.json") as file:
        matches_list = json.load(file) 

    objects = transform(matches_list)

    
