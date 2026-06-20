from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class Videogame(BaseModel):
    id: int
    name: str
    slug: Optional[str]

class League(BaseModel):
    id: int
    name: str
    slug: Optional[str]
    videogame_id: Optional[int]

class Serie(BaseModel):
    id: int
    name: str
    league_id: Optional[int]
    begin_at: Optional[datetime]
    end_at: Optional[datetime]

class Tournament(BaseModel):
    id: int
    name: str
    serie_id: Optional[int]
    begin_at: Optional[datetime]
    end_at: Optional[datetime]

class Team(BaseModel):
    id: int
    name: str
    slug: Optional[str]
    acronym: Optional[str]
    videogame_id: Optional[int]

class Match(BaseModel):
    id: int
    name: str
    status: str
    begin_at: Optional[datetime]
    end_at: Optional[datetime]
    tournament_id: Optional[int]
    videogame_id: Optional[int]
    winner_id: Optional[int]
    number_of_games: int = 0

class MatchOpponent(BaseModel):
    match_id: int
    team_id: int
    score: int = 0
    is_winner: int = 0
