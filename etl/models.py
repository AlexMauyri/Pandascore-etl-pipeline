from datetime import datetime
 
from pydantic import BaseModel
 
 
class Videogame(BaseModel):
    id: int
    name: str
    slug: str | None = None
 
 
class League(BaseModel):
    id: int
    name: str
    slug: str | None = None
    videogame_id: int | None = None
 
 
class Serie(BaseModel):
    id: int
    name: str
    league_id: int | None = None
    begin_at: datetime | None = None
    end_at: datetime | None = None
 
 
class Tournament(BaseModel):
    id: int
    name: str
    serie_id: int | None = None
    begin_at: datetime | None = None
    end_at: datetime | None = None
 
 
class Team(BaseModel):
    id: int
    name: str
    slug: str | None = None
    acronym: str | None = None
    videogame_id: int | None = None
 
 
class Match(BaseModel):
    id: int
    name: str
    status: str
    begin_at: datetime | None = None
    end_at: datetime | None = None
    tournament_id: int | None = None
    videogame_id: int | None = None
    winner_id: int | None = None
    number_of_games: int = 0
 
 
class MatchOpponent(BaseModel):
    match_id: int
    team_id: int
    score: int = 0
    is_winner: int = 0

