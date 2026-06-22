CREATE DATABASE IF NOT EXISTS etl;

CREATE TABLE etl.videogames (
    id UInt32,
    name String,
    slug String
) ENGINE = ReplacingMergeTree()
ORDER BY id;

CREATE TABLE etl.leagues (
    id UInt32,
    name String,
    slug String,
    videogame_id UInt32
) ENGINE = ReplacingMergeTree()
ORDER BY id;

CREATE TABLE etl.series (
    id UInt32,
    name String,
    league_id UInt32,
    begin_at DateTime,
    end_at Nullable(DateTime)
) ENGINE = ReplacingMergeTree()
ORDER BY id;

CREATE TABLE etl.tournaments (
    id UInt32,
    name String,
    serie_id UInt32,
    begin_at DateTime,
    end_at Nullable(DateTime)
) ENGINE = ReplacingMergeTree()
ORDER BY id;

CREATE TABLE etl.teams (
    id UInt32,
    name String,
    slug String,
    acronym Nullable(String),
    videogame_id UInt32
) ENGINE = ReplacingMergeTree()
ORDER BY id;

CREATE TABLE etl.matches (
    id UInt32,
    name String,
    status String,
    begin_at DateTime,
    end_at Nullable(DateTime),
    tournament_id UInt32,
    videogame_id UInt32,
    winner_id Nullable(UInt32),
    number_of_games UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY id;

CREATE TABLE etl.match_opponents (
    match_id UInt32,
    team_id UInt32,
    score UInt8,
    is_winner UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY (match_id, team_id);

CREATE TABLE etl.etl_run_log (
    dag_run_id String,
    task_id String,
    started_at DateTime,
    finished_at Nullable(DateTime),
    status String,
    rows_extracted UInt32,
    rows_loaded UInt32,
    error_message Nullable(String)
) ENGINE = MergeTree()
ORDER BY started_at;

CREATE TABLE etl.etl_state (
    entity_name String,
    last_loaded_at DateTime,
    last_id UInt32
) ENGINE = ReplacingMergeTree()
ORDER BY entity_name;
