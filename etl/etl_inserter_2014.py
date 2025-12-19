import duckdb

SQL_PIPELINE = """
DROP SEQUENCE IF EXISTS match_id_seq;
CREATE SEQUENCE match_id_seq START 1;

INSERT INTO Teams (team_id, team_name)
SELECT uuid(), team_name
FROM (
    SELECT DISTINCT "Home Team Name" AS team_name FROM staging_matches
    UNION
    SELECT DISTINCT "Away Team Name" FROM staging_matches
);

INSERT INTO Rounds (round_id, round_name)
SELECT uuid(), Stage
FROM staging_matches
GROUP BY Stage;

INSERT INTO City (city_id, city_name)
SELECT uuid(), City
FROM staging_matches
GROUP BY City;

INSERT INTO MatchTime (time_id, date_, day_, month_, year_)
SELECT
  uuid(),
  CAST("Datetime" AS TIMESTAMP),
  EXTRACT(DAY FROM "Datetime"),
  EXTRACT(MONTH FROM "Datetime"),
  EXTRACT(YEAR FROM "Datetime")
FROM staging_matches
GROUP BY CAST("Datetime" AS TIMESTAMP), EXTRACT(DAY FROM "Datetime"), 
         EXTRACT(MONTH FROM "Datetime"), EXTRACT(YEAR FROM "Datetime");

CREATE TEMP TABLE match_map AS
SELECT
  nextval('match_id_seq')::INTEGER AS match_id,
  s.*
FROM (
    SELECT DISTINCT
        "Datetime",
        "Stage",
        "City",
        "Home Team Name",
        "Away Team Name",
        "Home Team Goals",
        "Away Team Goals",
        "Home result",
        "Away result"
    FROM staging_matches
) s
ORDER BY "Datetime";

INSERT INTO Matches (match_id, round_id, city_id, time_id)
SELECT
  m.match_id,
  r.round_id,
  c.city_id,
  t.time_id
FROM match_map m
JOIN Rounds r ON r.round_name = m."Stage"
JOIN City c ON c.city_name = m."City"
JOIN MatchTime t ON t.date_ = CAST(m."Datetime" AS TIMESTAMP);

INSERT INTO Plays (match_id, team_id, position_, goal_nb, result_)
SELECT
  m.match_id,
  th.team_id,
  'home',
  m."Home Team Goals",
  m."Home result"
FROM match_map m
JOIN Teams th ON th.team_name = m."Home Team Name";

INSERT INTO Plays (match_id, team_id, position_, goal_nb, result_)
SELECT
  m.match_id,
  ta.team_id,
  'away',
  m."Away Team Goals",
  m."Away result"
FROM match_map m
JOIN Teams ta ON ta.team_name = m."Away Team Name";

DROP TABLE match_map;
"""

TRUNCATE_ALL = """
TRUNCATE TABLE Plays;
TRUNCATE TABLE Matches;
TRUNCATE TABLE MatchTime;
TRUNCATE TABLE City;
TRUNCATE TABLE Rounds;
TRUNCATE TABLE Teams;
"""

def load_matches(df, db_path="./../db/db.duckdb"):
    con = duckdb.connect(db_path)
    con.register("staging_matches", df)
    con.execute(TRUNCATE_ALL)
    con.commit()
    con.execute(SQL_PIPELINE)
    con.commit()
    con.unregister("staging_matches")
    con.close()