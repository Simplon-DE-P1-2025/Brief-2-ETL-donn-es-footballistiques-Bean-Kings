import duckdb

con = duckdb.connect("./../db/db.duckdb")

schema_sql = """
CREATE TABLE Teams (
    team_id UUID PRIMARY KEY,
    team_name TEXT NOT NULL
);

CREATE TABLE MatchTime (
    time_id UUID PRIMARY KEY,
    date_ DATE,
    day_ INTEGER,
    month_ INTEGER,
    year_ INTEGER
);

CREATE TABLE Rounds (
    round_id UUID PRIMARY KEY,
    round_name TEXT NOT NULL
);

CREATE TABLE City (
    city_id UUID PRIMARY KEY,
    city_name TEXT NOT NULL
);

CREATE TABLE Matches (
    match_id INTEGER PRIMARY KEY,
    round_id UUID NOT NULL,
    city_id UUID NOT NULL,
    time_id UUID NOT NULL,
    FOREIGN KEY (round_id) REFERENCES Rounds(round_id),
    FOREIGN KEY (city_id) REFERENCES City(city_id),
    FOREIGN KEY (time_id) REFERENCES MatchTime(time_id)
);

CREATE TABLE Plays (
    match_id INTEGER,
    team_id UUID,
    position_ TEXT,
    goal_nb INTEGER,
    result_ TEXT,
    PRIMARY KEY (match_id, team_id),
    FOREIGN KEY (match_id) REFERENCES Matches(match_id),
    FOREIGN KEY (team_id) REFERENCES Teams(team_id)
);
"""

con.execute(schema_sql)
con.close()
