import duckdb

VIEW = """ 
CREATE OR REPLACE VIEW v_matches_flat AS
SELECT
    m.match_id,

    mt.date_       AS match_date,
    mt.day_        AS day_,
    mt.month_      AS month_,
    mt.year_       AS year_,

    r.round_name   AS stage,
    c.city_name    AS city,

    th.team_name   AS home_team,
    ph.goal_nb     AS home_goals,
    ph.result_     AS home_result,

    ta.team_name   AS away_team,
    pa.goal_nb     AS away_goals,
    pa.result_     AS away_result

FROM Matches m

JOIN MatchTime mt ON mt.time_id = m.time_id
JOIN Rounds r     ON r.round_id = m.round_id
JOIN City c       ON c.city_id = m.city_id

JOIN Plays ph ON ph.match_id = m.match_id AND ph.position_ = 'home'
JOIN Teams th ON th.team_id = ph.team_id

JOIN Plays pa ON pa.match_id = m.match_id AND pa.position_ = 'away'
JOIN Teams ta ON ta.team_id = pa.team_id;
"""


def create_view(db_path="./../db/db.duckdb"):
    con = duckdb.connect(db_path)
    con.execute(VIEW)
    con.commit()
    con.close()