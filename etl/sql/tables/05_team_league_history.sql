-- Team and League History Tables
-- Matching OOTP CSV export files for historical data and current rosters
-- These tables support team pages, league pages, and roster displays

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS team_roster_staff CASCADE;
DROP TABLE IF EXISTS team_roster CASCADE;
DROP TABLE IF EXISTS team_history_record CASCADE;
DROP TABLE IF EXISTS team_history_pitching_stats CASCADE;
DROP TABLE IF EXISTS team_history_batting_stats CASCADE;
DROP TABLE IF EXISTS team_history CASCADE;
DROP TABLE IF EXISTS league_history_pitching_stats CASCADE;
DROP TABLE IF EXISTS league_history_batting_stats CASCADE;
DROP TABLE IF EXISTS league_history CASCADE;

-- =====================================================
-- League History Tables
-- =====================================================

-- League Awards by Year
-- Awards for best hitter, pitcher, rookie, manager, and fielders by position
CREATE TABLE league_history (
    league_id INTEGER NOT NULL,
    sub_league_id INTEGER NOT NULL,
    year SMALLINT NOT NULL,
    best_hitter_id INTEGER,
    best_pitcher_id INTEGER,
    best_rookie_id INTEGER,
    best_manager_id INTEGER,
    best_fielder_id0 INTEGER,  -- Pitcher
    best_fielder_id1 INTEGER,  -- Catcher
    best_fielder_id2 INTEGER,  -- 1B
    best_fielder_id3 INTEGER,  -- 2B
    best_fielder_id4 INTEGER,  -- 3B
    best_fielder_id5 INTEGER,  -- SS
    best_fielder_id6 INTEGER,  -- LF
    best_fielder_id7 INTEGER,  -- CF
    best_fielder_id8 INTEGER,  -- RF
    best_fielder_id9 INTEGER,  -- DH
    PRIMARY KEY (league_id, sub_league_id, year),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id),
    FOREIGN KEY (best_hitter_id) REFERENCES players_core(player_id),
    FOREIGN KEY (best_pitcher_id) REFERENCES players_core(player_id),
    FOREIGN KEY (best_rookie_id) REFERENCES players_core(player_id)
);

COMMENT ON TABLE league_history IS 'League awards by year - best players at each position';

-- League-wide Batting Statistics by Year
-- Used for league pages and historical comparisons
CREATE TABLE league_history_batting_stats (
    year SMALLINT NOT NULL,
    team_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    league_id INTEGER NOT NULL,
    level_id SMALLINT NOT NULL,
    split_id SMALLINT NOT NULL,
    pa INTEGER,
    ab INTEGER,
    h INTEGER,
    k INTEGER,
    tb INTEGER,
    s INTEGER,
    d INTEGER,
    t INTEGER,
    hr INTEGER,
    sb INTEGER,
    cs INTEGER,
    rbi INTEGER,
    r INTEGER,
    bb INTEGER,
    ibb INTEGER,
    hp INTEGER,
    sh INTEGER,
    sf INTEGER,
    ci INTEGER,
    gdp INTEGER,
    g INTEGER,
    gs INTEGER,
    ebh INTEGER,
    pitches_seen INTEGER,
    avg DECIMAL(6,4),
    obp DECIMAL(6,4),
    slg DECIMAL(6,4),
    rc DECIMAL(10,4),
    rc27 DECIMAL(8,4),
    iso DECIMAL(6,4),
    woba DECIMAL(6,4),
    ops DECIMAL(6,4),
    sbp DECIMAL(8,4),
    kp DECIMAL(6,4),
    bbp DECIMAL(6,4),
    wpa DECIMAL(10,3),
    babip DECIMAL(6,4),
    PRIMARY KEY (year, team_id, game_id, league_id, level_id, split_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

COMMENT ON TABLE league_history_batting_stats IS 'League-wide batting statistics for historical reporting';

-- League-wide Pitching Statistics by Year
-- Used for league pages and historical comparisons
CREATE TABLE league_history_pitching_stats (
    year SMALLINT NOT NULL,
    team_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    league_id INTEGER NOT NULL,
    level_id SMALLINT NOT NULL,
    split_id SMALLINT NOT NULL,
    ab INTEGER,
    ip INTEGER,
    bf INTEGER,
    tb INTEGER,
    ha INTEGER,
    k INTEGER,
    rs INTEGER,
    bb INTEGER,
    r INTEGER,
    er INTEGER,
    gb INTEGER,
    fb INTEGER,
    pi INTEGER,
    ipf INTEGER,
    g INTEGER,
    gs INTEGER,
    w INTEGER,
    l INTEGER,
    s INTEGER,
    sa INTEGER,
    da INTEGER,
    sh INTEGER,
    sf INTEGER,
    ta INTEGER,
    hra INTEGER,
    bk INTEGER,
    ci INTEGER,
    iw INTEGER,
    wp INTEGER,
    hp INTEGER,
    gf INTEGER,
    dp INTEGER,
    qs INTEGER,
    svo INTEGER,
    bs INTEGER,
    ra INTEGER,
    ir DECIMAL(10,3),
    irs DECIMAL(10,3),
    cg INTEGER,
    sho INTEGER,
    sb INTEGER,
    cs INTEGER,
    hld INTEGER,
    r9 DECIMAL(8,4),
    avg DECIMAL(6,4),
    obp DECIMAL(6,4),
    slg DECIMAL(6,4),
    ops DECIMAL(6,4),
    h9 DECIMAL(8,4),
    k9 DECIMAL(8,4),
    kp DECIMAL(6,4),
    bbp DECIMAL(6,4),
    kbbp DECIMAL(8,4),
    hr9 DECIMAL(8,4),
    bb9 DECIMAL(8,4),
    cgp DECIMAL(6,4),
    fip DECIMAL(8,2),
    qsp DECIMAL(6,4),
    winp DECIMAL(6,4),
    rsg DECIMAL(8,4),
    svp DECIMAL(6,4),
    bsvp DECIMAL(6,4),
    irsp DECIMAL(6,4),
    gfp DECIMAL(6,4),
    era DECIMAL(8,2),
    pig DECIMAL(10,4),
    ws DECIMAL(8,3),
    whip DECIMAL(8,4),
    gbfbp DECIMAL(6,4),
    kbb DECIMAL(8,4),
    babip DECIMAL(6,4),
    wpa DECIMAL(10,3),
    war DECIMAL(8,3),
    ra9war DECIMAL(8,3),
    sd INTEGER,
    md INTEGER,
    PRIMARY KEY (year, team_id, game_id, league_id, level_id, split_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

COMMENT ON TABLE league_history_pitching_stats IS 'League-wide pitching statistics for historical reporting';

-- =====================================================
-- Team History Tables
-- =====================================================

-- Team Identity and Performance by Year
-- Tracks team name changes, best players, playoff appearances
CREATE TABLE team_history (
    team_id INTEGER NOT NULL,
    year SMALLINT NOT NULL,
    league_id INTEGER NOT NULL,
    sub_league_id INTEGER NOT NULL,
    division_id INTEGER NOT NULL,
    name VARCHAR(100),
    abbr VARCHAR(10),
    nickname VARCHAR(50),
    best_hitter_id INTEGER,
    best_pitcher_id INTEGER,
    best_rookie_id INTEGER,
    manager_id INTEGER,
    made_playoffs SMALLINT,
    won_playoffs SMALLINT,
    fired SMALLINT,
    position_in_division SMALLINT,
    PRIMARY KEY (team_id, year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id),
    FOREIGN KEY (best_hitter_id) REFERENCES players_core(player_id),
    FOREIGN KEY (best_pitcher_id) REFERENCES players_core(player_id),
    FOREIGN KEY (best_rookie_id) REFERENCES players_core(player_id)
);

COMMENT ON TABLE team_history IS 'Team identity changes and yearly performance summary';

-- Team Yearly Batting Statistics
-- Aggregated team batting performance by season
CREATE TABLE team_history_batting_stats (
    team_id INTEGER NOT NULL,
    year SMALLINT NOT NULL,
    league_id INTEGER NOT NULL,
    sub_league_id INTEGER NOT NULL,
    division_id INTEGER NOT NULL,
    level_id SMALLINT NOT NULL,
    split_id SMALLINT NOT NULL,
    pa INTEGER,
    ab INTEGER,
    h INTEGER,
    k INTEGER,
    tb INTEGER,
    s INTEGER,
    d INTEGER,
    t INTEGER,
    hr INTEGER,
    sb INTEGER,
    cs INTEGER,
    rbi INTEGER,
    r INTEGER,
    bb INTEGER,
    ibb INTEGER,
    hp INTEGER,
    sh INTEGER,
    sf INTEGER,
    ci INTEGER,
    gdp INTEGER,
    g SMALLINT,
    gs SMALLINT,
    ebh INTEGER,
    pitches_seen INTEGER,
    avg DECIMAL(5,4),
    obp DECIMAL(5,4),
    slg DECIMAL(5,4),
    rc DECIMAL(8,4),
    rc27 DECIMAL(6,4),
    iso DECIMAL(5,4),
    woba DECIMAL(5,4),
    ops DECIMAL(5,4),
    sbp DECIMAL(6,4),
    PRIMARY KEY (team_id, year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);

COMMENT ON TABLE team_history_batting_stats IS 'Team yearly batting statistics totals';

-- Team Yearly Pitching Statistics
-- Aggregated team pitching performance by season
CREATE TABLE team_history_pitching_stats (
    team_id INTEGER NOT NULL,
    year SMALLINT NOT NULL,
    league_id INTEGER NOT NULL,
    sub_league_id INTEGER NOT NULL,
    division_id INTEGER NOT NULL,
    level_id SMALLINT NOT NULL,
    split_id SMALLINT NOT NULL,
    ab INTEGER,
    ip SMALLINT,
    bf INTEGER,
    tb INTEGER,
    ha INTEGER,
    k INTEGER,
    rs INTEGER,
    bb INTEGER,
    r INTEGER,
    er INTEGER,
    gb INTEGER,
    fb INTEGER,
    pi INTEGER,
    ipf SMALLINT,
    g SMALLINT,
    gs SMALLINT,
    w SMALLINT,
    l SMALLINT,
    s SMALLINT,
    sa INTEGER,
    da INTEGER,
    sh INTEGER,
    sf INTEGER,
    ta INTEGER,
    hra INTEGER,
    bk INTEGER,
    ci INTEGER,
    iw INTEGER,
    wp INTEGER,
    hp INTEGER,
    gf SMALLINT,
    dp INTEGER,
    qs SMALLINT,
    svo SMALLINT,
    bs SMALLINT,
    ra INTEGER,
    cg SMALLINT,
    sho SMALLINT,
    sb INTEGER,
    cs INTEGER,
    hld SMALLINT,
    r9 DECIMAL(6,4),
    avg DECIMAL(5,4),
    obp DECIMAL(5,4),
    slg DECIMAL(5,4),
    ops DECIMAL(5,4),
    h9 DECIMAL(6,4),
    k9 DECIMAL(6,4),
    hr9 DECIMAL(6,4),
    bb9 DECIMAL(6,4),
    cgp DECIMAL(5,4),
    fip DECIMAL(5,2),
    qsp DECIMAL(5,4),
    winp DECIMAL(5,4),
    rsg DECIMAL(6,4),
    svp DECIMAL(5,4),
    bsvp DECIMAL(5,4),
    gfp DECIMAL(5,4),
    era DECIMAL(5,2),
    pig DECIMAL(8,4),
    ws DECIMAL(6,3),
    whip DECIMAL(5,4),
    gbfbp DECIMAL(5,4),
    kbb DECIMAL(6,4),
    babip DECIMAL(5,4),
    PRIMARY KEY (team_id, year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);

COMMENT ON TABLE team_history_pitching_stats IS 'Team yearly pitching statistics totals';

-- Team Historical Standings/Record
-- Win/loss records and playoff results by season
CREATE TABLE team_history_record (
    team_id INTEGER NOT NULL,
    year SMALLINT NOT NULL,
    league_id INTEGER NOT NULL,
    sub_league_id INTEGER NOT NULL,
    division_id INTEGER NOT NULL,
    g SMALLINT,
    w SMALLINT,
    l SMALLINT,
    t SMALLINT,
    pos SMALLINT,
    pct DECIMAL(6,4),
    gb DECIMAL(7,4),
    streak SMALLINT,
    magic_number SMALLINT,
    PRIMARY KEY (team_id, year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);

COMMENT ON TABLE team_history_record IS 'Team yearly standings and win/loss records';

-- =====================================================
-- Current Roster Tables
-- =====================================================

-- Team Current Player Roster
-- Players on each team's active roster for current season
CREATE TABLE team_roster (
    team_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    list_id SMALLINT,
    PRIMARY KEY (team_id, player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (player_id) REFERENCES players_core(player_id)
);

COMMENT ON TABLE team_roster IS 'Current team rosters - active players';
COMMENT ON COLUMN team_roster.list_id IS 'Roster order/position in list';

-- Team Current Staff Roster
-- Coaching staff and front office for each team (denormalized)
CREATE TABLE team_roster_staff (
    team_id INTEGER PRIMARY KEY,
    head_scout INTEGER,
    manager INTEGER,
    general_manager INTEGER,
    pitching_coach INTEGER,
    hitting_coach INTEGER,
    bench_coach INTEGER,
    owner INTEGER,
    doctor INTEGER,
    first_base_coach INTEGER,
    third_base_coach INTEGER,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
    -- Note: coach IDs reference coaches table when that table exists
);

COMMENT ON TABLE team_roster_staff IS 'Current team staff rosters - denormalized for 11 fixed positions';

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_league_history_year ON league_history(year DESC);
CREATE INDEX IF NOT EXISTS idx_league_history_league ON league_history(league_id, sub_league_id);

CREATE INDEX IF NOT EXISTS idx_team_history_year ON team_history(year DESC);
CREATE INDEX IF NOT EXISTS idx_team_history_team ON team_history(team_id);
CREATE INDEX IF NOT EXISTS idx_team_history_league ON team_history(league_id, year);

CREATE INDEX IF NOT EXISTS idx_team_history_batting_year ON team_history_batting_stats(year DESC);
CREATE INDEX IF NOT EXISTS idx_team_history_pitching_year ON team_history_pitching_stats(year DESC);
CREATE INDEX IF NOT EXISTS idx_team_history_record_year ON team_history_record(year DESC);

CREATE INDEX IF NOT EXISTS idx_team_roster_player ON team_roster(player_id);
CREATE INDEX IF NOT EXISTS idx_team_roster_team ON team_roster(team_id);
