-- Calculation Support Tables
-- These tables store league-wide statistics needed for advanced metric calculations
-- Must be populated AFTER base statistics are loaded

-- League runs per out/PA (replaces LeagueRunsPerOut from original script)
CREATE TABLE IF NOT EXISTS league_runs_per_out (
    year INTEGER,
    league_id INTEGER,
    sub_league_id INTEGER,
    total_runs INTEGER,
    total_outs INTEGER,
    total_pa INTEGER,
    runs_per_out DECIMAL(8,6),
    runs_per_pa DECIMAL(8,6),
    PRIMARY KEY (year, league_id, sub_league_id),
    FOREIGN KEY (league_id, sub_league_id) 
        REFERENCES sub_leagues(league_id, sub_league_id)
);

-- Run values for offensive events (replaces tblRunValues, tblRunValues1A, tblRunValues2)
CREATE TABLE IF NOT EXISTS run_values (
    year INTEGER,
    league_id INTEGER,
    sub_league_id INTEGER,
    runs_per_out DECIMAL(8,6),
    -- Base run values
    run_bb DECIMAL(6,4),
    run_hbp DECIMAL(6,4),
    run_1b DECIMAL(6,4),
    run_2b DECIMAL(6,4),
    run_3b DECIMAL(6,4),
    run_hr DECIMAL(6,4),
    run_sb DECIMAL(6,4),
    run_cs DECIMAL(6,4),
    -- wOBA components
    run_minus DECIMAL(8,6),
    run_plus DECIMAL(8,6),
    woba DECIMAL(8,6),
    woba_scale DECIMAL(8,6),
    -- Weighted values for wOBA
    woba_bb DECIMAL(6,4),
    woba_hbp DECIMAL(6,4),
    woba_1b DECIMAL(6,4),
    woba_2b DECIMAL(6,4),
    woba_3b DECIMAL(6,4),
    woba_hr DECIMAL(6,4),
    woba_sb DECIMAL(6,4),
    woba_cs DECIMAL(6,4),
    PRIMARY KEY (year, league_id, sub_league_id),
    FOREIGN KEY (league_id, sub_league_id) 
        REFERENCES sub_leagues(league_id, sub_league_id)
);
ALTER TABLE run_values
    ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1,
    ADD COLUMN IF NOT EXISTS calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE run_values DROP CONSTRAINT run_values_pkey;
ALTER TABLE run_values
   ADD PRIMARY KEY (year, league_id, sub_league_id, version);


-- FIP constant for pitching calculations (replaces FIPConstant)
CREATE TABLE IF NOT EXISTS fip_constants (
    year INTEGER,
    league_id INTEGER,
    hr_fb_pct DECIMAL(6,4),
    adjusted_hr INTEGER,
    adjusted_bb INTEGER,
    adjusted_hp INTEGER,
    adjusted_k INTEGER,
    innings_pitched DECIMAL(8,1),
    league_era DECIMAL(5,2),
    fip_constant DECIMAL(5,2),
    PRIMARY KEY (year, league_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);
ALTER TABLE fip_constants
    ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1,
    ADD COLUMN IF NOT EXISTS calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE fip_constants DROP CONSTRAINT fip_constants_pkey;
ALTER TABLE fip_constants
    ADD PRIMARY KEY (year, league_id, version);


-- Sub-league batting environment (replaces sub_league_history_batting)
CREATE TABLE IF NOT EXISTS sub_league_batting_environment (
    year INTEGER,
    league_id INTEGER,
    sub_league_id INTEGER,
    total_pa INTEGER,
    total_runs INTEGER,
    runs_per_pa DECIMAL(8,6),
    PRIMARY KEY (year, league_id, sub_league_id),
    FOREIGN KEY (league_id, sub_league_id) 
        REFERENCES sub_leagues(league_id, sub_league_id)
);

-- Sub-league pitching environment (replaces sub_league_history_pitching)
CREATE TABLE IF NOT EXISTS sub_league_pitching_environment (
    year INTEGER,
    league_id INTEGER,
    sub_league_id INTEGER,
    total_ip DECIMAL(8,1),
    total_er INTEGER,
    adjusted_hra INTEGER,
    adjusted_bb INTEGER,
    adjusted_hp INTEGER,
    adjusted_k INTEGER,
    league_era DECIMAL(5,2),
    league_fip DECIMAL(5,2),
    PRIMARY KEY (year, league_id, sub_league_id),
    FOREIGN KEY (league_id, sub_league_id) 
        REFERENCES sub_leagues(league_id, sub_league_id)
);

-- Position definitions (for defensive calculations and display)
CREATE TABLE IF NOT EXISTS positions (
    position_id SMALLINT PRIMARY KEY,
    position_name VARCHAR(20),
    position_abbr VARCHAR(3),
    defensive_spectrum_value INTEGER -- For positional adjustments
);

-- Team batting stats (from team_batting_stats.csv)
CREATE TABLE IF NOT EXISTS team_batting_stats (
    team_id INTEGER,
    year INTEGER,
    league_id INTEGER,
    level_id INTEGER,
    split_id INTEGER,
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
    avg DECIMAL(4,3),
    obp DECIMAL(4,3),
    slg DECIMAL(4,3),
    rc DECIMAL(6,1),
    rc27 DECIMAL(5,2),
    iso DECIMAL(4,3),
    woba DECIMAL(4,3),
    ops DECIMAL(4,3),
    sbp DECIMAL(4,3),
    ws INTEGER,
    PRIMARY KEY (team_id, year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);

-- Team pitching stats (from team_pitching_stats.csv)
CREATE TABLE IF NOT EXISTS team_pitching_stats (
    team_id INTEGER,
    year INTEGER,
    league_id INTEGER,
    level_id INTEGER,
    split_id INTEGER,
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
    cg INTEGER,
    sho INTEGER,
    sb INTEGER,
    cs INTEGER,
    hld INTEGER,
    r9 DECIMAL(5,2),
    avg DECIMAL(4,3),
    obp DECIMAL(4,3),
    slg DECIMAL(4,3),
    ops DECIMAL(4,3),
    h9 DECIMAL(5,2),
    k9 DECIMAL(5,2),
    hr9 DECIMAL(5,2),
    bb9 DECIMAL(5,2),
    cgp DECIMAL(4,3),
    fip DECIMAL(5,2),
    qsp DECIMAL(4,3),
    winp DECIMAL(4,3),
    rsg DECIMAL(5,2),
    svp DECIMAL(4,3),
    bsvp DECIMAL(4,3),
    gfp DECIMAL(4,3),
    era DECIMAL(5,2),
    pig DECIMAL(5,2),
    ws INTEGER,
    whip DECIMAL(4,2),
    gbfbp DECIMAL(4,3),
    kbb DECIMAL(4,2),
    babip DECIMAL(4,3),
    PRIMARY KEY (team_id, year),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);

-- Insert position data
INSERT INTO positions VALUES 
    (1, 'Pitcher', 'P', 0),
    (2, 'Catcher', 'C', 12),
    (3, 'First Base', '1B', -12),
    (4, 'Second Base', '2B', 3),
    (5, 'Third Base', '3B', 2),
    (6, 'Shortstop', 'SS', 7),
    (7, 'Left Field', 'LF', -7),
    (8, 'Center Field', 'CF', 2),
    (9, 'Right Field', 'RF', -7),
    (10, 'Designated Hitter', 'DH', -17)
ON CONFLICT (position_id) DO NOTHING;
