-- Core Reference Tables - Created in dependency order
-- Table names match OOTP CSV export files exactly

-- Continents (referenced by nations)
CREATE TABLE IF NOT EXISTS continents (
    continent_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    abbreviation VARCHAR(10),
    demonym VARCHAR(30),
    population BIGINT,
    main_language_id INTEGER
);

-- Nations (top of geographic hierarchy)
CREATE TABLE IF NOT EXISTS nations (
    nation_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    short_name VARCHAR(50),
    abbreviation VARCHAR(10),
    demonym VARCHAR(50),
    population INTEGER,
    gender INTEGER,
    baseball_quality INTEGER,
    continent_id INTEGER,
    main_language_id INTEGER,
    quality_total INTEGER,
    capital_id INTEGER, -- Will be set after cities table exists
    use_hardcoded_ml_player_origins SMALLINT,
    this_is_the_usa SMALLINT,
    FOREIGN KEY (continent_id) REFERENCES continents(continent_id)
);

-- States/Provinces
CREATE TABLE IF NOT EXISTS states (
    state_id INTEGER,
    nation_id INTEGER,
    name VARCHAR(50) NOT NULL,
    abbreviation VARCHAR(10),
    population INTEGER,
    main_language_id INTEGER,
    PRIMARY KEY (state_id, nation_id),
    FOREIGN KEY (nation_id) REFERENCES nations(nation_id)
);

-- Cities
CREATE TABLE IF NOT EXISTS cities (
    city_id INTEGER PRIMARY KEY,
    nation_id INTEGER NOT NULL,
    state_id INTEGER,
    name VARCHAR(50) NOT NULL,
    abbreviation VARCHAR(50),
    latitude DECIMAL(10,7),
    longitude DECIMAL(11,7),
    population INTEGER,
    main_language_id INTEGER,
    FOREIGN KEY (nation_id) REFERENCES nations(nation_id)
);

-- Parks/Stadiums (extensive fields from OOTP)
CREATE TABLE IF NOT EXISTS parks (
    park_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    nation_id INTEGER,
    capacity INTEGER,
    type SMALLINT,
    foul_ground SMALLINT,
    turf SMALLINT,
    gender INTEGER,
    -- Field dimensions
    dimensions_x SMALLINT,
    dimensions_y SMALLINT,
    distances_0 SMALLINT,
    distances_1 SMALLINT,
    distances_2 SMALLINT,
    distances_3 SMALLINT,
    distances_4 SMALLINT,
    distances_5 SMALLINT,
    distances_6 SMALLINT,
    wall_heights_0 SMALLINT,
    wall_heights_1 SMALLINT,
    wall_heights_2 SMALLINT,
    wall_heights_3 SMALLINT,
    wall_heights_4 SMALLINT,
    wall_heights_5 SMALLINT,
    wall_heights_6 SMALLINT,
    -- Park factors
    avg DECIMAL(4,3),
    avg_l DECIMAL(4,3),
    avg_r DECIMAL(4,3),
    d DECIMAL(4,3),
    t DECIMAL(4,3),
    hr DECIMAL(4,3),
    hr_l DECIMAL(4,3),
    hr_r DECIMAL(4,3),
    -- Weather and other
    wind SMALLINT,
    wind_direction SMALLINT,
    temperature_0 SMALLINT,
    temperature_1 SMALLINT,
    temperature_2 SMALLINT,
    temperature_3 SMALLINT,
    temperature_4 SMALLINT,
    temperature_5 SMALLINT,
    temperature_6 SMALLINT,
    temperature_7 SMALLINT,
    temperature_8 SMALLINT,
    temperature_9 SMALLINT,
    temperature_10 SMALLINT,
    temperature_11 SMALLINT,
    rain_0 SMALLINT,
    rain_1 SMALLINT,
    rain_2 SMALLINT,
    rain_3 SMALLINT,
    rain_4 SMALLINT,
    rain_5 SMALLINT,
    rain_6 SMALLINT,
    rain_7 SMALLINT,
    rain_8 SMALLINT,
    rain_9 SMALLINT,
    rain_10 SMALLINT,
    rain_11 SMALLINT,
    picture VARCHAR(200),
    picture_night VARCHAR(200),
    home_team_dugout_is_at_first_base SMALLINT,
    FOREIGN KEY (nation_id) REFERENCES nations(nation_id)
);

-- League hierarchy
CREATE TABLE IF NOT EXISTS leagues (
    league_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    abbr VARCHAR(10),
    nation_id INTEGER,
    language_id INTEGER,
    gender INTEGER,
    historical_league SMALLINT,
    logo_file_name VARCHAR(200),
    start_date DATE,
    season_year INTEGER,
    historical_year SMALLINT,
    league_level SMALLINT,
    league_state SMALLINT,
    current_date DATE,
    background_color_id VARCHAR(8),
    text_color_id VARCHAR(8),
    FOREIGN KEY (nation_id) REFERENCES nations(nation_id)
);

CREATE TABLE IF NOT EXISTS sub_leagues (
    league_id INTEGER,
    sub_league_id INTEGER,
    name VARCHAR(50) NOT NULL,
    abbr VARCHAR(10),
    gender INTEGER,
    designated_hitter SMALLINT,
    PRIMARY KEY (league_id, sub_league_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);

CREATE TABLE IF NOT EXISTS divisions (
    league_id INTEGER,
    sub_league_id INTEGER,
    division_id INTEGER,
    name VARCHAR(50) NOT NULL,
    gender INTEGER,
    PRIMARY KEY (league_id, sub_league_id, division_id),
    FOREIGN KEY (league_id, sub_league_id) REFERENCES sub_leagues(league_id, sub_league_id)
);

-- Teams
CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    abbr VARCHAR(10),
    nickname VARCHAR(50),
    logo_file_name VARCHAR(200),
    city_id INTEGER,
    park_id INTEGER,
    league_id INTEGER,
    sub_league_id INTEGER,
    division_id INTEGER,
    nation_id INTEGER,
    parent_team_id INTEGER,
    level INTEGER,
    prevent_any_moves SMALLINT,
    human_team SMALLINT,
    human_id INTEGER,
    gender INTEGER,
    background_color_id VARCHAR(8),
    text_color_id VARCHAR(8),
    ballcaps_main_color_id VARCHAR(8),
    ballcaps_visor_color_id VARCHAR(8),
    jersey_main_color_id VARCHAR(8),
    jersey_away_color_id VARCHAR(8),
    jersey_secondary_color_id VARCHAR(8),
    jersey_pin_stripes_color_id VARCHAR(8),
    allstar_team SMALLINT,
    historical_id VARCHAR(50),
    FOREIGN KEY (park_id) REFERENCES parks(park_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id),
    FOREIGN KEY (nation_id) REFERENCES nations(nation_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id),
    FOREIGN KEY (parent_team_id) REFERENCES teams(team_id)
);

-- Team Relations (associates teams with divisions)
CREATE TABLE IF NOT EXISTS team_relations (
    team_id INTEGER,
    league_id INTEGER,
    sub_league_id INTEGER,
    division_id INTEGER,
    PRIMARY KEY (team_id, league_id, sub_league_id, division_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (league_id, sub_league_id, division_id) 
        REFERENCES divisions(league_id, sub_league_id, division_id)
);

-- Team Affiliations (parent/child relationships)
CREATE TABLE IF NOT EXISTS team_affiliations (
    team_id INTEGER,
    affiliated_team_id INTEGER,
    PRIMARY KEY (team_id, affiliated_team_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (affiliated_team_id) REFERENCES teams(team_id)
);

-- Languages (needed for various references)
CREATE TABLE IF NOT EXISTS languages (
    language_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

-- Insert special record for Free Agents
-- This will be done after initial data load:
-- INSERT INTO teams (team_id, name, abbr, league_id, level) 
-- VALUES (0, 'Free Agent', 'FA', 100, 1);

-- Add FK back to nations table after cities are loaded:
-- ALTER TABLE nations ADD CONSTRAINT fk_nations_capital 
--   FOREIGN KEY (capital_id) REFERENCES cities(city_id);