-- Core Reference Tables - Created in dependency order
-- Table names match OOTP CSV export files exactly


  -- Drop tables in reverse dependency order
DROP TABLE IF EXISTS team_affiliations CASCADE;
DROP TABLE IF EXISTS team_relations CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS divisions CASCADE;
DROP TABLE IF EXISTS sub_leagues CASCADE;
DROP TABLE IF EXISTS leagues CASCADE;
DROP TABLE IF EXISTS parks CASCADE;
DROP TABLE IF EXISTS languages CASCADE;
DROP TABLE IF EXISTS cities CASCADE;
DROP TABLE IF EXISTS states CASCADE;
DROP TABLE IF EXISTS nations CASCADE;
DROP TABLE IF EXISTS continents CASCADE;


-- Continents (referenced by nations)
CREATE TABLE continents (
    continent_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    abbreviation VARCHAR(10),
    demonym VARCHAR(30),
    population BIGINT,
    main_language_id INTEGER
);

-- Nations (top of geographic hierarchy)
CREATE TABLE nations (
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
-- Add nation_id 0 to nations table

-- States/Provinces
CREATE TABLE states (
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
CREATE TABLE cities (
    city_id INTEGER PRIMARY KEY,
    nation_id INTEGER NOT NULL,
    state_id INTEGER,
    name VARCHAR(100) NOT NULL,
    abbreviation VARCHAR(50),
    population INTEGER,
    main_language_id INTEGER,
    FOREIGN KEY (nation_id) REFERENCES nations(nation_id)
);

-- Parks/Stadiums (Simplified essential fields only)

CREATE TABLE parks (
  park_id INTEGER PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  nation_id INTEGER,
  capacity INTEGER,
  type SMALLINT,
  foul_ground SMALLINT,
  turf SMALLINT,

  -- Key outfield distances (match CSV: distances0, distances1, etc.)
  distances0 SMALLINT,
  distances1 SMALLINT,
  distances2 SMALLINT,
  distances3 SMALLINT,
  distances4 SMALLINT,
  distances5 SMALLINT,
  distances6 SMALLINT,

  -- Wall heights (match CSV: wall_heights0, wall_heights1, etc.)
  wall_heights0 SMALLINT,
  wall_heights1 SMALLINT,
  wall_heights2 SMALLINT,
  wall_heights3 SMALLINT,
  wall_heights4 SMALLINT,
  wall_heights5 SMALLINT,
  wall_heights6 SMALLINT,

  -- Park factors (match CSV exactly)
  avg DECIMAL(6,4),
  d DECIMAL(6,4),
  t DECIMAL(6,4),
  hr DECIMAL(6,4),

  FOREIGN KEY (nation_id) REFERENCES nations(nation_id)
);
-- Languages (needed for various references)
CREATE TABLE languages (
    language_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);
-- League hierarchy
  CREATE TABLE leagues (
      league_id INTEGER PRIMARY KEY,
      name VARCHAR(100) NOT NULL,
      abbr VARCHAR(10),
      nation_id INTEGER,
      language_id INTEGER,
      logo_file_name VARCHAR(200),
      parent_league_id INTEGER,
      league_state SMALLINT,
      season_year INTEGER,
      league_level SMALLINT,
      current_date_year INTEGER,  -- Renamed from current_date to avoid reserved keyword
      FOREIGN KEY (nation_id) REFERENCES nations(nation_id),
      FOREIGN KEY (language_id) REFERENCES languages(language_id),
      FOREIGN KEY (parent_league_id) REFERENCES leagues(league_id)
  );

CREATE TABLE sub_leagues (
    league_id INTEGER,
    sub_league_id INTEGER,
    name VARCHAR(50) NOT NULL,
    abbr VARCHAR(10),
    gender INTEGER,
    designated_hitter SMALLINT,
    PRIMARY KEY (league_id, sub_league_id),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);

CREATE TABLE divisions (
    league_id INTEGER,
    sub_league_id INTEGER,
    division_id INTEGER,
    name VARCHAR(50) NOT NULL,
    gender INTEGER,
    PRIMARY KEY (league_id, sub_league_id, division_id),
    FOREIGN KEY (league_id, sub_league_id) REFERENCES sub_leagues(league_id, sub_league_id)
);

-- Teams
CREATE TABLE teams (
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
CREATE TABLE team_relations (
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
CREATE TABLE team_affiliations (
    team_id INTEGER,
    affiliated_team_id INTEGER,
    PRIMARY KEY (team_id, affiliated_team_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id),
    FOREIGN KEY (affiliated_team_id) REFERENCES teams(team_id)
);






-- Insert special record for Free Agents
-- This will be done after initial data load:
-- INSERT INTO teams (team_id, name, abbr, league_id, level) 
-- VALUES (0, 'Free Agent', 'FA', 100, 1);

-- Add FK back to nations table after cities are loaded:
-- ALTER TABLE nations ADD CONSTRAINT fk_nations_capital 
--   FOREIGN KEY (capital_id) REFERENCES cities(city_id);