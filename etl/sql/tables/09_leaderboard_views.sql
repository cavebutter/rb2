-- =====================================================
  -- Leaderboard Materialized Views
  -- =====================================================
  -- Pre-aggregated views for high-performance leaderboard queries.
  -- These views are refreshed after each ETL run.
  --
  -- All views include is_active flag to support mixed
  -- active/retired player leaderboards with visual indicators.
  -- =====================================================

  -- Drop existing views (for clean re-creation)
  DROP MATERIALIZED VIEW IF EXISTS leaderboard_career_batting CASCADE;
  DROP MATERIALIZED VIEW IF EXISTS leaderboard_career_pitching CASCADE;
  DROP MATERIALIZED VIEW IF EXISTS leaderboard_single_season_batting CASCADE;
  DROP MATERIALIZED VIEW IF EXISTS leaderboard_single_season_pitching CASCADE;
  DROP MATERIALIZED VIEW IF EXISTS leaderboard_yearly_batting CASCADE;
  DROP MATERIALIZED VIEW IF EXISTS leaderboard_yearly_pitching CASCADE;

  -- =====================================================
  -- Career Leaderboards
  -- =====================================================

  -- Career Batting Leaders (all players, with active status)
  CREATE MATERIALIZED VIEW leaderboard_career_batting AS
  SELECT
      s.player_id,
      p.first_name,
      p.last_name,
      COUNT(DISTINCT s.year) as seasons,
      SUM(s.g) as g,
      SUM(s.pa) as pa,
      SUM(s.ab) as ab,
      SUM(s.r) as r,
      SUM(s.h) as h,
      SUM(s.d) as doubles,
      SUM(s.t) as triples,
      SUM(s.hr) as hr,
      SUM(s.rbi) as rbi,
      SUM(s.sb) as sb,
      SUM(s.cs) as cs,
      SUM(s.bb) as bb,
      SUM(s.k) as so,
      SUM(s.ibb) as ibb,
      SUM(s.hp) as hbp,
      SUM(s.sh) as sh,
      SUM(s.sf) as sf,
      SUM(s.gdp) as gdp,
      -- Calculated rate stats (weighted by PA)
      CASE WHEN SUM(s.ab) > 0
           THEN ROUND(SUM(s.h)::NUMERIC / SUM(s.ab)::NUMERIC, 3)
           ELSE 0 END as avg,
      CASE WHEN SUM(s.ab) > 0
           THEN ROUND((SUM(s.h) + SUM(s.bb) + SUM(s.hp))::NUMERIC /
                      (SUM(s.ab) + SUM(s.bb) + SUM(s.hp) + SUM(s.sf))::NUMERIC, 3)
           ELSE 0 END as obp,
      CASE WHEN SUM(s.ab) > 0
           THEN ROUND((SUM(s.h) + SUM(s.d) + SUM(s.t)*2 + SUM(s.hr)*3)::NUMERIC /
                      SUM(s.ab)::NUMERIC, 3)
           ELSE 0 END as slg,
      SUM(s.war) as war,
      -- Active status flag
      COALESCE(ps.retired, 1) = 0 as is_active,
      ps.retired
  FROM players_career_batting_stats s
  INNER JOIN players_core p ON s.player_id = p.player_id
  LEFT JOIN players_current_status ps ON s.player_id = ps.player_id
  WHERE s.split_id = 1  -- Only regular season stats
  GROUP BY s.player_id, p.first_name, p.last_name, ps.retired;

  -- Indexes for fast lookups
  CREATE INDEX IF NOT EXISTS idx_lb_career_bat_hr ON leaderboard_career_batting(hr DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_bat_avg ON leaderboard_career_batting(avg DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_bat_rbi ON leaderboard_career_batting(rbi DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_bat_sb ON leaderboard_career_batting(sb DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_bat_war ON leaderboard_career_batting(war DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_bat_active ON leaderboard_career_batting(is_active);

  COMMENT ON MATERIALIZED VIEW leaderboard_career_batting IS 'Career batting statistics for all players with active status indicator';

  -- Career Pitching Leaders (all players, with active status)
  CREATE MATERIALIZED VIEW leaderboard_career_pitching AS
  SELECT
      s.player_id,
      p.first_name,
      p.last_name,
      COUNT(DISTINCT s.year) as seasons,
      SUM(s.w) as w,
      SUM(s.l) as l,
      SUM(s.g) as g,
      SUM(s.gs) as gs,
      SUM(s.cg) as cg,
      SUM(s.sho) as sho,
      SUM(s.s) as sv,
      SUM(s.ip) as ip,
      SUM(s.ha) as h,
      SUM(s.r) as r,
      SUM(s.er) as er,
      SUM(s.hra) as hr,
      SUM(s.bb) as bb,
      SUM(s.k) as so,
      SUM(s.hp) as hbp,
      SUM(s.wp) as wp,
      SUM(s.bk) as bk,
      -- Calculated rate stats
      CASE WHEN SUM(s.ip) > 0
           THEN ROUND((SUM(s.er) * 9.0) / SUM(s.ip), 2)
           ELSE 0 END as era,
      CASE WHEN SUM(s.ip) > 0
           THEN ROUND((SUM(s.bb) + SUM(s.ha)) / SUM(s.ip), 2)
           ELSE 0 END as whip,
      CASE WHEN SUM(s.ip) > 0
           THEN ROUND((SUM(s.k) * 9.0) / SUM(s.ip), 2)
           ELSE 0 END as k_per_9,
      CASE WHEN SUM(s.bb) > 0
           THEN ROUND(SUM(s.k)::NUMERIC / SUM(s.bb)::NUMERIC, 2)
           ELSE 0 END as k_bb_ratio,
      SUM(s.war) as war,
      -- Active status flag
      COALESCE(ps.retired, 1) = 0 as is_active,
      ps.retired
  FROM players_career_pitching_stats s
  INNER JOIN players_core p ON s.player_id = p.player_id
  LEFT JOIN players_current_status ps ON s.player_id = ps.player_id
  WHERE s.split_id = 1  -- Only regular season stats
  GROUP BY s.player_id, p.first_name, p.last_name, ps.retired;

  -- Indexes for fast lookups
  CREATE INDEX IF NOT EXISTS idx_lb_career_pit_w ON leaderboard_career_pitching(w DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_pit_sv ON leaderboard_career_pitching(sv DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_pit_so ON leaderboard_career_pitching(so DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_pit_era ON leaderboard_career_pitching(era ASC) WHERE ip >= 500;
  CREATE INDEX IF NOT EXISTS idx_lb_career_pit_whip ON leaderboard_career_pitching(whip ASC) WHERE ip >= 500;
  CREATE INDEX IF NOT EXISTS idx_lb_career_pit_war ON leaderboard_career_pitching(war DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_career_pit_active ON leaderboard_career_pitching(is_active);

  COMMENT ON MATERIALIZED VIEW leaderboard_career_pitching IS 'Career pitching statistics for all players with active status indicator';

  -- =====================================================
  -- Single-Season Leaderboards
  -- =====================================================

  -- Single-Season Batting Records (all players, with active status)
  CREATE MATERIALIZED VIEW leaderboard_single_season_batting AS
  SELECT
      s.player_id,
      p.first_name,
      p.last_name,
      s.year,
      s.league_id,
      l.abbr as league_abbr,
      s.team_id,
      t.abbr as team_abbr,
      s.g,
      s.pa,
      s.ab,
      s.r,
      s.h,
      s.d as doubles,
      s.t as triples,
      s.hr,
      s.rbi,
      s.sb,
      s.bb,
      s.k as so,
      -- Calculated stats
      CASE WHEN s.ab > 0 THEN ROUND(s.h::NUMERIC / s.ab::NUMERIC, 3) ELSE 0 END as avg,
      CASE WHEN s.ab > 0
           THEN ROUND((s.h + s.bb + s.hp)::NUMERIC /
                      (s.ab + s.bb + s.hp + s.sf)::NUMERIC, 3)
           ELSE 0 END as obp,
      CASE WHEN s.ab > 0
           THEN ROUND((s.h + s.d + s.t*2 + s.hr*3)::NUMERIC / s.ab::NUMERIC, 3)
           ELSE 0 END as slg,
      s.war,
      -- Active status flag
      COALESCE(ps.retired, 1) = 0 as is_active
  FROM players_career_batting_stats s
  INNER JOIN players_core p ON s.player_id = p.player_id
  LEFT JOIN players_current_status ps ON s.player_id = ps.player_id
  LEFT JOIN leagues l ON s.league_id = l.league_id
  LEFT JOIN teams t ON s.team_id = t.team_id
  WHERE s.split_id = 1  -- Only regular season stats
    AND s.pa >= 100     -- Minimum PA threshold for meaningful stats
    AND s.team_id != 0; -- Exclude college/HS players (team_id=0 stats don't count)

  -- Indexes for fast lookups
  CREATE INDEX IF NOT EXISTS idx_lb_ss_bat_year ON leaderboard_single_season_batting(year DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_bat_hr ON leaderboard_single_season_batting(hr DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_bat_avg ON leaderboard_single_season_batting(avg DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_bat_war ON leaderboard_single_season_batting(war DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_bat_league ON leaderboard_single_season_batting(league_id, year);

  COMMENT ON MATERIALIZED VIEW leaderboard_single_season_batting IS 'Single-season batting records with active status indicator';

  -- Single-Season Pitching Records (all players, with active status)
  CREATE MATERIALIZED VIEW leaderboard_single_season_pitching AS
  SELECT
      s.player_id,
      p.first_name,
      p.last_name,
      s.year,
      s.league_id,
      l.abbr as league_abbr,
      s.team_id,
      t.abbr as team_abbr,
      s.w,
      s.l,
      s.g,
      s.gs,
      s.cg,
      s.sho,
      s.s as sv,
      s.ip,
      s.ha as h,
      s.er,
      s.bb,
      s.k as so,
      -- Calculated stats
      CASE WHEN s.ip > 0 THEN ROUND((s.er * 9.0) / s.ip, 2) ELSE 0 END as era,
      CASE WHEN s.ip > 0 THEN ROUND((s.bb + s.ha) / s.ip, 2) ELSE 0 END as whip,
      CASE WHEN s.ip > 0 THEN ROUND((s.k * 9.0) / s.ip, 2) ELSE 0 END as k_per_9,
      s.war,
      -- Active status flag
      COALESCE(ps.retired, 1) = 0 as is_active
  FROM players_career_pitching_stats s
  INNER JOIN players_core p ON s.player_id = p.player_id
  LEFT JOIN players_current_status ps ON s.player_id = ps.player_id
  LEFT JOIN leagues l ON s.league_id = l.league_id
  LEFT JOIN teams t ON s.team_id = t.team_id
  WHERE s.split_id = 1  -- Only regular season stats
    AND s.ip >= 50      -- Minimum IP threshold for meaningful stats
    AND s.team_id != 0; -- Exclude college/HS players (team_id=0 stats don't count)

  -- Indexes for fast lookups
  CREATE INDEX IF NOT EXISTS idx_lb_ss_pit_year ON leaderboard_single_season_pitching(year DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_pit_w ON leaderboard_single_season_pitching(w DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_pit_so ON leaderboard_single_season_pitching(so DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_pit_era ON leaderboard_single_season_pitching(era ASC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_pit_war ON leaderboard_single_season_pitching(war DESC);
  CREATE INDEX IF NOT EXISTS idx_lb_ss_pit_league ON leaderboard_single_season_pitching(league_id, year);

  COMMENT ON MATERIALIZED VIEW leaderboard_single_season_pitching IS 'Single-season pitching records with active status indicator';

  -- =====================================================
  -- Yearly League Leaders (Top 10 per year/league)
  -- =====================================================

  -- Yearly Batting Leaders by League
  CREATE MATERIALIZED VIEW leaderboard_yearly_batting AS
  WITH ranked_stats AS (
      SELECT
          s.player_id,
          p.first_name,
          p.last_name,
          s.year,
          s.league_id,
          l.abbr as league_abbr,
          s.hr,
          s.rbi,
          s.sb,
          s.h,
          CASE WHEN s.ab >= 300 AND s.ab > 0
               THEN ROUND(s.h::NUMERIC / s.ab::NUMERIC, 3)
               ELSE NULL END as avg,
          s.war,
          COALESCE(ps.retired, 1) = 0 as is_active,
          -- Rank by each stat
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.hr DESC) as hr_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.rbi DESC) as rbi_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.sb DESC) as sb_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.h DESC) as h_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id
                            ORDER BY CASE WHEN s.ab >= 300 AND s.ab > 0
                                          THEN s.h::NUMERIC / s.ab::NUMERIC
                                          ELSE 0 END DESC) as avg_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.war DESC) as war_rank
      FROM players_career_batting_stats s
      INNER JOIN players_core p ON s.player_id = p.player_id
      LEFT JOIN players_current_status ps ON s.player_id = ps.player_id
      LEFT JOIN leagues l ON s.league_id = l.league_id
      WHERE s.split_id = 1
        AND s.pa >= 100
        AND s.team_id != 0  -- Exclude college/HS players
  )
  SELECT * FROM ranked_stats
  WHERE hr_rank <= 10
     OR rbi_rank <= 10
     OR sb_rank <= 10
     OR h_rank <= 10
     OR avg_rank <= 10
     OR war_rank <= 10;

  CREATE INDEX IF NOT EXISTS idx_lb_yearly_bat_year_league ON leaderboard_yearly_batting(year, league_id);
  CREATE INDEX IF NOT EXISTS idx_lb_yearly_bat_hr_rank ON leaderboard_yearly_batting(year, league_id, hr_rank);
  CREATE INDEX IF NOT EXISTS idx_lb_yearly_bat_avg_rank ON leaderboard_yearly_batting(year, league_id, avg_rank);

  COMMENT ON MATERIALIZED VIEW leaderboard_yearly_batting IS 'Top 10 batting leaders per year/league for key statistics';

  -- Yearly Pitching Leaders by League
  CREATE MATERIALIZED VIEW leaderboard_yearly_pitching AS
  WITH ranked_stats AS (
      SELECT
          s.player_id,
          p.first_name,
          p.last_name,
          s.year,
          s.league_id,
          l.abbr as league_abbr,
          s.w,
          s.s as sv,
          s.k as so,
          CASE WHEN s.ip >= 100 THEN ROUND((s.er * 9.0) / s.ip, 2) ELSE NULL END as era,
          CASE WHEN s.ip >= 100 THEN ROUND((s.bb + s.ha) / s.ip, 2) ELSE NULL END as whip,
          s.war,
          COALESCE(ps.retired, 1) = 0 as is_active,
          -- Rank by each stat
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.w DESC) as w_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.s DESC) as sv_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.k DESC) as so_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id
                            ORDER BY CASE WHEN s.ip >= 100
                                          THEN (s.er * 9.0) / s.ip
                                          ELSE 999 END ASC) as era_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id
                            ORDER BY CASE WHEN s.ip >= 100
                                          THEN (s.bb + s.ha) / s.ip
                                          ELSE 999 END ASC) as whip_rank,
          ROW_NUMBER() OVER (PARTITION BY s.year, s.league_id ORDER BY s.war DESC) as war_rank
      FROM players_career_pitching_stats s
      INNER JOIN players_core p ON s.player_id = p.player_id
      LEFT JOIN players_current_status ps ON s.player_id = ps.player_id
      LEFT JOIN leagues l ON s.league_id = l.league_id
      WHERE s.split_id = 1
        AND s.ip >= 50
        AND s.team_id != 0  -- Exclude college/HS players
  )
  SELECT * FROM ranked_stats
  WHERE w_rank <= 10
     OR sv_rank <= 10
     OR so_rank <= 10
     OR era_rank <= 10
     OR whip_rank <= 10
     OR war_rank <= 10;

  CREATE INDEX IF NOT EXISTS idx_lb_yearly_pit_year_league ON leaderboard_yearly_pitching(year, league_id);
  CREATE INDEX IF NOT EXISTS idx_lb_yearly_pit_w_rank ON leaderboard_yearly_pitching(year, league_id, w_rank);
  CREATE INDEX IF NOT EXISTS idx_lb_yearly_pit_era_rank ON leaderboard_yearly_pitching(year, league_id, era_rank);

  COMMENT ON MATERIALIZED VIEW leaderboard_yearly_pitching IS 'Top 10 pitching leaders per year/league for key statistics';

  -- Analyze all views for query optimization
  ANALYZE leaderboard_career_batting;
  ANALYZE leaderboard_career_pitching;
  ANALYZE leaderboard_single_season_batting;
  ANALYZE leaderboard_single_season_pitching;
  ANALYZE leaderboard_yearly_batting;
  ANALYZE leaderboard_yearly_pitching;
