-- PostgreSQL Functions to populate calculation tables
-- These should be run after base statistics are loaded
-- Updated to use exact CSV table and column names

-- Function to refresh league runs per out
  CREATE OR REPLACE FUNCTION refresh_league_runs_per_out(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Delete existing data for target year only
          DELETE FROM league_runs_per_out WHERE year = target_year;

          -- Insert calculations for specific year
          INSERT INTO league_runs_per_out (
              year, league_id, sub_league_id,
              total_runs, total_outs, total_pa,
              runs_per_out, runs_per_pa
          )
          SELECT
              p.year,
              p.league_id,
              COALESCE(p.sub_league_id, 0) AS sub_league_id,
              SUM(p.r) AS total_runs,
              SUM(p.outs) AS total_outs,
              SUM(p.outs + p.ha + p.bb + p.iw + p.sh + p.sf) AS total_pa,
              CASE
                  WHEN SUM(p.outs) = 0 THEN 0
                  ELSE SUM(p.r)::DECIMAL / SUM(p.outs)
              END AS runs_per_out,
              CASE
                  WHEN SUM(p.outs + p.ha + p.bb + p.iw + p.sh + p.sf) = 0 THEN 0
                  ELSE ROUND(SUM(p.r)::DECIMAL /
                            SUM(p.outs + p.ha + p.bb + p.iw + p.sh + p.sf), 8)
              END AS runs_per_pa
          FROM players_career_pitching_stats p
          WHERE p.split_id = 1
            AND p.league_id <> 0
            AND p.year = target_year
          GROUP BY p.year, p.league_id, COALESCE(p.sub_league_id, 0);
      ELSE
          -- Process all years (initial load/rebuild)
          TRUNCATE TABLE league_runs_per_out;

          INSERT INTO league_runs_per_out (
              year, league_id, sub_league_id,
              total_runs, total_outs, total_pa,
              runs_per_out, runs_per_pa
          )
          SELECT
              p.year,
              p.league_id,
              COALESCE(p.sub_league_id, 0) AS sub_league_id,
              SUM(p.r) AS total_runs,
              SUM(p.outs) AS total_outs,
              SUM(p.outs + p.ha + p.bb + p.iw + p.sh + p.sf) AS total_pa,
              CASE
                  WHEN SUM(p.outs) = 0 THEN 0
                  ELSE SUM(p.r)::DECIMAL / SUM(p.outs)
              END AS runs_per_out,
              CASE
                  WHEN SUM(p.outs + p.ha + p.bb + p.iw + p.sh + p.sf) = 0 THEN 0
                  ELSE ROUND(SUM(p.r)::DECIMAL /
                            SUM(p.outs + p.ha + p.bb + p.iw + p.sh + p.sf), 8)
              END AS runs_per_pa
          FROM players_career_pitching_stats p
          WHERE p.split_id = 1
            AND p.league_id <> 0
          GROUP BY p.year, p.league_id, COALESCE(p.sub_league_id, 0);
      END IF;
  END;
  $$ LANGUAGE plpgsql;

-- Function to refresh run values (complex calculation)
  CREATE OR REPLACE FUNCTION refresh_run_values(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Delete existing data for target year only
          DELETE FROM run_values WHERE year = target_year;

          -- Step 1: Calculate basic run values
          WITH base_run_values AS (
              SELECT
                  year,
                  league_id,
                  sub_league_id,
                  runs_per_out,
                  ROUND(runs_per_out + 0.14, 4) AS run_bb,
                  ROUND(runs_per_out + 0.14 + 0.025, 4) AS run_hbp,
                  ROUND(runs_per_out + 0.14 + 0.155, 4) AS run_1b,
                  ROUND(runs_per_out + 0.14 + 0.155 + 0.3, 4) AS run_2b,
                  ROUND(runs_per_out + 0.14 + 0.155 + 0.3 + 0.27, 4) AS run_3b,
                  1.4 AS run_hr,
                  0.2 AS run_sb,
                  2 * runs_per_out + 0.075 AS run_cs
              FROM league_runs_per_out
              WHERE year = target_year
          ),
          -- Step 2: Calculate league totals for batting stats
          league_batting_totals AS (
              SELECT
                  b.year,
                  b.league_id,
                  COALESCE(b.sub_league_id, 0) AS sub_league_id,
                  SUM(b.ab) AS ab,
                  SUM(b.bb) AS bb,
                  SUM(b.ibb) AS ibb,
                  SUM(b.hp) AS hbp,
                  SUM(b.h) AS h,
                  SUM(b.h - b.d - b.t - b.hr) AS singles,
                  SUM(b.d) AS d,
                  SUM(b.t) AS t,
                  SUM(b.hr) AS hr,
                  SUM(b.sb) AS sb,
                  SUM(b.cs) AS cs,
                  SUM(b.sf) AS sf
              FROM players_career_batting_stats b
              WHERE b.split_id = 1
                AND b.year = target_year
              GROUP BY b.year, b.league_id, COALESCE(b.sub_league_id, 0)
          ),
          -- Step 3: Calculate run_minus, run_plus, and wOBA
          intermediate_values AS (
              SELECT
                  rv.year,
                  rv.league_id,
                  rv.sub_league_id,
                  rv.runs_per_out,
                  rv.run_bb,
                  rv.run_hbp,
                  rv.run_1b,
                  rv.run_2b,
                  rv.run_3b,
                  rv.run_hr,
                  rv.run_sb,
                  rv.run_cs,
                  -- run_minus calculation
                  (rv.run_bb * (bt.bb - bt.ibb) + rv.run_hbp * bt.hbp +
                   rv.run_1b * bt.singles + rv.run_2b * bt.d +
                   rv.run_3b * bt.t + rv.run_hr * bt.hr +
                   rv.run_sb * bt.sb - rv.run_cs * bt.cs) /
                  NULLIF(bt.ab - bt.h + bt.sf, 0) AS run_minus,
                  -- run_plus calculation
                  (rv.run_bb * (bt.bb - bt.ibb) + rv.run_hbp * bt.hbp +
                   rv.run_1b * bt.singles + rv.run_2b * bt.d +
                   rv.run_3b * bt.t + rv.run_hr * bt.hr +
                   rv.run_sb * bt.sb - rv.run_cs * bt.cs) /
                  NULLIF(bt.bb - bt.ibb + bt.hbp + bt.h, 0) AS run_plus,
                  -- wOBA calculation
                  (bt.h + bt.bb - bt.ibb + bt.hbp)::DECIMAL /
                  NULLIF(bt.ab + bt.bb - bt.ibb + bt.hbp + bt.sf, 0) AS woba
              FROM base_run_values rv
              JOIN league_batting_totals bt
                  ON rv.year = bt.year
                  AND rv.league_id = bt.league_id
                  AND rv.sub_league_id = bt.sub_league_id
          )
          -- Final insert with all calculated values
          INSERT INTO run_values
          SELECT
              year,
              league_id,
              sub_league_id,
              runs_per_out,
              run_bb,
              run_hbp,
              run_1b,
              run_2b,
              run_3b,
              run_hr,
              run_sb,
              run_cs,
              run_minus,
              run_plus,
              woba,
              1.0 / (run_plus + run_minus) AS woba_scale,
              (run_bb + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_bb,
              (run_hbp + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_hbp,
              (run_1b + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_1b,
              (run_2b + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_2b,
              (run_3b + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_3b,
              (run_hr + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_hr,
              run_sb * (1.0 / (run_plus + run_minus)) AS woba_sb,
              run_cs * (1.0 / (run_plus + run_minus)) AS woba_cs
          FROM intermediate_values;
      ELSE
          -- Process all years (initial load/rebuild)
          TRUNCATE TABLE run_values;

          -- Step 1: Calculate basic run values
          WITH base_run_values AS (
              SELECT
                  year,
                  league_id,
                  sub_league_id,
                  runs_per_out,
                  ROUND(runs_per_out + 0.14, 4) AS run_bb,
                  ROUND(runs_per_out + 0.14 + 0.025, 4) AS run_hbp,
                  ROUND(runs_per_out + 0.14 + 0.155, 4) AS run_1b,
                  ROUND(runs_per_out + 0.14 + 0.155 + 0.3, 4) AS run_2b,
                  ROUND(runs_per_out + 0.14 + 0.155 + 0.3 + 0.27, 4) AS run_3b,
                  1.4 AS run_hr,
                  0.2 AS run_sb,
                  2 * runs_per_out + 0.075 AS run_cs
              FROM league_runs_per_out
          ),
          -- Step 2: Calculate league totals for batting stats
          league_batting_totals AS (
              SELECT
                  b.year,
                  b.league_id,
                  COALESCE(b.sub_league_id, 0) AS sub_league_id,
                  SUM(b.ab) AS ab,
                  SUM(b.bb) AS bb,
                  SUM(b.ibb) AS ibb,
                  SUM(b.hp) AS hbp,
                  SUM(b.h) AS h,
                  SUM(b.h - b.d - b.t - b.hr) AS singles,
                  SUM(b.d) AS d,
                  SUM(b.t) AS t,
                  SUM(b.hr) AS hr,
                  SUM(b.sb) AS sb,
                  SUM(b.cs) AS cs,
                  SUM(b.sf) AS sf
              FROM players_career_batting_stats b
              WHERE b.split_id = 1
              GROUP BY b.year, b.league_id, COALESCE(b.sub_league_id, 0)
          ),
          -- Step 3: Calculate run_minus, run_plus, and wOBA
          intermediate_values AS (
              SELECT
                  rv.year,
                  rv.league_id,
                  rv.sub_league_id,
                  rv.runs_per_out,
                  rv.run_bb,
                  rv.run_hbp,
                  rv.run_1b,
                  rv.run_2b,
                  rv.run_3b,
                  rv.run_hr,
                  rv.run_sb,
                  rv.run_cs,
                  -- run_minus calculation
                  (rv.run_bb * (bt.bb - bt.ibb) + rv.run_hbp * bt.hbp +
                   rv.run_1b * bt.singles + rv.run_2b * bt.d +
                   rv.run_3b * bt.t + rv.run_hr * bt.hr +
                   rv.run_sb * bt.sb - rv.run_cs * bt.cs) /
                  NULLIF(bt.ab - bt.h + bt.sf, 0) AS run_minus,
                  -- run_plus calculation
                  (rv.run_bb * (bt.bb - bt.ibb) + rv.run_hbp * bt.hbp +
                   rv.run_1b * bt.singles + rv.run_2b * bt.d +
                   rv.run_3b * bt.t + rv.run_hr * bt.hr +
                   rv.run_sb * bt.sb - rv.run_cs * bt.cs) /
                  NULLIF(bt.bb - bt.ibb + bt.hbp + bt.h, 0) AS run_plus,
                  -- wOBA calculation
                  (bt.h + bt.bb - bt.ibb + bt.hbp)::DECIMAL /
                  NULLIF(bt.ab + bt.bb - bt.ibb + bt.hbp + bt.sf, 0) AS woba
              FROM base_run_values rv
              JOIN league_batting_totals bt
                  ON rv.year = bt.year
                  AND rv.league_id = bt.league_id
                  AND rv.sub_league_id = bt.sub_league_id
          )
          -- Final insert with all calculated values
          INSERT INTO run_values
          SELECT
              year,
              league_id,
              sub_league_id,
              runs_per_out,
              run_bb,
              run_hbp,
              run_1b,
              run_2b,
              run_3b,
              run_hr,
              run_sb,
              run_cs,
              run_minus,
              run_plus,
              woba,
              1.0 / (run_plus + run_minus) AS woba_scale,
              (run_bb + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_bb,
              (run_hbp + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_hbp,
              (run_1b + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_1b,
              (run_2b + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_2b,
              (run_3b + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_3b,
              (run_hr + run_minus) * (1.0 / (run_plus + run_minus)) AS woba_hr,
              run_sb * (1.0 / (run_plus + run_minus)) AS woba_sb,
              run_cs * (1.0 / (run_plus + run_minus)) AS woba_cs
          FROM intermediate_values;
      END IF;
  END;
  $$ LANGUAGE plpgsql;

-- Function to refresh FIP constants
  CREATE OR REPLACE FUNCTION refresh_fip_constants(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Delete existing data for target year only
          DELETE FROM fip_constants WHERE year = target_year;

          -- Calculate FIP constants for specific year
          INSERT INTO fip_constants (
              year, league_id, hr_fb_pct, adjusted_hr, adjusted_bb,
              adjusted_hp, adjusted_k, innings_pitched, league_era, fip_constant
          )
          SELECT
              year,
              league_id,
              CASE WHEN SUM(fb) = 0 THEN 0 ELSE SUM(hra)::DECIMAL / SUM(fb) END AS hr_fb_pct,
              13 * SUM(hra) AS adjusted_hr,
              3 * SUM(bb) AS adjusted_bb,
              3 * SUM(hp) AS adjusted_hp,
              2 * SUM(k) AS adjusted_k,
              ((SUM(ip) * 3) + SUM(ipf))::DECIMAL / 3 AS innings_pitched,
              ROUND((SUM(er)::DECIMAL / NULLIF(((SUM(ip) * 3) + SUM(ipf))::DECIMAL / 3, 0)) * 9, 2) AS league_era,
              ROUND(
                  (SUM(er)::DECIMAL / NULLIF(((SUM(ip) * 3) + SUM(ipf))::DECIMAL / 3, 0)) * 9 -
                  ((13 * SUM(hra) + 3 * SUM(bb) + 3 * SUM(hp) - 2 * SUM(k))::DECIMAL /
                   NULLIF(((SUM(ip) * 3) + SUM(ipf))::DECIMAL / 3, 0)), 2
              ) AS fip_constant
          FROM players_career_pitching_stats
          WHERE league_id <> 0
            AND year = target_year
          GROUP BY year, league_id;
      ELSE
          -- Process all years (initial load/rebuild)
          TRUNCATE TABLE fip_constants;

          -- Calculate FIP constants for all years
          INSERT INTO fip_constants (
              year, league_id, hr_fb_pct, adjusted_hr, adjusted_bb,
              adjusted_hp, adjusted_k, innings_pitched, league_era, fip_constant
          )
          SELECT
              year,
              league_id,
              CASE WHEN SUM(fb) = 0 THEN 0 ELSE SUM(hra)::DECIMAL / SUM(fb) END AS hr_fb_pct,
              13 * SUM(hra) AS adjusted_hr,
              3 * SUM(bb) AS adjusted_bb,
              3 * SUM(hp) AS adjusted_hp,
              2 * SUM(k) AS adjusted_k,
              ((SUM(ip) * 3) + SUM(ipf))::DECIMAL / 3 AS innings_pitched,
              ROUND((SUM(er)::DECIMAL / NULLIF(((SUM(ip) * 3) + SUM(ipf))::DECIMAL / 3, 0)) * 9, 2) AS league_era,
              ROUND(
                  (SUM(er)::DECIMAL / NULLIF(((SUM(ip) * 3) + SUM(ipf))::DECIMAL / 3, 0)) * 9 -
                  ((13 * SUM(hra) + 3 * SUM(bb) + 3 * SUM(hp) - 2 * SUM(k))::DECIMAL /
                   NULLIF(((SUM(ip) * 3) + SUM(ipf))::DECIMAL / 3, 0)), 2
              ) AS fip_constant
          FROM players_career_pitching_stats
          WHERE league_id <> 0
          GROUP BY year, league_id;
      END IF;
  END;
  $$ LANGUAGE plpgsql;

-- Function to refresh sub_league batting environment
  -- NOTE: Added league_id <> 0 filter to exclude free agents/invalid records from league aggregates
  CREATE OR REPLACE FUNCTION refresh_sub_league_batting_environment(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Delete existing data for target year only
          DELETE FROM sub_league_batting_environment WHERE year = target_year;

          -- Calculate sub-league batting environment for specific year (non-pitchers only)
          INSERT INTO sub_league_batting_environment (
              year, league_id, sub_league_id, total_pa, total_runs, runs_per_pa
          )
          SELECT
              b.year,
              b.league_id,
              COALESCE(b.sub_league_id, 0) AS sub_league_id,
              SUM(b.pa) AS total_pa,
              SUM(b.r) AS total_runs,
              CASE
                  WHEN SUM(b.pa) = 0 THEN 0
                  ELSE SUM(b.r)::DECIMAL / SUM(b.pa)
              END AS runs_per_pa
          FROM players_career_batting_stats b
          JOIN players_current_status pcs ON b.player_id = pcs.player_id
          WHERE b.split_id = 1
            AND pcs.position <> 1  -- Exclude pitchers
            AND b.league_id <> 0   -- FILTER: Exclude league_id=0 (free agents/invalid records)
            AND b.year = target_year
          GROUP BY b.year, b.league_id, COALESCE(b.sub_league_id, 0);
      ELSE
          -- Process all years (initial load/rebuild)
          TRUNCATE TABLE sub_league_batting_environment;

          -- Calculate sub-league batting environment for all years (non-pitchers only)
          INSERT INTO sub_league_batting_environment (
              year, league_id, sub_league_id, total_pa, total_runs, runs_per_pa
          )
          SELECT
              b.year,
              b.league_id,
              COALESCE(b.sub_league_id, 0) AS sub_league_id,
              SUM(b.pa) AS total_pa,
              SUM(b.r) AS total_runs,
              CASE
                  WHEN SUM(b.pa) = 0 THEN 0
                  ELSE SUM(b.r)::DECIMAL / SUM(b.pa)
              END AS runs_per_pa
          FROM players_career_batting_stats b
          JOIN players_current_status pcs ON b.player_id = pcs.player_id
          WHERE b.split_id = 1
            AND pcs.position <> 1  -- Exclude pitchers
            AND b.league_id <> 0   -- FILTER: Exclude league_id=0 (free agents/invalid records)
          GROUP BY b.year, b.league_id, COALESCE(b.sub_league_id, 0);
      END IF;
  END;
  $$ LANGUAGE plpgsql;


  CREATE OR REPLACE FUNCTION refresh_sub_league_pitching_environment(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      -- Calculate league-wide pitching environment for ERA+/ERA-/FIP- calculations
      -- Aggregates by year/league_id/sub_league_id
      -- IMPORTANT: Excludes league_id=0 records (free agents with NULL sub_league_id)
      -- If this causes issues, remove the "AND p.league_id <> 0" filters below

      IF target_year IS NOT NULL THEN
          -- Incremental: Single year update
          DELETE FROM sub_league_pitching_environment WHERE year = target_year;

          INSERT INTO sub_league_pitching_environment (
              year, league_id, sub_league_id,
              total_ip, total_er,
              adjusted_hra, adjusted_bb, adjusted_hp, adjusted_k,
              league_era, league_fip
          )
          SELECT
              p.year,
              p.league_id,
              COALESCE(p.sub_league_id, 0) AS sub_league_id,
              -- Total innings pitched (converted from outs)
              ROUND(SUM(p.outs) / 3.0, 1) as total_ip,
              SUM(p.er) as total_er,
              -- Components for FIP calculation
              SUM(p.hra) as adjusted_hra,
              SUM(p.bb) as adjusted_bb,
              SUM(p.hp) as adjusted_hp,
              SUM(p.k) as adjusted_k,
              -- League ERA
              CASE
                  WHEN SUM(p.outs) > 0
                  THEN ROUND((SUM(p.er) * 9.0) / (SUM(p.outs) / 3.0), 2)
                  ELSE 0
              END as league_era,
              -- League FIP (requires join to fip_constants)
              CASE
                  WHEN SUM(p.outs) > 0
                  THEN ROUND(
                      ((13.0 * SUM(p.hra)) + (3.0 * (SUM(p.bb) + SUM(p.hp))) - (2.0 * SUM(p.k)))
                      / (SUM(p.outs) / 3.0)
                      + fc.fip_constant,
                      2
                  )
                  ELSE 0
              END as league_fip
          FROM players_career_pitching_stats p
          INNER JOIN fip_constants fc ON p.year = fc.year AND p.league_id = fc.league_id
          WHERE p.split_id = 1  -- Overall stats only
            AND p.league_id <> 0  -- FILTER: Exclude free agents/invalid records
            AND p.year = target_year
          GROUP BY p.year, p.league_id, COALESCE(p.sub_league_id, 0), fc.fip_constant;

      ELSE
          -- Full refresh: All years
          TRUNCATE TABLE sub_league_pitching_environment;

          INSERT INTO sub_league_pitching_environment (
              year, league_id, sub_league_id,
              total_ip, total_er,
              adjusted_hra, adjusted_bb, adjusted_hp, adjusted_k,
              league_era, league_fip
          )
          SELECT
              p.year,
              p.league_id,
              COALESCE(p.sub_league_id, 0) AS sub_league_id,
              -- Total innings pitched (converted from outs)
              ROUND(SUM(p.outs) / 3.0, 1) as total_ip,
              SUM(p.er) as total_er,
              -- Components for FIP calculation
              SUM(p.hra) as adjusted_hra,
              SUM(p.bb) as adjusted_bb,
              SUM(p.hp) as adjusted_hp,
              SUM(p.k) as adjusted_k,
              -- League ERA
              CASE
                  WHEN SUM(p.outs) > 0
                  THEN ROUND((SUM(p.er) * 9.0) / (SUM(p.outs) / 3.0), 2)
                  ELSE 0
              END as league_era,
              -- League FIP (requires join to fip_constants)
              CASE
                  WHEN SUM(p.outs) > 0
                  THEN ROUND(
                      ((13.0 * SUM(p.hra)) + (3.0 * (SUM(p.bb) + SUM(p.hp))) - (2.0 * SUM(p.k)))
                      / (SUM(p.outs) / 3.0)
                      + fc.fip_constant,
                      2
                  )
                  ELSE 0
              END as league_fip
          FROM players_career_pitching_stats p
          INNER JOIN fip_constants fc ON p.year = fc.year AND p.league_id = fc.league_id
          WHERE p.split_id = 1  -- Overall stats only
            AND p.league_id <> 0  -- FILTER: Exclude free agents/invalid records
          GROUP BY p.year, p.league_id, COALESCE(p.sub_league_id, 0), fc.fip_constant;

      END IF;

      RAISE NOTICE 'Pitching environment refreshed for year: %', COALESCE(target_year::TEXT, 'ALL');
  END;
  $$ LANGUAGE plpgsql;



  -- Master function to refresh all calculation tables in correct order
  CREATE OR REPLACE FUNCTION refresh_all_calculations(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      -- Phase B: League Constants
      RAISE NOTICE 'Refreshing league runs per out...';
      PERFORM refresh_league_runs_per_out(target_year);

      RAISE NOTICE 'Refreshing run values...';
      PERFORM refresh_run_values(target_year);

      RAISE NOTICE 'Refreshing FIP constants...';
      PERFORM refresh_fip_constants(target_year);

      RAISE NOTICE 'Refreshing sub-league batting environment...';
      PERFORM refresh_sub_league_batting_environment(target_year);

      RAISE NOTICE 'Refreshing sub-league pitching environment...';
      PERFORM refresh_sub_league_pitching_environment(target_year);

      -- Phase C: Apply to Player Stats - BATTING (NEW)
      RAISE NOTICE 'Calculating player wOBA...';
      PERFORM refresh_player_woba(target_year);

      RAISE NOTICE 'Calculating player wRAA...';
      PERFORM refresh_player_wraa(target_year);

      RAISE NOTICE 'Calculating player wRC...';
      PERFORM refresh_player_wrc(target_year);

      RAISE NOTICE 'Calculating player wRC+...';
      PERFORM refresh_player_wrc_plus(target_year);

      -- Phase C: Apply to Player Stats - PITCHING
      RAISE NOTICE 'Calculating player FIP...';
      PERFORM refresh_player_fip(target_year);

      RAISE NOTICE 'Calculating player xFIP...';
      PERFORM refresh_player_xfip(target_year);

      RAISE NOTICE 'Calculating player ERA+...';
      PERFORM refresh_player_era_plus(target_year);

      RAISE NOTICE 'Calculating player ERA-...';
      PERFORM refresh_player_era_minus(target_year);

      RAISE NOTICE 'Calculating player FIP-...';
      PERFORM refresh_player_fip_minus(target_year);


      IF target_year IS NOT NULL THEN
          RAISE NOTICE 'All calculations complete for year %', target_year;
      ELSE
          RAISE NOTICE 'All calculations complete for all years';
      END IF;
  END;
  $$ LANGUAGE plpgsql;


 -- ============================================================================
  -- BATTING ADVANCED METRICS FUNCTIONS
  -- ============================================================================
  -- NOTE: These functions calculate advanced batting metrics (wOBA, wRAA, wRC, wRC+)
  -- using league constants from the run_values table. They filter out records where:
  -- 1. league_id = 0 (invalid/placeholder records for free agents, unsigned players)
  -- 2. These records have team_id = 0 and sub_league_id = NULL
  -- 3. If we need to include these records in the future, remove the "AND b.league_id <> 0" filters
  -- ============================================================================

  -- Function to calculate wOBA for batting stats
  CREATE OR REPLACE FUNCTION refresh_player_woba(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_batting_stats b
          SET
              woba = ROUND(
                  (rv.woba_bb * (b.bb - b.ibb) +
                   rv.woba_hbp * b.hp +
                   rv.woba_1b * (b.h - b.d - b.t - b.hr) +
                   rv.woba_2b * b.d +
                   rv.woba_3b * b.t +
                   rv.woba_hr * b.hr) /
                  NULLIF(b.ab + b.bb - b.ibb + b.sf + b.hp, 0),
                  3
              ),
              last_updated = CURRENT_TIMESTAMP
          FROM run_values rv
          WHERE b.year = rv.year
            AND b.league_id = rv.league_id
            AND b.sub_league_id = rv.sub_league_id
            AND b.split_id = 1
            AND b.league_id <> 0  -- FILTER: Exclude league_id=0 (free agents/invalid records with no team/league assignment)
            AND b.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_batting_stats b
          SET
              woba = ROUND(
                  (rv.woba_bb * (b.bb - b.ibb) +
                   rv.woba_hbp * b.hp +
                   rv.woba_1b * (b.h - b.d - b.t - b.hr) +
                   rv.woba_2b * b.d +
                   rv.woba_3b * b.t +
                   rv.woba_hr * b.hr) /
                  NULLIF(b.ab + b.bb - b.ibb + b.sf + b.hp, 0),
                  3
              ),
              last_updated = CURRENT_TIMESTAMP
          FROM run_values rv
          WHERE b.year = rv.year
            AND b.league_id = rv.league_id
            AND b.sub_league_id = rv.sub_league_id
            AND b.split_id = 1
            AND b.league_id <> 0;  -- FILTER: Exclude league_id=0 (free agents/invalid records)
      END IF;

      RAISE NOTICE 'wOBA calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

  -- Function to calculate wRAA (Weighted Runs Above Average)
  -- Depends on wOBA being calculated first
  CREATE OR REPLACE FUNCTION refresh_player_wraa(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_batting_stats b
          SET
              wraa = ROUND(
                  ((b.woba - rv.woba) / rv.woba_scale) * b.pa,
                  1
              ),
              last_updated = CURRENT_TIMESTAMP
          FROM run_values rv
          WHERE b.year = rv.year
            AND b.league_id = rv.league_id
            AND b.sub_league_id = rv.sub_league_id
            AND b.split_id = 1
            AND b.woba IS NOT NULL
            AND b.league_id <> 0  -- FILTER: Exclude league_id=0 (free agents/invalid records)
            AND b.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_batting_stats b
          SET
              wraa = ROUND(
                  ((b.woba - rv.woba) / rv.woba_scale) * b.pa,
                  1
              ),
              last_updated = CURRENT_TIMESTAMP
          FROM run_values rv
          WHERE b.year = rv.year
            AND b.league_id = rv.league_id
            AND b.sub_league_id = rv.sub_league_id
            AND b.split_id = 1
            AND b.woba IS NOT NULL
            AND b.league_id <> 0;  -- FILTER: Exclude league_id=0 (free agents/invalid records)
      END IF;

      RAISE NOTICE 'wRAA calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

  -- Function to calculate wRC (Weighted Runs Created)
  -- Depends on wOBA being calculated first
  CREATE OR REPLACE FUNCTION refresh_player_wrc(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_batting_stats b
          SET
              wrc = ROUND(
                  (((b.woba - rv.woba) / rv.woba_scale) + (lro.runs_per_pa)) * b.pa,
                  0
              )::INTEGER,
              last_updated = CURRENT_TIMESTAMP
          FROM run_values rv
          JOIN league_runs_per_out lro
              ON rv.year = lro.year
              AND rv.league_id = lro.league_id
              AND rv.sub_league_id = lro.sub_league_id
          WHERE b.year = rv.year
            AND b.league_id = rv.league_id
            AND b.sub_league_id = rv.sub_league_id
            AND b.split_id = 1
            AND b.woba IS NOT NULL
            AND b.league_id <> 0  -- FILTER: Exclude league_id=0 (free agents/invalid records)
            AND b.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_batting_stats b
          SET
              wrc = ROUND(
                  (((b.woba - rv.woba) / rv.woba_scale) + (lro.runs_per_pa)) * b.pa,
                  0
              )::INTEGER,
              last_updated = CURRENT_TIMESTAMP
          FROM run_values rv
          JOIN league_runs_per_out lro
              ON rv.year = lro.year
              AND rv.league_id = lro.league_id
              AND rv.sub_league_id = lro.sub_league_id
          WHERE b.year = rv.year
            AND b.league_id = rv.league_id
            AND b.sub_league_id = rv.sub_league_id
            AND b.split_id = 1
            AND b.woba IS NOT NULL
            AND b.league_id <> 0;  -- FILTER: Exclude league_id=0 (free agents/invalid records)
      END IF;

      RAISE NOTICE 'wRC calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

  -- Function to calculate wRC+ (park and league adjusted)
  -- Depends on wRAA being calculated first
  -- Uses old-style FROM clause syntax to avoid PostgreSQL UPDATE-FROM-JOIN reference issues
  CREATE OR REPLACE FUNCTION refresh_player_wrc_plus(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_batting_stats b
          SET
              wrc_plus = ROUND(
                  (((b.wraa / NULLIF(b.pa, 0) + lro.runs_per_pa) +
                    (lro.runs_per_pa - team_parks.park_avg * lro.runs_per_pa)) /
                   NULLIF(slg.runs_per_pa, 0)) * 100,
                  0
              )::INTEGER,
              constants_version = 1,
              last_updated = CURRENT_TIMESTAMP
          FROM run_values rv,
               league_runs_per_out lro,
               sub_league_batting_environment slg,
               (SELECT t.team_id, p.avg as park_avg
                FROM teams t
                JOIN parks p ON p.park_id = t.park_id
               ) team_parks
          WHERE b.year = rv.year
            AND b.league_id = rv.league_id
            AND b.sub_league_id = rv.sub_league_id
            AND rv.year = lro.year
            AND rv.league_id = lro.league_id
            AND rv.sub_league_id = lro.sub_league_id
            AND rv.year = slg.year
            AND rv.league_id = slg.league_id
            AND rv.sub_league_id = slg.sub_league_id
            AND b.team_id = team_parks.team_id
            AND b.split_id = 1
            AND b.wraa IS NOT NULL
            AND b.pa > 0
            AND b.league_id <> 0  -- FILTER: Exclude league_id=0 (free agents/invalid records)
            AND b.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_batting_stats b
          SET
              wrc_plus = ROUND(
                  (((b.wraa / NULLIF(b.pa, 0) + lro.runs_per_pa) +
                    (lro.runs_per_pa - team_parks.park_avg * lro.runs_per_pa)) /
                   NULLIF(slg.runs_per_pa, 0)) * 100,
                  0
              )::INTEGER,
              constants_version = 1,
              last_updated = CURRENT_TIMESTAMP
          FROM run_values rv,
               league_runs_per_out lro,
               sub_league_batting_environment slg,
               (SELECT t.team_id, p.avg as park_avg
                FROM teams t
                JOIN parks p ON p.park_id = t.park_id
               ) team_parks
          WHERE b.year = rv.year
            AND b.league_id = rv.league_id
            AND b.sub_league_id = rv.sub_league_id
            AND rv.year = lro.year
            AND rv.league_id = lro.league_id
            AND rv.sub_league_id = lro.sub_league_id
            AND rv.year = slg.year
            AND rv.league_id = slg.league_id
            AND rv.sub_league_id = slg.sub_league_id
            AND b.team_id = team_parks.team_id
            AND b.split_id = 1
            AND b.wraa IS NOT NULL
            AND b.pa > 0
            AND b.league_id <> 0;  -- FILTER: Exclude league_id=0 (free agents/invalid records)
      END IF;

      RAISE NOTICE 'wRC+ calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

-- ============================================================================
  -- PITCHING ADVANCED METRICS FUNCTIONS
  -- ============================================================================
  -- NOTE: These functions calculate advanced pitching metrics (FIP, xFIP, ERA+, ERA-, FIP-)
  -- using league constants from fip_constants and sub_league_pitching_environment tables.
  -- They filter out records where league_id = 0 (same as batting)
  -- ============================================================================

  -- Function to calculate FIP (Fielding Independent Pitching)
  -- Formula: ((13*HR) + (3*(BB+HBP)) - (2*K)) / IP + FIP_constant
  CREATE OR REPLACE FUNCTION refresh_player_fip(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_pitching_stats p
          SET
              fip = CASE
                  WHEN p.outs > 0 THEN
                      ROUND(
                          ((13.0 * p.hra) + (3.0 * (p.bb + p.hp)) - (2.0 * p.k))
                          / (p.outs / 3.0)
                          + fc.fip_constant,
                          2
                      )::DECIMAL(5,2)
                  ELSE 0::DECIMAL(5,2)
              END,
              last_updated = CURRENT_TIMESTAMP
          FROM fip_constants fc
          WHERE p.year = fc.year
            AND p.league_id = fc.league_id
            AND p.split_id = 1
            AND p.league_id <> 0  -- FILTER: Exclude free agents/invalid records
            AND p.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_pitching_stats p
          SET
              fip = CASE
                  WHEN p.outs > 0 THEN
                      ROUND(
                          ((13.0 * p.hra) + (3.0 * (p.bb + p.hp)) - (2.0 * p.k))
                          / (p.outs / 3.0)
                          + fc.fip_constant,
                          2
                      )::DECIMAL(5,2)
                  ELSE 0::DECIMAL(5,2)
              END,
              last_updated = CURRENT_TIMESTAMP
          FROM fip_constants fc
          WHERE p.year = fc.year
            AND p.league_id = fc.league_id
            AND p.split_id = 1
            AND p.league_id <> 0;  -- FILTER: Exclude free agents/invalid records
      END IF;

      RAISE NOTICE 'FIP calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

  -- Function to calculate xFIP (Expected FIP using league HR/FB rate)
  -- Formula: ((13*(FB*league_hr_fb_pct)) + (3*(BB+HBP)) - (2*K)) / IP + FIP_constant
  CREATE OR REPLACE FUNCTION refresh_player_xfip(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_pitching_stats p
          SET
              xfip = CASE
                  WHEN p.outs > 0 THEN
                      ROUND(
                          ((13.0 * (p.fb * fc.hr_fb_pct)) + (3.0 * (p.bb + p.hp)) - (2.0 * p.k))
                          / (p.outs / 3.0)
                          + fc.fip_constant,
                          2
                      )::DECIMAL(5,2)
                  ELSE 0::DECIMAL(5,2)
              END,
              last_updated = CURRENT_TIMESTAMP
          FROM fip_constants fc
          WHERE p.year = fc.year
            AND p.league_id = fc.league_id
            AND p.split_id = 1
            AND p.league_id <> 0  -- FILTER: Exclude free agents/invalid records
            AND p.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_pitching_stats p
          SET
              xfip = CASE
                  WHEN p.outs > 0 THEN
                      ROUND(
                          ((13.0 * (p.fb * fc.hr_fb_pct)) + (3.0 * (p.bb + p.hp)) - (2.0 * p.k))
                          / (p.outs / 3.0)
                          + fc.fip_constant,
                          2
                      )::DECIMAL(5,2)
                  ELSE 0::DECIMAL(5,2)
              END,
              last_updated = CURRENT_TIMESTAMP
          FROM fip_constants fc
          WHERE p.year = fc.year
            AND p.league_id = fc.league_id
            AND p.split_id = 1
            AND p.league_id <> 0;  -- FILTER: Exclude free agents/invalid records
      END IF;

      RAISE NOTICE 'xFIP calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

  -- Function to calculate ERA+ (100 = league average, higher is better)
  -- Formula: (league_ERA / player_ERA) * park_factor * 100
  CREATE OR REPLACE FUNCTION refresh_player_era_plus(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_pitching_stats p
          SET
              era_plus = CASE
                  WHEN p.era > 0 THEN
                      ROUND(
                          (slpe.league_era / p.era) * parks.avg * 100,
                          0
                      )::INTEGER
                  ELSE 0::INTEGER
              END,
              constants_version = 1,
              last_updated = CURRENT_TIMESTAMP
          FROM sub_league_pitching_environment slpe,
               teams t,
               parks
          WHERE p.year = slpe.year
            AND p.league_id = slpe.league_id
            AND p.sub_league_id = slpe.sub_league_id
            AND p.team_id = t.team_id
            AND t.park_id = parks.park_id
            AND p.split_id = 1
            AND p.era IS NOT NULL
            AND p.league_id <> 0  -- FILTER: Exclude free agents/invalid records
            AND p.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_pitching_stats p
          SET
              era_plus = CASE
                  WHEN p.era > 0 THEN
                      ROUND(
                          (slpe.league_era / p.era) * parks.avg * 100,
                          0
                      )::INTEGER
                  ELSE 0::INTEGER
              END,
              constants_version = 1,
              last_updated = CURRENT_TIMESTAMP
          FROM sub_league_pitching_environment slpe,
               teams t,
               parks
          WHERE p.year = slpe.year
            AND p.league_id = slpe.league_id
            AND p.sub_league_id = slpe.sub_league_id
            AND p.team_id = t.team_id
            AND t.park_id = parks.park_id
            AND p.split_id = 1
            AND p.era IS NOT NULL
            AND p.league_id <> 0;  -- FILTER: Exclude free agents/invalid records
      END IF;

      RAISE NOTICE 'ERA+ calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

  -- Function to calculate ERA- (100 = league average, lower is better)
  -- Formula: ((player_ERA + (player_ERA - player_ERA*park_factor)) / league_ERA) * 100
  CREATE OR REPLACE FUNCTION refresh_player_era_minus(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_pitching_stats p
          SET
              era_minus = CASE
                  WHEN slpe.league_era > 0 THEN
                      ROUND(
                          ((p.era + (p.era - p.era * parks.avg)) / slpe.league_era) * 100,
                          0
                      )::INTEGER
                  ELSE 0::INTEGER
              END,
              constants_version = 1,
              last_updated = CURRENT_TIMESTAMP
          FROM sub_league_pitching_environment slpe,
               teams t,
               parks
          WHERE p.year = slpe.year
            AND p.league_id = slpe.league_id
            AND p.sub_league_id = slpe.sub_league_id
            AND p.team_id = t.team_id
            AND t.park_id = parks.park_id
            AND p.split_id = 1
            AND p.era IS NOT NULL
            AND p.league_id <> 0  -- FILTER: Exclude free agents/invalid records
            AND p.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_pitching_stats p
          SET
              era_minus = CASE
                  WHEN slpe.league_era > 0 THEN
                      ROUND(
                          ((p.era + (p.era - p.era * parks.avg)) / slpe.league_era) * 100,
                          0
                      )::INTEGER
                  ELSE 0::INTEGER
              END,
              constants_version = 1,
              last_updated = CURRENT_TIMESTAMP
          FROM sub_league_pitching_environment slpe,
               teams t,
               parks
          WHERE p.year = slpe.year
            AND p.league_id = slpe.league_id
            AND p.sub_league_id = slpe.sub_league_id
            AND p.team_id = t.team_id
            AND t.park_id = parks.park_id
            AND p.split_id = 1
            AND p.era IS NOT NULL
            AND p.league_id <> 0;  -- FILTER: Exclude free agents/invalid records
      END IF;

      RAISE NOTICE 'ERA- calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

  -- Function to calculate FIP- (100 = league average, lower is better)
  -- Formula: ((player_FIP + (player_FIP - player_FIP*park_factor)) / league_FIP) * 100
  CREATE OR REPLACE FUNCTION refresh_player_fip_minus(target_year INTEGER DEFAULT NULL)
  RETURNS void AS $$
  BEGIN
      IF target_year IS NOT NULL THEN
          -- Update specific year
          UPDATE players_career_pitching_stats p
          SET
              fip_minus = CASE
                  WHEN slpe.league_fip > 0 THEN
                      ROUND(
                          ((p.fip + (p.fip - p.fip * parks.avg)) / slpe.league_fip) * 100,
                          0
                      )::INTEGER
                  ELSE 0::INTEGER
              END,
              constants_version = 1,
              last_updated = CURRENT_TIMESTAMP
          FROM sub_league_pitching_environment slpe,
               teams t,
               parks
          WHERE p.year = slpe.year
            AND p.league_id = slpe.league_id
            AND p.sub_league_id = slpe.sub_league_id
            AND p.team_id = t.team_id
            AND t.park_id = parks.park_id
            AND p.split_id = 1
            AND p.fip IS NOT NULL
            AND p.league_id <> 0  -- FILTER: Exclude free agents/invalid records
            AND p.year = target_year;
      ELSE
          -- Update all years
          UPDATE players_career_pitching_stats p
          SET
              fip_minus = CASE
                  WHEN slpe.league_fip > 0 THEN
                      ROUND(
                          ((p.fip + (p.fip - p.fip * parks.avg)) / slpe.league_fip) * 100,
                          0
                      )::INTEGER
                  ELSE 0::INTEGER
              END,
              constants_version = 1,
              last_updated = CURRENT_TIMESTAMP
          FROM sub_league_pitching_environment slpe,
               teams t,
               parks
          WHERE p.year = slpe.year
            AND p.league_id = slpe.league_id
            AND p.sub_league_id = slpe.sub_league_id
            AND p.team_id = t.team_id
            AND t.park_id = parks.park_id
            AND p.split_id = 1
            AND p.fip IS NOT NULL
            AND p.league_id <> 0;  -- FILTER: Exclude free agents/invalid records
      END IF;

      RAISE NOTICE 'FIP- calculation complete for year %', COALESCE(target_year::text, 'ALL');
  END;
  $$ LANGUAGE plpgsql;

