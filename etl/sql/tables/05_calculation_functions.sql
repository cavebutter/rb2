-- PostgreSQL Functions to populate calculation tables
-- These should be run after base statistics are loaded
-- Updated to use exact CSV table and column names

-- Function to refresh league runs per out
CREATE OR REPLACE FUNCTION refresh_league_runs_per_out()
RETURNS void AS $$
BEGIN
    -- Clear existing data
    TRUNCATE TABLE league_runs_per_out;
    
    -- Insert fresh calculations based on pitching stats
    INSERT INTO league_runs_per_out (
        year, league_id, sub_league_id, 
        total_runs, total_outs, total_pa,
        runs_per_out, runs_per_pa
    )
    SELECT 
        p.year,
        p.league_id,
        p.sub_league_id,
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
    WHERE p.split_id = 1  -- Overall stats only
      AND p.league_id <> 0
    GROUP BY p.year, p.league_id, p.sub_league_id;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh run values (complex calculation)
CREATE OR REPLACE FUNCTION refresh_run_values()
RETURNS void AS $$
BEGIN
    -- Clear existing data
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
            b.sub_league_id,
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
        GROUP BY b.year, b.league_id, b.sub_league_id
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
END;
$$ LANGUAGE plpgsql;

-- Function to refresh FIP constants
CREATE OR REPLACE FUNCTION refresh_fip_constants()
RETURNS void AS $$
BEGIN
    -- Clear existing data
    TRUNCATE TABLE fip_constants;
    
    -- Calculate FIP constants
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
END;
$$ LANGUAGE plpgsql;

-- Function to refresh sub-league batting environment
CREATE OR REPLACE FUNCTION refresh_sub_league_batting_environment()
RETURNS void AS $$
BEGIN
    -- Clear existing data
    TRUNCATE TABLE sub_league_batting_environment;
    
    -- Calculate sub-league batting environment (non-pitchers only)
    INSERT INTO sub_league_batting_environment (
        year, league_id, sub_league_id, total_pa, total_runs, runs_per_pa
    )
    SELECT
        b.year,
        b.league_id,
        b.sub_league_id,
        SUM(b.pa) AS total_pa,
        SUM(b.r) AS total_runs,
        CASE 
            WHEN SUM(b.pa) = 0 THEN 0 
            ELSE SUM(b.r)::DECIMAL / SUM(b.pa) 
        END AS runs_per_pa
    FROM players_career_batting_stats b
    JOIN players p ON b.player_id = p.player_id
    WHERE b.split_id = 1 
      AND p.position <> 1  -- Exclude pitchers
    GROUP BY b.year, b.league_id, b.sub_league_id;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh sub-league pitching environment
CREATE OR REPLACE FUNCTION refresh_sub_league_pitching_environment()
RETURNS void AS $$
BEGIN
    -- Clear existing data
    TRUNCATE TABLE sub_league_pitching_environment;
    
    -- Calculate sub-league pitching environment
    INSERT INTO sub_league_pitching_environment (
        year, league_id, sub_league_id, total_ip, total_er,
        adjusted_hra, adjusted_bb, adjusted_hp, adjusted_k,
        league_era, league_fip
    )
    SELECT
        p.year,
        p.league_id,
        p.sub_league_id,
        ((SUM(p.ip) * 3) + SUM(p.ipf))::DECIMAL / 3 AS total_ip,
        SUM(p.er) AS total_er,
        13 * SUM(p.hra) AS adjusted_hra,
        3 * SUM(p.bb) AS adjusted_bb,
        3 * SUM(p.hp) AS adjusted_hp,
        2 * SUM(p.k) AS adjusted_k,
        ROUND(
            (SUM(p.er)::DECIMAL / NULLIF(((SUM(p.ip) * 3) + SUM(p.ipf))::DECIMAL / 3, 0)) * 9, 
            2
        ) AS league_era,
        ROUND(
            ((13 * SUM(p.hra) + 3 * SUM(p.bb) + 3 * SUM(p.hp) - 2 * SUM(p.k))::DECIMAL / 
             NULLIF(((SUM(p.ip) * 3) + SUM(p.ipf))::DECIMAL / 3, 0)) + fc.fip_constant,
            2
        ) AS league_fip
    FROM players_career_pitching_stats p
    JOIN fip_constants fc ON p.year = fc.year AND p.league_id = fc.league_id
    WHERE p.league_id <> 0
    GROUP BY p.year, p.league_id, p.sub_league_id, fc.fip_constant;
END;
$$ LANGUAGE plpgsql;

-- Master function to refresh all calculation tables in correct order
CREATE OR REPLACE FUNCTION refresh_all_calculations()
RETURNS void AS $$
BEGIN
    RAISE NOTICE 'Refreshing league runs per out...';
    PERFORM refresh_league_runs_per_out();
    
    RAISE NOTICE 'Refreshing run values...';
    PERFORM refresh_run_values();
    
    RAISE NOTICE 'Refreshing FIP constants...';
    PERFORM refresh_fip_constants();
    
    RAISE NOTICE 'Refreshing sub-league batting environment...';
    PERFORM refresh_sub_league_batting_environment();
    
    RAISE NOTICE 'Refreshing sub-league pitching environment...';
    PERFORM refresh_sub_league_pitching_environment();
    
    RAISE NOTICE 'All calculations refreshed successfully';
END;
$$ LANGUAGE plpgsql;