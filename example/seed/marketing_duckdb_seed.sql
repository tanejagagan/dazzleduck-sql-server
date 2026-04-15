-- Marketing campaign seed data for DuckDB
-- Run: duckdb example/marketing.duckdb < example/marketing_duckdb_seed.sql
-- Or via DazzleDuck: POST /v1/query with each statement

-- ============================================================
-- Schema
-- ============================================================

DROP TABLE IF EXISTS conversions;
DROP TABLE IF EXISTS impressions;
DROP TABLE IF EXISTS leads;
DROP TABLE IF EXISTS campaigns;

CREATE TABLE campaigns (
    id         INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    channel    TEXT NOT NULL,   -- email | social | search | display | referral
    budget     REAL NOT NULL,
    spent      REAL NOT NULL,
    status     TEXT NOT NULL,   -- active | paused | completed
    start_date DATE NOT NULL,
    end_date   DATE NOT NULL
);

CREATE TABLE leads (
    id              INTEGER PRIMARY KEY,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    email           TEXT NOT NULL UNIQUE,
    company         TEXT,
    source_campaign INTEGER REFERENCES campaigns(id),
    created_at      DATE NOT NULL
);

CREATE TABLE impressions (
    id          INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
    event_type  TEXT NOT NULL,   -- impression | click
    occurred_at DATE NOT NULL,
    device      TEXT NOT NULL    -- desktop | mobile | tablet
);

CREATE TABLE conversions (
    id           INTEGER PRIMARY KEY,
    lead_id      INTEGER NOT NULL REFERENCES leads(id),
    campaign_id  INTEGER NOT NULL REFERENCES campaigns(id),
    type         TEXT NOT NULL,  -- signup | purchase | trial
    value        REAL,
    converted_at DATE NOT NULL
);

-- ============================================================
-- campaigns — 20 rows
-- ============================================================

INSERT INTO campaigns
SELECT
    n::INTEGER                                                          AS id,
    (['Spring Launch','Summer Boost','Back to School','Black Friday Blitz',
      'Holiday Special','New Year Push','Brand Awareness Q1','Retargeting Wave',
      'Product Launch Beta','Always-On Nurture'])
        [(floor(random() * 10)::INTEGER) + 1] || ' ' || n             AS name,
    (['email','social','search','display','referral'])
        [(floor(random() * 5)::INTEGER) + 1]                          AS channel,
    round(5000 + (random() * 45000)::REAL, 2)                         AS budget,
    round((random() * 5000)::REAL, 2)                                 AS spent,
    (['active','paused','completed'])
        [(floor(random() * 3)::INTEGER) + 1]                          AS status,
    DATE '2024-01-01' + INTERVAL (floor(random() * 180)::INTEGER) DAY AS start_date,
    DATE '2024-07-01' + INTERVAL (floor(random() * 180)::INTEGER) DAY AS end_date
FROM generate_series(1, 20) t(n);

-- ============================================================
-- leads — 100 rows
-- ============================================================

INSERT INTO leads
SELECT
    n::INTEGER                                                             AS id,
    (['Alice','Bob','Carol','David','Eva','Frank','Grace','Hector','Isla','James'])
        [(floor(random() * 10)::INTEGER) + 1]                             AS first_name,
    (['Smith','Jones','Williams','Brown','Taylor','Davies','Wilson','Evans','Thomas','Roberts'])
        [(floor(random() * 10)::INTEGER) + 1]                             AS last_name,
    'lead' || n || '@' ||
    (['acme.com','globex.io','initech.net','umbrella.co','waynetech.com'])
        [(floor(random() * 5)::INTEGER) + 1]                              AS email,
    (['Acme Corp','Globex','Initech','Umbrella Ltd','Wayne Tech', NULL])
        [(floor(random() * 6)::INTEGER) + 1]                              AS company,
    (floor(random() * 20)::INTEGER + 1)                                   AS source_campaign,
    DATE '2024-01-01' + INTERVAL (floor(random() * 365)::INTEGER) DAY     AS created_at
FROM generate_series(1, 100) t(n);

-- ============================================================
-- impressions — 500 rows
-- ============================================================

INSERT INTO impressions
SELECT
    n::INTEGER                                                          AS id,
    (floor(random() * 20)::INTEGER + 1)                                AS campaign_id,
    IF(random() < 0.25, 'click', 'impression')                         AS event_type,
    DATE '2024-01-01' + INTERVAL (floor(random() * 365)::INTEGER) DAY AS occurred_at,
    (['desktop','mobile','tablet'])
        [(floor(random() * 3)::INTEGER) + 1]                           AS device
FROM generate_series(1, 500) t(n);

-- ============================================================
-- conversions — 30 rows
-- ============================================================

INSERT INTO conversions
SELECT
    n::INTEGER                                                          AS id,
    (floor(random() * 100)::INTEGER + 1)                               AS lead_id,
    (floor(random() * 20)::INTEGER + 1)                                AS campaign_id,
    (['signup','purchase','trial'])
        [(floor(random() * 3)::INTEGER) + 1]                           AS type,
    IF(random() < 0.2, NULL, round(9.99 + (random() * 490)::REAL, 2)) AS value,
    DATE '2024-01-01' + INTERVAL (floor(random() * 365)::INTEGER) DAY AS converted_at
FROM generate_series(1, 30) t(n);

-- ============================================================
-- Sanity check
-- ============================================================

SELECT 'campaigns'   AS tbl, count(*) AS rows FROM campaigns
UNION ALL
SELECT 'leads',       count(*) FROM leads
UNION ALL
SELECT 'impressions', count(*) FROM impressions
UNION ALL
SELECT 'conversions', count(*) FROM conversions;
