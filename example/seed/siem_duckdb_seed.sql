-- SIEM seed data for DuckDB
-- Run: duckdb example/siem.duckdb < example/siem_duckdb_seed.sql

-- ============================================================
-- Schema
-- ============================================================

DROP TABLE IF EXISTS alerts;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS hosts;

CREATE TABLE hosts (
    id            INTEGER PRIMARY KEY,
    hostname      TEXT NOT NULL,
    ip_address    TEXT NOT NULL,
    os            TEXT NOT NULL,      -- windows | linux | macos
    environment   TEXT NOT NULL,      -- prod | dev | staging
    owner         TEXT,
    registered_at DATE NOT NULL
);

CREATE TABLE users (
    id              INTEGER PRIMARY KEY,
    username        TEXT NOT NULL UNIQUE,
    full_name       TEXT NOT NULL,
    department      TEXT NOT NULL,    -- engineering | finance | hr | ops | security
    role            TEXT NOT NULL,    -- analyst | engineer | admin | manager
    privilege_level TEXT NOT NULL,    -- standard | elevated | admin
    created_at      DATE NOT NULL
);

CREATE TABLE events (
    id          INTEGER PRIMARY KEY,
    host_id     INTEGER NOT NULL REFERENCES hosts(id),
    user_id     INTEGER REFERENCES users(id),  -- NULL for system events
    event_type  TEXT NOT NULL,   -- login | logout | file_access | network_connection |
                                 -- privilege_escalation | process_spawn | dns_query
    severity    TEXT NOT NULL,   -- info | low | medium | high | critical
    source_ip   TEXT,
    dest_ip     TEXT,
    occurred_at TIMESTAMP NOT NULL,
    raw_message TEXT NOT NULL
);

CREATE TABLE alerts (
    id              INTEGER PRIMARY KEY,
    event_id        INTEGER NOT NULL REFERENCES events(id),
    host_id         INTEGER NOT NULL REFERENCES hosts(id),
    rule_name       TEXT NOT NULL,
    mitre_tactic    TEXT NOT NULL,
    mitre_technique TEXT NOT NULL,
    status          TEXT NOT NULL,    -- open | investigating | closed | false_positive
    severity        TEXT NOT NULL,    -- low | medium | high | critical
    created_at      TIMESTAMP NOT NULL,
    closed_at       TIMESTAMP         -- NULL if still open
);

-- ============================================================
-- hosts — 20 rows
-- ============================================================

INSERT INTO hosts
SELECT
    n::INTEGER                                                              AS id,
    (['web','app','db','cache','proxy','worker','monitor','bastion'])
        [(floor(random() * 8)::INTEGER) + 1] || '-' ||
    (['prod','dev','stg'])
        [(floor(random() * 3)::INTEGER) + 1] || '-' ||
    lpad(n::TEXT, 2, '0')                                                   AS hostname,
    '10.' || (floor(random() * 3)::INTEGER + 1)::TEXT || '.' ||
    (floor(random() * 254)::INTEGER + 1)::TEXT || '.' ||
    (floor(random() * 254)::INTEGER + 1)::TEXT                              AS ip_address,
    (['windows','linux','linux','linux','macos'])
        [(floor(random() * 5)::INTEGER) + 1]                                AS os,
    (['prod','prod','dev','staging'])
        [(floor(random() * 4)::INTEGER) + 1]                                AS environment,
    (['alice.smith','bob.jones','carol.w','david.b', NULL])
        [(floor(random() * 5)::INTEGER) + 1]                                AS owner,
    DATE '2023-01-01' + INTERVAL (floor(random() * 365)::INTEGER) DAY       AS registered_at
FROM generate_series(1, 20) t(n);

-- ============================================================
-- users — 50 rows
-- ============================================================

INSERT INTO users
SELECT
    n::INTEGER                                                              AS id,
    (['alice','bob','carol','david','eva','frank','grace','hector','isla','james',
      'karen','leo','mia','nick','olivia','paul','quinn','rosa','sam','tara'])
        [(floor(random() * 20)::INTEGER) + 1] || '.' ||
    (['smith','jones','williams','brown','taylor','davies','wilson','evans'])
        [(floor(random() * 8)::INTEGER) + 1] || n::TEXT                     AS username,
    (['Alice','Bob','Carol','David','Eva','Frank','Grace','Hector','Isla','James',
      'Karen','Leo','Mia','Nick','Olivia','Paul','Quinn','Rosa','Sam','Tara'])
        [(floor(random() * 20)::INTEGER) + 1] || ' ' ||
    (['Smith','Jones','Williams','Brown','Taylor','Davies','Wilson','Evans'])
        [(floor(random() * 8)::INTEGER) + 1]                                AS full_name,
    (['engineering','finance','hr','ops','security'])
        [(floor(random() * 5)::INTEGER) + 1]                                AS department,
    (['analyst','engineer','engineer','admin','manager'])
        [(floor(random() * 5)::INTEGER) + 1]                                AS role,
    (['standard','standard','standard','elevated','admin'])
        [(floor(random() * 5)::INTEGER) + 1]                                AS privilege_level,
    DATE '2022-01-01' + INTERVAL (floor(random() * 730)::INTEGER) DAY       AS created_at
FROM generate_series(1, 50) t(n);

-- ============================================================
-- events — 500 rows
-- ============================================================

INSERT INTO events
SELECT
    n::INTEGER                                                              AS id,
    (floor(random() * 20)::INTEGER + 1)                                     AS host_id,
    IF(random() < 0.15, NULL, floor(random() * 50)::INTEGER + 1)            AS user_id,
    (['login','login','logout','file_access','file_access','network_connection',
      'privilege_escalation','process_spawn','dns_query'])
        [(floor(random() * 9)::INTEGER) + 1]                                AS event_type,
    (['info','info','info','low','low','medium','high','critical'])
        [(floor(random() * 8)::INTEGER) + 1]                                AS severity,
    '10.' || (floor(random() * 3)::INTEGER + 1)::TEXT || '.' ||
    (floor(random() * 254)::INTEGER + 1)::TEXT || '.' ||
    (floor(random() * 254)::INTEGER + 1)::TEXT                              AS source_ip,
    IF(random() < 0.3, NULL,
        '10.' || (floor(random() * 3)::INTEGER + 1)::TEXT || '.' ||
        (floor(random() * 254)::INTEGER + 1)::TEXT || '.' ||
        (floor(random() * 254)::INTEGER + 1)::TEXT)                         AS dest_ip,
    TIMESTAMP '2024-01-01 00:00:00' +
        INTERVAL (floor(random() * 525600)::INTEGER) MINUTE                 AS occurred_at,
    (['Authentication success','Authentication failure','File read','File write',
      'Outbound connection established','Process created','DNS lookup performed',
      'Privilege elevation attempted','User session ended'])
        [(floor(random() * 9)::INTEGER) + 1] ||
    ' on host ' || (floor(random() * 20)::INTEGER + 1)::TEXT               AS raw_message
FROM generate_series(1, 500) t(n);

-- ============================================================
-- alerts — 30 rows
-- ============================================================

INSERT INTO alerts
SELECT
    n::INTEGER                                                              AS id,
    (floor(random() * 500)::INTEGER + 1)                                    AS event_id,
    (floor(random() * 20)::INTEGER + 1)                                     AS host_id,
    (['Brute Force Detected','Suspicious Login','Lateral Movement',
      'Data Exfiltration','Privilege Escalation','Malware Execution',
      'C2 Beacon','Unusual DNS','Credential Dumping','Policy Violation'])
        [(floor(random() * 10)::INTEGER) + 1]                               AS rule_name,
    (['initial_access','execution','persistence','privilege_escalation',
      'lateral_movement','exfiltration','command_and_control'])
        [(floor(random() * 7)::INTEGER) + 1]                                AS mitre_tactic,
    (['T1078','T1059','T1053','T1003','T1021','T1041','T1071','T1110','T1136','T1543'])
        [(floor(random() * 10)::INTEGER) + 1]                               AS mitre_technique,
    (['open','open','investigating','closed','false_positive'])
        [(floor(random() * 5)::INTEGER) + 1]                                AS status,
    (['low','medium','medium','high','critical'])
        [(floor(random() * 5)::INTEGER) + 1]                                AS severity,
    TIMESTAMP '2024-01-01 00:00:00' +
        INTERVAL (floor(random() * 525600)::INTEGER) MINUTE                 AS created_at,
    IF(random() < 0.5, NULL,
        TIMESTAMP '2024-06-01 00:00:00' +
        INTERVAL (floor(random() * 262800)::INTEGER) MINUTE)                AS closed_at
FROM generate_series(1, 30) t(n);

-- ============================================================
-- Sanity check
-- ============================================================

SELECT 'hosts'   AS tbl, count(*) AS rows FROM hosts
UNION ALL
SELECT 'users',   count(*) FROM users
UNION ALL
SELECT 'events',  count(*) FROM events
UNION ALL
SELECT 'alerts',  count(*) FROM alerts;
