INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:/demo_db' AS my_data (DATA_PATH '/data/demo_db');
CREATE TABLE demo_db.main.demo (key STRING,value STRING,partition INT);
ALTER TABLE demo_db.main.demo SET PARTITIONED BY (partition);
INSERT INTO demo_db.main.demo VALUES('k00', 'v00', 0),('k01', 'v01', 0),('k51', 'v51', 1),('k61', 'v61', 1);