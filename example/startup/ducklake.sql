INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:/data/ducklake_demo/metadata.ducklake' AS demo_db (DATA_PATH '/data/ducklake_demo');
CREATE TABLE demo_db.main.demo (key STRING,value STRING,partition INT);
ALTER TABLE demo_db.main.demo SET PARTITIONED BY (partition);
INSERT INTO demo_db.main.demo VALUES('k00', 'v00', 0),('k01', 'v01', 0),('k51', 'v51', 1),('k61', 'v61', 1);