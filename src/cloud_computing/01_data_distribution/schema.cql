-- Data distribution strategy for airline on-time performance data on Apache
-- Cassandra. These are demonstrative code snippets (the "code snippets" outcome
-- of the task): they show the replication factor, the partition key, and how the
-- main queries use that partition key. A live cluster is optional for this task,
-- so running this file is not required; if you want to, use: cqlsh -f schema.cql

-- 1. Keyspace = the replication factor decision.
--    NetworkTopologyStrategy (rack/DC aware) with RF 3 in datacenter dc1:
--    survives one node down and allows strongly-consistent QUORUM (2 of 3).
CREATE KEYSPACE IF NOT EXISTS pex_airline
    WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};

USE pex_airline;

-- 2. Table = the partition key decision.
--    Partition key (origin, year, month) decides which node owns the row and
--    spreads a skewed dataset evenly (one partition per airport-month).
--    Clustering columns (day, carrier, flight_num) sort rows inside a partition
--    for fast range reads.
CREATE TABLE IF NOT EXISTS flights_by_origin (
    origin      text,
    year        int,
    month       int,
    day         int,
    carrier     text,
    flight_num  int,
    dest        text,
    dep_delay   int,
    arr_delay   int,
    distance    int,
    PRIMARY KEY ((origin, year, month), day, carrier, flight_num)
);

-- 3. Sample rows.
INSERT INTO flights_by_origin (origin, year, month, day, carrier, flight_num, dest, dep_delay, arr_delay, distance)
    VALUES ('ATL', 2008, 6, 1, 'DL', 100, 'JFK',  5, -3,  760);
INSERT INTO flights_by_origin (origin, year, month, day, carrier, flight_num, dest, dep_delay, arr_delay, distance)
    VALUES ('ATL', 2008, 6, 1, 'DL', 220, 'LAX', 12, 18, 1946);
INSERT INTO flights_by_origin (origin, year, month, day, carrier, flight_num, dest, dep_delay, arr_delay, distance)
    VALUES ('ATL', 2008, 6, 3, 'WN', 415, 'MCO',  0, -7,  404);

-- 4. W1: airport-month performance. Uses the FULL partition key -> single
--    partition -> the coordinator contacts only that token's replicas.
SELECT count(*) AS flights, avg(arr_delay) AS avg_arr_delay
FROM flights_by_origin
WHERE origin = 'ATL' AND year = 2008 AND month = 6;

-- 5. W2: flight history inside that partition. Clustering-range read on sorted
--    rows -> sequential scan within one node.
SELECT day, carrier, flight_num, dest, dep_delay, arr_delay
FROM flights_by_origin
WHERE origin = 'ATL' AND year = 2008 AND month = 6 AND day <= 3;

-- 6. Show how a partition key becomes a token (the value that decides the node).
--    Different airport-months hash to different tokens -> even distribution.
SELECT origin, year, month, token(origin, year, month) AS partition_token
FROM flights_by_origin;

-- 7. From a terminal (not cqlsh), inspect placement and balance:
--    nodetool status pex_airline   -- ownership % per node (balance)
--    nodetool ring                 -- token assignment around the ring
