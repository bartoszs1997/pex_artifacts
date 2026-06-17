-- Optional JDBC join setup (MySQL).
-- Creates the `pex` database and a small series_ratings table whose series_id
-- joins to the JSON series id. Self-contained: open in MySQL Workbench and run
-- the whole script (the lightning-bolt button), or from the terminal:
--   mysql -h 127.0.0.1 -P 3306 -u <user> -p < setup_ratings.sql

CREATE DATABASE IF NOT EXISTS pex;
USE pex;

DROP TABLE IF EXISTS series_ratings;

CREATE TABLE series_ratings (
    series_id         BIGINT        PRIMARY KEY,
    imdb_rating       DECIMAL(3, 1) NOT NULL,
    viewers_millions  DECIMAL(6, 2) NOT NULL
);

INSERT INTO series_ratings (series_id, imdb_rating, viewers_millions) VALUES
    (1399,   9.2, 17.40),  -- Game of Thrones
    (60625,  9.1,  3.20),  -- Rick and Morty
    (1396,   9.5, 10.30),  -- Breaking Bad
    (60059,  8.9,  2.50),  -- Better Call Saul
    (71712,  8.0,  6.10),  -- The Good Doctor
    (105248, 8.6,  1.40),  -- Cyberpunk: Edgerunners
    (95396,  8.7,  3.30),  -- Severance
    (87739,  8.5,  4.20),  -- The Queen's Gambit  (87739 in our dataset, not Squid Game)
    (76479,  8.7,  4.10),  -- The Boys
    (94605,  9.0,  2.80);  -- Arcane
