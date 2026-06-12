-- Task 01 optional JDBC join setup.
-- Creates a series_ratings table with rows that join to series.id.
-- Run with: psql -h localhost -p 5433 -U peex -d peex -f setup_pg.sql
--
-- NOTE: imdb_rating and viewers_millions are SYNTHETIC seed data, used only to
-- demonstrate the JDBC read + join mechanics. The series_id values are real
-- TMDB ids verified to exist in tvs.json, so the inner join returns 10 rows.

DROP TABLE IF EXISTS series_ratings;

CREATE TABLE series_ratings (
    series_id          BIGINT PRIMARY KEY,
    imdb_rating        NUMERIC(3, 1) NOT NULL,
    viewers_millions   NUMERIC(6, 2) NOT NULL
);

INSERT INTO series_ratings (series_id, imdb_rating, viewers_millions) VALUES
    (1399,   9.2, 17.40),  -- Game of Thrones
    (60625,  9.1,  3.20),  -- Rick and Morty
    (1396,   9.5, 10.30),  -- Breaking Bad
    (60059,  8.9,  2.50),  -- Better Call Saul
    (71712,  8.0,  6.10),  -- The Good Doctor
    (1622,   8.4,  4.80),  -- Supernatural
    (87739,  8.0, 142.00), -- The Queen's Gambit
    (76479,  8.7,  4.10),  -- The Boys
    (94605,  9.0,  2.80),  -- Arcane
    (1416,   7.6,  9.40);  -- Grey's Anatomy
