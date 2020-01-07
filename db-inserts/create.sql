-- CREATE DATABASE shakespeare;
-- USE DATABASE shakespeare;

-- DROP TABLE IF EXISTS document;

CREATE TABLE IF NOT EXISTS document (
  id INTEGER NOT NULL AUTO_INCREMENT,
  type VARCHAR(255),
  line_id INTEGER,
  play_name VARCHAR(255),
  speech_number INTEGER,
  line_number VARCHAR(255),
  speaker VARCHAR(255),
  line VARCHAR(60000),

  PRIMARY KEY (id)
);
