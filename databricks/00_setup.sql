CREATE CATALOG IF NOT EXISTS student_streaming;
CREATE SCHEMA IF NOT EXISTS student_streaming.market_demo;

USE CATALOG student_streaming;
USE SCHEMA market_demo;

CREATE VOLUME IF NOT EXISTS raw_ticks;
CREATE VOLUME IF NOT EXISTS checkpoints;
CREATE VOLUME IF NOT EXISTS schemas;

