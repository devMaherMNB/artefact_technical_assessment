-- Create secondary database for Airflow (for later use)
CREATE DATABASE airflow;

-- Connect to primary DB
\c assessment_dw;

-- Create schemas to isolate the two projects
CREATE SCHEMA IF NOT EXISTS supply_chain;
CREATE SCHEMA IF NOT EXISTS online_retail;

-- Enable any common extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
