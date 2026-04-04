---SQL script to set up the database for the PayFlow case study
CREATE DATABASE payflow_db;

--- Connect to the newly created database
\c payflow_db;

--- Create schemas for operational and analytical data
CREATE SCHEMA operationals;
CREATE SCHEMA analytics;

 COMMIT;

