-- ## FILE FORMAT ## --
CREATE SCHEMA IF NOT EXISTS FILE_FORMATS; 
USE SCHEMA FILE_FORMATS;

CREATE OR REPLACE FILE FORMAT MANAGE_DB.FILE_FORMATS.JSON_FORMAT
  TYPE = 'JSON'
  COMPRESSION = AUTO
  TRIM_SPACE = TRUE 
  ALLOW_DUPLICATE = FALSE 
  REPLACE_INVALID_CHARACTERS=TRUE;

SHOW FILE FORMATS; 
DESC FILE FORMAT JSON_FORMAT; 