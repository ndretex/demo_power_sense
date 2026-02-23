-- Avoid GRANT ALL here: newer ClickHouse versions include row-policy grants in ALL,
-- which can fail during bootstrap if grant-option for those privileges is unavailable.
CREATE USER IF NOT EXISTS electricity IDENTIFIED BY 'electricity';

GRANT
    SELECT,
    INSERT,
    ALTER,
    CREATE TABLE,
    CREATE VIEW,
    DROP TABLE,
    DROP VIEW,
    TRUNCATE,
    OPTIMIZE
ON electricity.* TO electricity;
