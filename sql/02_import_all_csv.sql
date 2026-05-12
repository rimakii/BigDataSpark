TRUNCATE TABLE mock_data;

-- ВАЖНО:
-- CSV должны лежать в папке "исходные данные" рядом с docker-compose.yml.
-- Имена файлов при необходимости поменяй ниже под свои реальные имена.

\copy mock_data FROM '/data/mock_data.csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (1).csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (2).csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (3).csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (4).csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (5).csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (6).csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (7).csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (8).csv' WITH (FORMAT csv, HEADER true);
\copy mock_data FROM '/data/mock_data (9).csv' WITH (FORMAT csv, HEADER true);
