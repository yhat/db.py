dropdb --if-exists -U glamp Chinook
createdb -U glamp Chinook
psql -f Chinook_PostgreSql.sql -q Chinook glamp
