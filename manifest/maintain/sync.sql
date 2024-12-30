INSERT INTO need_sync_table
SELECT * FROM mysql('domain:3306', 'schema', 'need_sync_table', 'user', 'password');
