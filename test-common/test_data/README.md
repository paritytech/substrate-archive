# Test Data

Test data is gathered by pulling from synced substrate-archive databases, and copying to a CSV:


This can be done in PSQL, for example use:
```sql
\copy (SELECT * FROM blocks WHERE spec = 25 LIMIT 5) To '/path/to/output/file.csv' WITH CSV;
```

to copy 5 blocks from runtime version 25 to a CSV file

