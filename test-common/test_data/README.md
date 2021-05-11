# Test Data

Test data is gathered by pulling from synced substrate-archive databases, and copying to a CSV:


This can be done in PSQL, for example:
```sql
\copy (Select * From foo) To '/tmp/test.csv' With CSV
```

