#!/bin/bash

sudo -i -u postgres bash << EOF
psql postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='archive'" | grep -q 1 \
	|| (echo "Run init.sh first" && exit 1)
EOF
(($?)) && exit 1 # check if heredoc finished sucesfully

sudo -i -u postgres bash << EOF
	psql postgres -tAc "CREATE DATABASE test_archive WITH OWNER=archive ENCODING='UTF8' TABLESPACE=pg_default LC_COLLATE='en_US.UTF-8' LC_CTYPE='en_US.UTF-8' CONNECTION LIMIT=25;"
	
EOF

(($?)) && (echo 'error creating database' && exit 1)

psql -d test_archive -U archive -h localhost -p 5432 << EOF
\c test_archive
\i ./../test-data/MOCK_BLOCK_DATA.sql
EOF
