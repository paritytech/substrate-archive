#! /bin/bash

echo 'Initializing new database...'

echo 'Database Name: '
read DB_NAME
echo -n Password:
read -s PASSWORD 
echo

if [ -z "$PASSWORD" ]
then
  echo "Empty password provided, setting password to '123'"
  PASSWORD=123
fi

set_environment() {
  export DATABASE_URL="postgres://postgres:${PASSWORD}@localhost:5432/${DB_NAME}"
}

# Check if database exists
if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw $DB_NAME; then
  echo "Database already exists. Setting DATABASE_URL. This will not work if you haven't 'sourced' this script."
  set_environment
else
  echo "Setting DATABASE_URL for current user environment. This will not work if you haven't 'sourced' this script."
  sudo -u postgres createdb "${DB_NAME}"
  set_environment
fi

