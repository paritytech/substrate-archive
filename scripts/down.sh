#! /bin/bash

echo 'Database Name: '
read DB_NAME

read -p "This will delete any data in the database. Are you sure? " -n 1 -r
echo 
if [[ $REPLY =~ ^[Yy]$ ]]
then
  echo "Dropping Database ${DB_NAME}"
  sudo -u postgres dropdb "${DB_NAME}"
fi
