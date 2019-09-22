#! /bin/bash
red_color=$'\033[31;1m'
color_end=$'\033[0m'

echo "installing dependencies"

sudo apt-get install postgresql postgresql-contrib libpq-dev
cargo install diesel_cli --no-default-features --features postgres
printf "\n\n"
echo "Creating Database and default user"
printf "%s[WARNING] change the password of user 'archive'. Default password being used...%s\n" "$red_color" "$color_end"
# echo "[WARNING] change the password of user 'archive'. default password being used..."
printf "If you would rather configure the database yourself, all settings \n\
are stored in .env file in the root directory and init.sql\n\n"

while true; do
    read -p "Do you wish to proceed? [Y\\n] " yn
    case $yn in
        [Yy]* ) sudo -u postgres psql -f init.sql; break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done
