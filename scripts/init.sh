#! /bin/bash

# Global Constants
red_color=$'\033[31;1m'
yellow_color=$'\e[33m'
color_end=$'\033[0m'

os_ver() {
	if [ -f /etc/os-release ]; then
	    # freedesktop.org and systemd
	    . /etc/os-release
	    OS=$NAME
	    VER=$VERSION_ID
	elif type lsb_release >/dev/null 2>&1; then
	    # linuxbase.org
	    OS=$(lsb_release -si)
	    VER=$(lsb_release -sr)
	elif [ -f /etc/lsb-release ]; then
	    # For some versions of Debian/Ubuntu without lsb_release command
	    . /etc/lsb-release
	    OS=$DISTRIB_ID
	    VER=$DISTRIB_RELEASE
	elif [ -f /etc/debian_version ]; then
	    # Older Debian/Ubuntu/etc.
	    OS=Debian
	    VER=$(cat /etc/debian_version)
	elif [ -f /etc/SuSe-release ]; then
	    # Older SuSE/etc.
	    ...
	elif [ -f /etc/redhat-release ]; then
	    # Older Red Hat, CentOS, etc.
	    ...
	else
	    # Fall back to uname, e.g. "Linux <version>", also works for BSD, etc.
	    OS=$(uname -s)
	    VER=$(uname -r)
	fi
}

install() {
	if [ $OS == "Fedora" ]; then
		sudo dnf install postgresql postgresql-contrib postgresql-devel postgresql-server
		sudo postgresql-setup --initdb
		sudo systemctl start postgresql
		sudo systemctl enable postgresql
	elif [ $OS == "Ubuntu" ]; then
		sudo apt-get install postgresql postgresql-contrib libpq-dev
	else 
		printf "%s[ERROR] Distribution not supported. Install postgresql dependencies seperately.%s\n", "$red_color" "$color_end"
	fi
	cargo install diesel_cli --no-default-features --features postgres

	printf "\n\n"
	echo "Creating Database and default user"
	printf "%s[WARNING] change the password of user 'archive'. Default password being used...%s\n" "$yellow_color" "$color_end"
	# echo "[WARNING] change the password of user 'archive'. default password being used..."
	printf "If you would rather configure the database yourself, all settings \n\
	are stored in .env file in the root directory and init.sql\n\n"
	cp init.sql /tmp/init.sql
	while true; do
	    read -p "Do you wish to proceed? [Y\\n] " yn
	    case $yn in
		[Yy]* ) sudo -i -u postgres psql -f /tmp/init.sql; break;;
		[Nn]* ) exit;;
		* ) echo "Please answer yes or no.";;
	    esac
	done
}

echo "installing dependencies"
os_ver
install

