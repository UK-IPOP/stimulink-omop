#!/bin/bash

# ! BE SURE THE POS AND WSD SERVERS ARE RUNNING
# ^ that should be handled by the python script, but if not
# they can be started by running the `start_mm_servers.sh` script in the home directory

if [ $# -ne 1 ]
then
	echo "Usage: mm_test <text>"
	exit
fi

echo $1 | timeout 300 ~/public_mm/bin/metamap -Ny --silent -R SNOMEDCT_US 