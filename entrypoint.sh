#!/bin/sh

env >> /etc/environment

gzip -dv /usr/src/app/dataStore/**/*.gz

# execute CMD
echo "$@"
exec "$@"
