#!/bin/sh

env >> /etc/environment

gzip -dvf /usr/src/app/dataStore/**/*.gz

# execute CMD
echo "$@"
exec "$@"
