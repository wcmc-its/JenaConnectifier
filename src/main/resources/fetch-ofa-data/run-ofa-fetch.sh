#!/bin/bash

export JENACONNECTIFIER_INSTALL_DIR=/Users/szd2013/git/vivo-import-data/vivo-import-data
export HARVEST_NAME=import-ofa
export DATE=`date +%Y-%m-%d'T'%T`
export JENACONNECTIFIER_DIR=$JENACONNECTIFIER_INSTALL_DIR/src/main/resources/fetch-ofa-data

# Add harvester binaries to path for execution
# The tools within this script refer to binaries supplied within the harvester
#	Since they can be located in another directory their path should be
#	included within the classpath and the path environment variables.
export PATH=$PATH:$JENACONNECTIFIER_INSTALL_DIR/bin
export CLASSPATH=$CLASSPATH:$JENACONNECTIFIER_INSTALL_DIR/target/vivo-import-data-0.0.1-SNAPSHOT-jar-with-dependencies.jar:$JENACONNECTIFIER_INSTALL_DIR/lib/*.jar
export CLASSPATH=$CLASSPATH:$JENACONNECTIFIER_INSTALL_DIR/lib/classes12.jar

# Exit on first error
# The -e flag prevents the script from continuing even though a tool fails.
#	Continuing after a tool failure is undesirable since the harvested
#	data could be rendered corrupted and incompatible.
set -e

## Do not run if a handshake file exists
if [ ! -f "handshake" ]; then

touch handshake


echo "Full Logging in $HARVEST_NAME.$DATE.log"
if [ ! -d logs ]; then
  mkdir logs
fi
cd logs
touch $HARVEST_NAME.$DATE.log
ln -sf $HARVEST_NAME.$DATE.log $HARVEST_NAME.latest.log
cd ..

fetch-ofa-data

echo 'Fetch completed successfully'

rm handshake

fi

## === Send email alerts if exceptions are found ===

export JENACONNECTIFIER_DIR=$JENACONNECTIFIER_INSTALL_DIR/src/main/resources/fetch-ofa-data

EXCEPTION=$(grep Exception $JENACONNECTIFIER_DIR/logs/import-ofa.latest.log)

if [ "$EXCEPTION" != "" ]; then
        HOST="{your host name}"
        echo "$EXCEPTION" | mail -s $HOST": Exceptions found in "$HARVEST_NAME.$DATE.log {your email address}
fi
