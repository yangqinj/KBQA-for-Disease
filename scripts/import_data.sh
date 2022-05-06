#!/bin/bash

CUR_PATH=$(cd $(dirname $0); pwd)


# find all entity data files
ENTITY_DAT_PATH=$(cd ${CUR_PATH}/../data/entity; pwd)
echo "Finding all entity data files in directory ${ENTITY_DAT_PATH}"
COMMAND_NODES=""
for file in ${ENTITY_DAT_PATH}/*.csv
do
    COMMAND_NODES+="--nodes=${file} "
done


# find all relationship data files
RELATIONSHIP_DAT_PATH=$(cd ${CUR_PATH}/../data/relationship; pwd)
echo "Finding all relationship data files in directory ${RELATIONSHIP_DAT_PATH}"
COMMAND_RELS=""
for file in ${RELATIONSHIP_DAT_PATH}/*.csv
do
    COMMAND_RELS+="--relationships=${file} "
done

COMMAND="bin/neo4j-admin import --multiline-fields=true --database=disease.db ${COMMAND_NODES} ${COMMAND_RELS}"


NEO4J_PATH="/Users/yangqj/Documents/Install_Package/neo4j-community-3.5.31"
cd ${NEO4J_PATH}
ECHO "start import data $(date)"
$COMMAND
ECHO "finish import data $(date)"

