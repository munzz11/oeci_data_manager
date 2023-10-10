#!/bin/bash

### Usage:
#
##  Test pull of the data over a wired Ethernet connection to the boat.
#  ./data_pull.sh dryrun
## Pull of the data over a wired Ethernet connection to the boat.
#  ./data_pull.sh
#  ./data_pull.sh wire
## Pull of the data over the mbr
#  ./data_pull.sh mbr
## Pull of the data over wifi
#  ./data_pull.sh wifi

### EDIT THIS ####
# Path to raw data in operator station archive (robonuc)
EXPEDITION="DX082303"
CRUISE="NA155"
RAW="/home/field/robonuc_docs/${CRUISE}/${EXPEDITION}/drix08/02-raw"

### DO NOT EDIT BELOW HERE ###
# Archive path on echo
ARCHIVE='/home/field/data/mnt/external_drive/archive'

LINK=$1

if [ `echo ${LINK} | grep dryrun` ]
then
    DRYRUN='--dry-run'
    LINK=`echo "${LINK}" | sed 's/dryrun//g'`
else
    DRYRUN=''
fi

if [ "${LINK}" == '' ] || [ `echo "${LINK}" | grep wire` ]
then
    ECHOIP='192.168.8.180'
elif [ `echo ${LINK} | grep mbr` ]
then
    ECHOIP='172.16.11.8'
    ECHOPORT='13001'
elif [ `echo ${LINK} | grep wifi` ]
then
    ECHOIP='172.16.10.8'
    ECHOPORT='13001'
fi



# A failed experiment to filter data by date/time so we only archive
# data while deployed. Could not figure out how to get the paths right
# ----
#FILTER=`echo ${ARCHIVE} | sed 'sv\/v\\\/vg'`
#FILES2TRANSFER=`ssh ${ECHOIP} "find ${ARCHIVE} -type f -newermt '2023-10-09 23:00:00'"`
#echo "${FILES2TRANSFER}"
#echo ${FILES2TRANSFER} | rsync -ravPz ${DRYRUN} --files-from=- ${ECHOIP}:/ ${RAW}/
# ----

if [ "${LINK}" = '' ] || [ `echo "${LINK}" | grep wire` ]
then
   # Use this one for transfers via a direct connection to DriX.
   echo "Pulling data directly over the wire."
   rsync -ravPz ${DRYRUN} ${ECHOIP}:${ARCHIVE}/ ${RAW}/
else

   # Use this one for transfers over wifi or mbr:
   echo "Pulling data by mbr or wifi"
   rsync -ravPz -e "ssh -p ${ECHOPORT}" ${DRYRUN} ${ECHOIP}:${ARCHIVE}/ ${RAW}/
fi