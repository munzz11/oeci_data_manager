#!/bin/bash

ARCHIVE='/home/field/data/mnt/external_drive/archive'
EXPEDITION="DX082303"
RAW="/home/field/robonuc_docs/NA155/${EXPEDITION}/drix08/02-raw"
#RAW="/home/field/scratch"
ECHOIP='192.168.8.180'
#ECHOIP='172.16.11.8'
#ECHOIP='172.16.10.8'
#ECHOPORT='13001'
#ECHOIP='172.16.10.8'
#ECHOPORT='13001'
#DRYRUN='--dry-run'
DRYRUN=''

# A failed experiment to filter data by date/time so we only archive
# data while deployed. Could not figure out how to get the paths right
# ----
#FILTER=`echo ${ARCHIVE} | sed 'sv\/v\\\/vg'`
#FILES2TRANSFER=`ssh ${ECHOIP} "find ${ARCHIVE} -type f -newermt '2023-10-09 23:00:00'"`
#echo "${FILES2TRANSFER}"
#echo ${FILES2TRANSFER} | rsync -ravPz ${DRYRUN} --files-from=- ${ECHOIP}:/ ${RAW}/
# ----

# Use this one for transfers via a direct connection to DriX.
rsync -ravPz ${DRYRUN} ${ECHOIP}:${ARCHIVE}/ ${RAW}/
# Use this one for transfers over wifi or mbr:
#rsync -ravPz -e "ssh -p ${ECHOPORT}" ${DRYRUN} ${ECHOIP}:${ARCHIVE} ${RAW}/
