#!/bin/bash

# A script to collect data from various sources in the DriX in preparation
# for transfer to shore or elsewhere

DATATOPULL="$1"
if [ "${DATATOPULL}" = '' ]
then
    DATATOPULL="ctd,mbes,ek80,gps,ins,drix,p11"
fi

echo "DATA TO PULL: ${DATATOPULL}"

MISSIONSTART='2023-10-01 00:00:00'
MBESMISSIONSURVEY='NA155_DX082303_NautilusTechChallenge'
DRYRUN='--dry-run'
#DRYRUN=''

# SOURCE PATHS
SURVEYPCMOUNT='/home/field/data/mnt/surveypc'
MDTMOUNT='/home/field/data/mnt/mdt'
P11LOGS='/home/field/project11/logs'

# DESTINATION PATHS
ARCHIVEMOUNT='/home/field/data/mnt/external_drive'
ARCHIVE="${ARCHIVEMOUNT}/archive"
DRIXMDT="${ARCHIVE}/drix/vehicle/mdt"
EK80="${ARCHIVE}/ek80"
GPS="${ARCHIVE}/gps"
MBES="${ARCHIVE}/mbes"
PHINS="${ARCHIVE}/phins"
CTD="${ARCHIVE}/ctd"
PROJECT11="${ARCHIVE}/drix/payload/project11"

if /usr/bin/mountpoint -q ${ARCHIVEMOUNT};
then
    echo "Archive is mounted."
else
    echo "Archive is not mounted. Exiting."
    exit 1
fi


if /usr/bin/mountpoint -q ${MDTMOUNT};
then
    echo "MDT is mounted."
else
    echo "MDT is not mounted."
    exit 1
fi

if /usr/bin/mountpoint -q ${SURVEYPCMOUNT};
then
    echo "Suvey PC is mounted."
else
    echo "Survey PC is not mounted."
    exit 1
fi

# Set up directory structure (does nothing if done already)
mkdir -p ${ARCHIVE}
mkdir -p ${DRIXMDT}
mkdir -p ${EK80}
mkdir -p ${GPS}
mkdir -p ${MBES}
mkdir -p ${PHINS}
mkdir -p ${CTD}
mkdir -p ${PROJECT11}

CWD=`pwd`

# PULL THE DATA
if [ `echo "${DATATOPULL}" | grep drix` ]
then
    echo "Pulling drix data"
    # Get MDT data
    cd /home/field/data/mnt/mdt/logs
    # The grep here ensures new sensor log buffers are not copied every time.
    find . -type f -newermt "${MISSIONSTART}" | grep _logs | rsync -ravP --files-from=- ${DRYRUN} ./ ${DRIXMDT}
fi

if [ `echo "${DATATOPULL}" | grep mbes` ]
then
    echo "Pulling mbes data"
    # Get Kongsberg data
    rsync -ravP ${DRYRUN} ${SURVEYPCMOUNT}/sisdata/raw/${MBESMISSIONSURVEY} ${MBES} 
fi

if [ `echo "${DATATOPULL}" | grep gps` ]
then
    echo "Pulling gps data"
    # Get GPS data
    cd ${SURVEYPCMOUNT}/GNSS\ Data
    find . -type f -newermt "${MISSIONSTART}" | rsync -ravP ${DRYRUN} --files-from=- ./ ${GPS}
fi

if [ `echo "${DATATOPULL}" | grep ins` ]
then
    echo "Pulling ins data"
    # Get INS data
    cd ${SURVEYPCMOUNT}/INS\ Data
    find . -type f -newermt "${MISSIONSTART}" | rsync -ravP --files-from=- ${DRYRUN} ./ ${PHINS}
fi
 
if [ `echo "${DATATOPULL}" | grep ctd` ]
then
    echo "Pulling ctd data"
    # Get CTD data
    cd ${SURVEYPCMOUNT}/SVP\ Exports
    find . -type f -newermt "${MISSIONSTART}" | rsync -ravP --files-from=- ${DRYRUN} ./ ${CTD}
fi

if [ `echo "${DATATOPULL}" | grep ek80` ]
then
    echo "Pulling ek80 data" 
    # Get EK80 data
    cd ${SURVEYPCMOUNT}/EK80\ Data
    find . -type f -newermt "${MISSIONSTART}" | rsync -ravP --files-from=- ${DRYRUN} ./ ${EK80}
fi

if [ `echo "${DATATOPULL}" | grep p11` ]
then
    echo "Organizing the project11 logs"
    files=`find ${P11LOGS} -type f`
    for f in $files 
        do 
	# Get the date of the log file creation and use this to 
	# create the directory.
        tmp=`echo $f | sed 's/.*project11_//'`; 
        DIR="${P11LOGS}/${tmp:0:10}"
        mkdir -p ${DIR}
        mv $f $DIR/
    done

    echo "Pulling project11 data" 
    # Get Project11 data
    cd ${P11LOGS}
    find . -type d -newermt "${MISSIONSTART}" | grep -v -e logs$ | grep -v archive | rsync -ravP --files-from=- ${DRYRUN} ./ ${PROJECT11}
fi

cd ${CWD}

