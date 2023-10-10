#!/bin/bash

# Mount the data drives on the MDT and SURVEPC read-only.

MDTMOUNT=/home/field/data/mnt/mdt
SURVEYPCMOUNT=/home/field/data/mnt/surveypc
mkdir -p ${MDTMOUNT}
mkdir -p ${SURVEYPCMOUNT}

export SSHPASS=drix
#sshpass -p drix sshfs -p 2222 drix@mdt: ${MDTMOUNT} -o ro
echo "mdt:"
sshfs -p 2222 drix@mdt: ${MDTMOUNT} -o ro
export SSHPASS=Uncrewed1! 
echo "survey:"
sshfs IXBLUE@192.168.8.100:D:/ ${SURVEYPCMOUNT} -o ro
