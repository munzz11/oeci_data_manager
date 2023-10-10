# OECI Data Manager
Running oeci_data_manager.py without args will produce usage information.

The fist step is to initialize a new project.

oeci_data_manager.py init --source /mount/data/DX1234

Next, the data can be processed.

oeci_data_manager.py process --project DX1234

Scan can be used to see what needs processing. This is useful when new data
is added to the source directory.

oeci_data_manager.py scan --project DX1234

# UTILITY SCRIPTS

## data_sync.sh

`data_sync.sh` is a script to synchronize logs between various sources within the DriX USV into a single linux-based archive in preparation for offload to shore data stores. This prepatory step is done because Linux is more easily scriptable, forgiving when things go wrong, and tunable for various amounts of bandwidth. `data_sync.sh` is run on "echo", the backseat driver aboard the DriX-8 USV, with the aid of an attached external drive for storage space. 

`data_sync.sh` is not to be run blindly! At the beginning of the cruise, the user must edit `data_sync.sh` and specify the start date of the data archive, and the name of the MBES "Survey". Look for this block:

```
...
MISSIONSTART='2023-10-01 00:00:00'
MBESMISSIONSURVEY='NA155_DX082303_NautilusTechChallenge'
DRYRUN='--dry-run'
#DRYRUN=''
...
```
When running the script one may comment out the optional definitions of DRYRUN (see above) to selectively report all files to be transfered without actually transferring any files.

Before running `data_sync.py` one must have the drives mounted from which it reads, and to where it writes. This can be done by executing `mount_drix_data_drives_on_echo.sh` (also in this repo), whose contents are reproduced here so you can see what it does and where the drives are mounted:

```
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
```
** At the moment, sshpass is not working properly here, and so one must manually
enter passwords at each step. These can be found in the DriX operator's information. **

Run `data_sync.py` with optional arguments which may be one of or more of `ctd,mbes,ek80,gps,ins,drix,p11`, specifying collection only those data types. Omitting all of these will collect all data types.

```
# Collect ctd and mbes data:
./data_sync.py ctd,mbes
# Collect gps and ins data:
./data_sync.py gps,ins
# Collect DriX vehicle logs:
./data_sync.py drix
# Collect all logs
./data_sync.py
```
## data_pull.sh
The `data_pull.sh` script lives on robonuc, and its purpose is to pull the data archive created by `data_sync.sh` to the operator station aboard robonuc. This script is also not to be run blindly. 

Open `data_pull.sh` and edit the **EXPEDITION** name and **CRUISE** items to ensure the data goes to the right place. Run `data_pull.sh` like this:

```
# Test pull data over the wire:
./data_pull.sh dryrun
# Pull data over th wire:
./data_pull.sh 
./data_pull.sh wire
# Pull data over the mbr:
./data_pull.sh mbr
# Test pull data over the wifi:
./data_pull.sh wifi,dryrun
```

