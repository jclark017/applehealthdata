#!/bin/bash

cd ~/.ssh
ssh-keygen -t rsa -C “jclark017@gmail.com”

cat id_rsa.pub
# Copy key to github

mkdir applehealthdata
cd applehealthdata/

git init
git clone git@github.com:jclark017/applehealthdata.git

cd applehealthdata/

mkdir hollydata
cd hollydata

# in windows command line
#cd Documents\repos\applehealthdata\hollydata
#pscp export.xml "AWS Instance":applehealthdata/applehealthdata/hollydata

#Alternatively mount the EBS volume to /dev/xvdf, then mount it:
sudo mkdir /healthdata
sudo mount /dev/xvdf /healthdata/
sudo chmod 777 /healthdata

#Set automount
sudo nano /etc/fstab

#add this line
/dev/xvdf       /healthdata     ext4    defaults,nofail         0 0

# From the script directory, run to output
python applehealthdataeventsqlite.py /healthdata/export.xml
