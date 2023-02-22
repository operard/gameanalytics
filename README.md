# gameanalytics
Game Analytics Dashboard

## How to install GameAnalytics Agents in a Oracle Linux 8 VM in Oracle Cloud ?

Deploy an Oracle Linux 8 VM in the OCI Console / Compute.

Connect with your SSH Key to your VM and configure the python3 environment.

By default, the python installed is 3.6.8. GameAnalytics needs a python version >= 3.7.

```code
sudo dnf update -y
sudo dnf install curl gcc openssl-devel bzip2-devel libffi-devel zlib-devel wget make -y
sudo dnf install git -y

```

Install python3.9

```code
sudo dnf module install python39
sudo dnf install python3-requests
```

Create an environment in order to isolate your GameAnalytics agents.

```code
sudo pip3.9 install virtualenv
which python3.9
virtualenv -p /usr/local/bin/python3.9 gameanalytics
 
```

Activate GameAnalytics environment.

```code
source gameanalytics/bin/activate
 
```

## Install GameAnalytics Agent Software.

```code
cd /opt
git clone https://github.com/operard/gameanalytics
 
```


## Install all python3 libraries used by GameAnalytics Agent.

```code
pip3.9 install -r /opt/gameanalytics/requirements.txt
```

## Other links



