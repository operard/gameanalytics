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

Install python3.10

https://www.centlinux.com/2022/06/how-to-install-python-310-on-rocky-linux.html

Create an environment in order to isolate your GameAnalytics agents.

```code

sudo pip3.10 install virtualenv
which python3.10
virtualenv -p /usr/bin/python3.10 gameanalytics
source gameanalytics/bin/activate
 
```

## Install all python3 libraries used by GameAnalytics Agent.

```code

pip install -r gameanalytics/requirements.txt

```


## Other links






# Connect to the database
!pip install oracledb


def initialize_database_connection_and_table():
    # Initializes the connection to the database (we will use this to store the tracking information)
    import oracledb
    connection = oracledb.connect(
        user='admin',
        password='_Oracle12345',
        dsn='pl_high',
        config_dir='/home/datascience/wallet',
        wallet_location='/home/datascience/wallet',
        wallet_password='_Oracle12345')
    cur = connection.cursor()
    try:
        cur.execute("create table tracking(framenumber number, object varchar2(10), shirt_number number, position_x number, position_y number)")
    except Exception:
        print("Could not create the tracking table, moving on...")
    try:
        cur.execute("truncate table tracking")
    except Exception:
        print("Could not empty the tracking table")
    return connection, cur

