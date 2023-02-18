# Twitter Agent

Twitter agent is using TWINT Library in order to webscrap the twitter account.

You can find the documentation in the next link: [twint library](https://github.com/twintproject/twint/)

## How to install Twitter Agent in a Oracle Linux 8 VM in Oracle Cloud ?

Deploy an Oracle Linux 8 VM in the OCI Console / Compute.

Connect with your SSH Key to your VM and configure the python3 environment.

Create an environment in order to isolate your twitter agent.

```code

 sudo pip3 install virtualenv
 which python3
 virtualenv -p /usr/bin/python3 <your env>
 source <your env>/bin/activate
 
```

## Install all python3 libraries used by Twitter Agent.

```code

pip3 install requests
pip3 install pyyaml
pip3 install pandas
pip3 install openpyxl
pip3 install xlrd
pip3 install cx_Oracle
pip3 install oci
sudo yum install git
pip3 install --upgrade git+https://github.com/twintproject/twint.git@origin/master#egg=twint


pip install -U nltk
pip install -U numpy

pip3 install sklearn
pip3 install vader
pip3 install vaderSentiment


```


## Other links

[Example 1](https://basilkjose.medium.com/twint-twitter-scraping-without-twitters-api-aca8ba1b210e)

[Medium Example](https://medium.com/analytics-vidhya/how-to-scrape-tweets-from-twitter-with-python-twint-83b4c70c5536)
