# Tutorials 
These examples are provided to showcase the possibility of the API and the adaptability of nawalny library. 
As mentionned in INSTALL.txt, these files are installed in :
\$HOME/py3nm/lib64/python3.12/site-packages/nawalny/examples/ 

## nawalny_ps_demo.py: working with Publish/Subscribe principle 

``` 
./nawalny_ps_demo.py --help 
Usage: nawalny_ps_demo.py [--help] [--debug] --nmconf=<file> --cmd=<cmd> [--subconf=<file> | --uuid=uuid]
 cmd among create,update,retrieve,resume,pause,delete,pull,list,check,listen 
 create,update requires a subconf definition 
 retrieve,resume,pause,delete,pull require the uuid of the subscription 
 list,check have no arg 
 listen with no arg listen all , with uuid arg listen only this subscription
 cmd=list is the default and list all subscriptions  

``` 
 This simple command with your conf parameters should give you the list of your subscription 
 which should be empty at start . 

``` 
./nawalny_ps_demo.py --conf=$WD/my.conf --cmd=list 

``` 

## howto04na_r01.py : downloading the whole set of documentation, API, examples. 

Working with GeneralinformationServices_PREOPS_27.0.0.wsdl set of services only is **very limited**. 
The whole purpose of howto04na_r01.py  is to use GeneralinformationServices to download all
the other parts of the documentation, API, examples that are hosted on NMB2B site. 
It looks like a bootstrap process, isn't it ? 
The GeneralinformationServices services used by howto04na_r01.py are : 
retrieveNMReleaseInformation, queryNMB2BReferenceManuals, queryNMB2BWSDLs, queryNMB2BScenarios, queryNMB2BAddendaErrata . 

``` 
./howto04na_r01.py --help 
howto04na_r01.py with ERROR level of logging
Usage: howto04na_r01.py [--help] [--debug] [--nodownload] --dir=<dir> --conf=<file> 
dir should be a directory with write auth , if it does not exist we attempt to create it 
conf file defines where to find NMB2B definition, certificates stuff 
--nodownload allows to extract list of files ONLY and stop before downloading file (testing purposes)
``` 

These two command should give you first the assesmment that everything is working, at second  
the required set of files - be patient a total of 60 Mb of compressed data will be obtained this way.

``` 
# first to test 
./howto04na_r01.py --conf=$WD/my.conf --nodownload 

# second to download actually to a temp dir HOME/tmp/nmreffile   created by the occasion 
./howto04na_r01.py --conf=$WD/my.conf --dir=$HOME/tmp/nmreffile 

``` 




