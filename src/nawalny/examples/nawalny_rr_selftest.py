#!/usr/bin/env python3 
# -*-mode: Python; coding: utf-8; -*-
'''Nawalny nickname for New AWesome Access Library to Nm in pYthon;
this script aims at testing Request / Reply from basic to more sophisticated aspects  '''

import datetime,getopt,logging,os,pprint,re,ssl,sys,time
from traceback import print_exc
from termcolor import colored 
from nawalny import NawalnyAccess,NawalnyRRService,SSLAdapter,snawalnytodaytime,dnawalnyparam

lpathitem = sys.argv[0].split('/')

# this version uses logging 
# CRITICAL,ERROR,WARNING,INFO,DEBUG,
logging.basicConfig(level=logging.INFO)
ologger = logging.getLogger("nawalny")
ologger.setLevel(logging.DEBUG) ## increase to debug nawalny aspect

lcmd = ['runall']

def usage(arg0):
    fmt = '''Usage: {:s} [--help] [--debug] --conf=<file>] 
  cmd among {:s} 
  conf file defines filter parameters and output format see '''
    print( fmt.format(arg0,",".join(lcmd)))

def mygetopt(cmd,largs):
    ''' process argument and if success, return	'''
    lpathitem = sys.argv[0].split('/')
    dopt = {}
    sacmd = lpathitem[-1]
    bdebug = 0 
    sconffn = None     
    suuid = None
    stlist, laddfield = "", [] 
    try:
        optlist, lrargs = getopt.getopt(
            largs,'', ['help','debug','conf=','cmd='])
    except :
        print("a wrong parameters has been found")
        usage(sacmd)
        sys.exit(1)
    for k,v in optlist :
        dopt[k] = v
    if '--help' in dopt :
        usage(sacmd)
        sys.exit(1)
    if '--debug' in dopt:
        bdebug = 1 
    if '--conf' in dopt :
        sconffn = dopt['--conf']
    else:
        print("--conf option is compulsory ")
        usage(sacmd)
        sys.exit(1)
   
    if bdebug :
        print( dopt, len(lrargs))
    return sacmd,bdebug,sconffn

def neatconflines(ofile): 
    lrealine = [] 
    while 1 :
        sali = ofile.readline()
        if sali == "" : break
        elif sali[0] == "#" : pass # discard comment 
        else:
            lrealine.append(sali)
    return "".join(lrealine)

class DESAdapter(SSLAdapter):
    ''' A TransportAdapter that re-enables 3DES support in Requests. 
it show how to subclass SSLAdapter and tweaking ssl things '''
    def _create_ssl_context(self):
        '''same but disallow TLS_V1.3 '''
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.options |= ssl.OP_NO_TLSv1_3
        return ctx  

BTLS13_SUPPORTED = True # à début 2023, tlsv1_3 supported, no need to downgrade

## to highlight result of test 
def success(stxt):
    return colored(stxt,'green')
def failure(stxt):
    return colored(stxt,'red')

if __name__ == '__main__':
    sacmd,bdebug,sconffilename = mygetopt(sys.argv[0],sys.argv[1:])
    ologger.debug("%s %s %s %s",sacmd,bdebug,sconffilename )
    
    try:
        oconffile = open(sconffilename)
    except:
        print("cannot conf file %s " % sconffilename )
        usage(sacmd)
        sys.exit(1)
    llconf = neatconflines(oconffile)
    dmconf = None
    try:
        dmconf = eval(llconf)
    except:
        print("error in configuration %s " % sconffilename)
        print_exc()
        sys.exit(1)
    ologger.info("dmconf is %s", pprint.pformat(dmconf))
    ## std variables to poll NMB2B 
    tnow = datetime.datetime.now(datetime.UTC)
    stodaytime = tnow.strftime("%Y-%m-%d %H:%M:%S")
    stoday = tnow.strftime("%Y-%m-%d")
#    dparam_default = {'endUserId':"Nanwalnytest",'sendTime': snawalnytodaytime()}
    suserdef = "Nanwalnytest"
    # creation d'un accesseur 
    try:
       onmaccess = NawalnyAccess(
           dmconf['swsdlpath'], suserdef, dmconf['nmapiindex'], dmconf['nmcontext'])
    except:
        print("error when building nmaccess")
        print_exc()
        sys.exit(1)
        
    ologger.info("realbackend is %s", onmaccess.get_backend())
    # ce test ne doit pas marcher 
    # lret = onmaccess.test_std()   
    # print(lret)
    try:
        if BTLS13_SUPPORTED :
            onmaccess.set_credential(dmconf['nmcertpath'],dmconf['nmkeypath'],dmconf['nmpassword'])
        else: 
            oaltadapter =  DESAdapter(dmconf['nmcertpath'],dmconf['nmkeypath'],dmconf['nmpassword'],\
                max_retries=0)        
            onmaccess.set_credential(dmconf['nmcertpath'],dmconf['nmkeypath'],dmconf['nmpassword'],\
                oaltadapter )
    except:
        print_exc()
        sys.exit(1)
        
    lret = onmaccess.test_tech()
    # eg. call with "https://www.b2b.nm.eurocontrol.int/B2B_PREOPS/gateway/spec/27.0.0 "  
    # with a preops cert to raise wrong cred 
    print(lret)
    if lret[0] : 
        print(success("Technical Acces to nmb2b is valid"))
    else:
        print(failure("Technical Acces to nmb2b is INVALID.stop."))
        sys.exit(0)
    
    lret = onmaccess.test_std()        
    print(lret[1])
    if lret[0] : 
        print(success("Soap Acces to nmb2b is valid"))
    else:
        print(failure("Soap Acces to nmb2b is INVALID"))
        sys.exit(1)       
    try:
        print(onmaccess.odefaultservs)
    except:
        print_exc()
        sys.exit(1)
    print("first")
    linforeq = ["retrieveNMReleaseInformation"] # , "retrieveUserInformation"]  
    ogeninfoservs =  onmaccess.get_asetofservice("GeneralinformationServices")  ## or odefaultservs
    
    # new pythonic form
    for sareq in linforeq :
        try:
            dresult = ogeninfoservs('NMB2BInfoService',sareq,**dnawalnyparam(suserdef))
            print(pprint.pformat(dresult))
        except:
            print_exc()
            sys.exit(1)            
    print("The end for selftest00.")
    sys.exit(0)        
    lavailablewsdl = ["GeneralinformationServices", "CommonServices", "AirspaceServices",  
                       "FlightServices", "FlowServices", "FficeServices"]  
    # "FficeServices" pose un problème lié à zeep (déjà vu) zeep/xsd/types/collection.py 
    # un patch doit être fait sur collection.py done 
    ntotalreq = 0 
    for sawsdl in lavailablewsdl:
        try:
            omainserv = onmaccess.get_asetofservice(sawsdl)
        except:
            serror = "cannot access %s main service " % sawsdl
            ologger.debug(serror)
            print_exc()
        else:
            print("---- %s ----" % sawsdl)
            ntotalreq += omainserv.doc()  
        # ogeninfoservs.doc() 
    print( "-" * 10 )
    print("A total of {:d} Soap Request documented. ".format(ntotalreq))
    sys.exit(0)