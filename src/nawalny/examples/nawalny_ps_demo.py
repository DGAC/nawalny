#!/usr/bin/env python3 
# -*-mode: Python; coding: utf-8; -*-
'''nawalny_ps_demo.py: demonstrator inspired by howto40_nm27.py 
but adapted to nawalny framework nawalny_rr.py nawalny_ps.py  

Pour créer une souscription, on passe par un fichier de data python/json 
Des exemples de souscription sont fournis: <nom>.sub27 

En NM27, l'attribut ['subscription']['topic'] est déterminant puisqu'il 
conditionne la requète de create,update,retrieve et la famille d'interface utilisé. 
Exemple :
si   ['subscription']['topic'] vaut 'EAUP'  on ira chercher la méthode 
createEAUPSubscription du service AirspaceServices::AirspaceStructureService

IMPORTANT: 
sur des subscriptions existantes en release "nm26" on ne peut faire que :
"delete","list"
"resume","pause","listen" n'agissent que sur des subscriptions "nm27"
'''
# this version uses logging  # CRITICAL,ERROR,WARNING,INFO,DEBUG,
# important to set global level before setting ologger object 
import logging
logging.basicConfig(level=logging.ERROR)  ## increase to debug other lib 
# logging.basicConfig(filename="ht40_log.tmp", level=logging.WARNING)  ## increase to debug other lib 
ologger = logging.getLogger("nawalny")
ologger.setLevel(logging.DEBUG) ## increase to debug howto40 aspect

import datetime,getopt,gzip,os,pprint,ssl,sys,time

# from howto40_nm27_r01_lib import NmPublishsubscribeServices, NmAmqpAgent
from traceback import print_exc

from nawalny_rr import NawalnyAccess,NawalnyRRService,dnawalnyparam,snawalnytodaytime
from nawalny_ps import NawalnyPSService, NAHandler  

lcmd1 =  ["create","update"]  # option with --subconf 
lcmd2 =  ["retrieve","resume","pause","delete","pull"] # option with --uuid 
lcmd3 =  ["list","check"] 
lcmd4 =  ["listen"]
# all possible options 
lcmd  = lcmd1 + lcmd2 + lcmd3 + lcmd4 
sapp = "Nawalnytestbydpa"

def usage(arg0):
    fmt = '''Usage: {:s} [--help] [--debug] --nmconf=<file> --cmd=<cmd> [--subconf=<file> | --uuid=uuid]
 cmd among {:s} 
 {:s} requires a subconf definition 
 {:s} require the uuid of the subscription 
 {:s} have no arg 
 {:s} with no arg listen all , with uuid arg listen only this subscription
 cmd=list is the default and list all subscriptions  '''
    print( fmt.format(arg0,",".join(lcmd),
                      ",".join(lcmd1) ,
                      ",".join(lcmd2) ,
                      ",".join(lcmd3) ,
                      ",".join(lcmd4)) )

def mygetopt(cmd,largs):
    ''' process argument and if success, return	'''
    lpathitem = sys.argv[0].split('/')
    dopt = {}
    sacmd = lpathitem[-1]
    bdebug = 0 
    snmconf = None 
    ssubconffn = None # par defaut "nm10.cfg" 
    scmd = "list"     # par defaut
    suuid = None
    stlist, laddfield = "", [] 
    try:
        optlist, lrargs = getopt.getopt(
            largs,'', ['help','debug','nmconf=','subconf=','cmd=','uuid='])
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
    if '--nmconf' in dopt :
        snmconf = dopt['--nmconf']
    else:
        print("--nmconf option is compulsory ")
        usage(sacmd)
        sys.exit(1)        
    if '--subconf' in dopt :
        ssubconffn = dopt['--subconf']
    if '--uuid' in dopt:
        suuid = dopt['--uuid']
    if '--cmd' in dopt :
        scmd = dopt['--cmd']
    # print(scmd,lcmd)
    if scmd not in lcmd :
        print("{:s} arg is not among {:s}".format(scmd, ",".join(lcmd)))
        usage(sacmd)
        sys.exit(1)
    
    if bdebug :
        print( dopt, len(lrargs))
#    if scmd in ["create","retrieve"] and  False  :
#        print("command [%s] is not implemented in this early version. exit. " % scmd)
#        sys.exit(1)
    return sacmd,scmd,bdebug,snmconf,ssubconffn,suuid  

def neatconflines(ofile): 
    lrealine = [] 
    while 1 :
        sali = ofile.readline()
        if sali == "" : break
        elif sali[0] == "#" : pass # discard comment 
        else:
            lrealine.append(sali)
    return "".join(lrealine)

class MyHandler(NAHandler):
    ''' the client class doing something useful with the message '''
    def on_message(self, event):
        squeuename = event.link.name
        if  event.message.content_encoding == 'gzip' :
            try:
                smsg = gzip.decompress(event.message.body)
            except:
                smsg = "***error decompressing message ***"
        else:
            smsg = event.message.body 
        print("_myhandler_")
        print("[%s]: %s" % (squeuename, smsg ))
if __name__ == '__main__':
    sacmd,scmd,bdebug,snmconf,ssubconffilename,suuid = mygetopt(sys.argv[0],sys.argv[1:])
    ologger.debug("%s %s %s %s %s %s" % (sacmd,scmd,bdebug,snmconf,ssubconffilename,suuid ))
    # semantic ctrl
    if scmd == "create" or scmd == "update" :
        if ssubconffilename :
            try:
                osubconffile = open(ssubconffilename)
            except:
                print("cannot read subscription file %s " % ssubconffilename)
                usage(sacmd)
                sys.exit(1)
            llsubconf = neatconflines(osubconffile)
            dsubconf = None
            try:
                dsubconf = eval(llsubconf)
            except:
                print("error in subscription definition %s " % lssubconffilename)
                print_exc()
                sys.exit(1)
            if 'subscription' not in dsubconf :
                print("%s file does not define a 'subscription' keyword " % ssubconffilename)
                sys.exit(1)
            if bdebug :
                print("subscription definition is ")
                print(pprint.pformat(dsubconf))
        else:  # ssubconffilename is none 
            print('''you shoud define a subscription definition file using --subconf parameter
an example in nm10.cfg ''')
            usage(sacmd)
            sys.exit(1)
    elif scmd in ("list","listen","check"): 
        pass
    else :
        if not suuid : 
            print("you shoud refer to an existing subscription using --uuid parameter")
            usage(sacmd)
            sys.exit(1)
    # all check are "ok", we go on 
    try:
        oconffile = open(snmconf)
    except:
        print("cannot conf file %s " % snmconf)
        usage(sacmd)
        sys.exit(1)
    try: 
        llconf = neatconflines(oconffile)
        dmconf = eval(llconf)
    except:
        print("error in configuration %s " % snmconf)
        print_exc()
        sys.exit(1)
    ologger.info("dmconf is %s", pprint.pformat(dmconf))
    
    # creation d'un accesseur 
    try:
       onmaccess = NawalnyAccess(
           dmconf['swsdlpath'], sapp, dmconf['nmapiindex'], dmconf['nmcontext'])
    except:
        print("error when building nmaccess")
        print_exc()
        sys.exit(1)
    try:
        onmaccess.set_credential(dmconf['nmcertpath'],dmconf['nmkeypath'],dmconf['nmpassword'])
    except:
        print("error validating creds ")
        print_exc()
        sys.exit(1)
    try:
        onmpsservice = NawalnyPSService(onmaccess,sapp)
    except:
        print("error when building onmpsservice")
        print_exc()
        sys.exit(1)
    dres = None 
    if scmd == "create" :
        dres = onmpsservice.create(**dsubconf['subscription'])
    elif scmd == "update" :
        dres = onmpsservice.update(**dsubconf['subscription'])
    elif scmd == "list" :
        dres = onmpsservice.llist()
    elif scmd == "check" :
        dres = None 
        onmpsservice.t_check()
    elif scmd == "retrieve" :
        dres = onmpsservice.retrieve(suuid)
    elif scmd == "delete" :
        dres = onmpsservice.delete(suuid)
    elif scmd == "pause" :
        dres = onmpsservice.pause(suuid)
    elif scmd == "resume" :
        dres = onmpsservice.resume(suuid)
    elif scmd == "pull" :     # NEW #
        sstatus,sqname,stopic,srelease = onmpsservice.getstatus(suuid)
        # print(sstatus,sqname)
        # if sstatus != 'DELETED' : 
        if sstatus == 'ACTIVE' : 
            dres = onmpsservice.pull(sqname,srelease)
        else:
            print("queue %s is in %s state" % (sqname,sstatus) )
            print("  need to resume the subscription to pull the message")        
    elif scmd == "listen" : 
        dres = onmpsservice.llist()
        if dres :
            try:
                dcurrentqueue = dres['data']['subscriptions']['item'] 
            except:
                print("Ther is no existing subscrition to listen to. <stop>")
                sys.exit(0)
            if suuid : # a specific uuid is selected, we restrict the list to it
                lqueuetarget = []
                for daqueue in  dcurrentqueue :
                    try :
                        if daqueue['uuid'] == suuid :
                            lqueuetarget.append(daqueue)
                    except:
                        pass
            else:      # we get all the queue / by default 
                lqueuetarget = dcurrentqueue

            if len(lqueuetarget) >= 1 : 
                lactivequeue = onmpsservice.resumeall(lqueuetarget) 
                # all valid queue (>=NM27) )are now supposed ACTIVE
                ologger.info("Listening with %s %s ", sapp,lactivequeue)
                # new Nawalny things 
                omyhandler = MyHandler(onmpsservice,sapp,lactivequeue)
                onmpsservice.amqpregister(omyhandler)
                ## being simplistic here 
                try:
                    onmpsservice.amqprun()
                except KeyboardInterrupt:
                    print("\nyou press  '<ctrl>-c' , so exiting.")
                    sys.exit(1)
                except:
                    print_exc()
                    print("run in error")
                    sys.exit(0)
            else:
                print("NO queue/subscription is selected to resume. <stop>")
                sys.exit(0)
        else:
            print("list of subscription is incorrest. Check first with list. <stop>")
            sys.exit(0)
            
    else :
        print("[%s] command is not implemented ! " % scmd)
        sys.exit(0)
         
    print(pprint.pformat(dres))

    
    print("the end.")
    sys.exit(0)