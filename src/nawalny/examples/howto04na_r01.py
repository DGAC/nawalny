#!/usr/bin/env python3 
# -*-mode: Python; coding: utf-8; -*-
'''howto04na_r01.py : exploite le service GeneralInformationServices pour récupérer 
les derniers éléments techniques du NM: wsdl, xsd, versions, url disponibles 
version + solide en cas de perte de connexion 
cette version utilise nawalny '''

import datetime,getopt,logging,os,pprint,requests,ssl,sys,time,zeep
from termcolor import colored 
from nawalny import NawalnyAccess,NawalnyRRService,SSLAdapter,snawalnytoday,snawalnytodaytime,dnawalnyparam 

from traceback import print_exc
lpathitem = sys.argv[0].split('/')
scmd = lpathitem[-1]

# this version uses logging 
# CRITICAL,ERROR,WARNING,INFO,DEBUG,
logging.basicConfig(level=logging.INFO )
ologger = logging.getLogger(scmd)

print('%s with %s level of logging' % (scmd,"ERROR") )


def date2nms(adatetime) :
    return( adatetime.strftime("%Y-%m-%d %H:%M") )
def nms2date(snmdate):
    '''2022-08-04 09:46:00'''
    return datetime.datetime.strptime(snmdate,"%Y-%m-%d %H:%M:%S") 

class AbsPostProcess:
    '''differed processing abstrac class '''
    def __init__(self,name=None):
        # pointing on 2 global objects 
        self.onmaccess = onmaccess 
        self.sdownloaddir = sdirname
        self.name = name 
        self.load = None
    def set_name(self,name):
        self.name = name 
    def set_load(self,load):
        self.load = load
    def process(self):
        return None
    def ldownload(self,surl,slocalpath):
        ''' donwload a file available at this url to this local path using nawalny feature'''
        try: 
            self.onmaccess.download(surl,slocalpath)
        except:
            print("exception on %s %s" % (surl,slocalpath) )
            print_exc()
    def prep_n_download(self,sressname):
        self.sressname = sressname 
        try:
            ssubpath = self.load['data']['file']['id']
        except:
            logging.error("AbsPostProcess: no %s file provided in data %s" % \
                          (self.sressname, str(self.load)))
            return
        snmurlfile =  onmaccess.service2urlfile()
        # something as  "https://www.b2b.{:s}nm.eurocontrol.int/FILE_{:s}/gateway/spec/" 
        
        surl = snmurlfile + ssubpath
        lsubpath = ssubpath.split('/')
        sfnname = lsubpath[-1]

        # sdirname is the local dir where file are downloaded         
        spathname = self.sdownloaddir + sfnname  
        print("download %s to %s" % (surl, spathname))
        self.ldownload(surl, spathname)
            
class ppNMB2BWSDLs(AbsPostProcess):
    '''check time stamp of wsdl delivery, raise an  alert if older than the reference
download the wsdl file et put it in directory pointed by '''
    def process(self):
        if self.load :
            try:
                sreleasetime = self.load['data']['file']['releaseTime']
            except:
                logging.error("ppNMB2BWSDLs: no release time provided in data %s" % \
                              str(self.load))
                return
        try:
            datereleasetime = nms2date(sreleasetime)
        except:
            logging.error("ppNMB2BWSDLs: release time is not in a date format %s" % \
                              str(self.load))
            return
        if datereleasetime > datewsdlservice :
            # not a failure but important warning 
            print(failure("ppNMB2BWSDLs: the wsdl available by download is older [%s] than the one locally in charge  [%s]" % \
            (str(datereleasetime) , str(datewsdlservice))))
        try:
            swsdlsubpath = self.load['data']['file']['id']
        except:
            logging.error("ppNMB2BWSDLs: no wsdl file provided in data %s" % \
                          str(self.load))
            return
        self.prep_n_download("wsdls")
        return

class ppNMB2BReferenceManuals(AbsPostProcess):
    '''download the refmanuals file et put it in directory pointed by '''
    def process(self):
        self.prep_n_download("manuals")
        return
class ppNMB2BScenarios(AbsPostProcess):
    '''download the scenarios file et put it in directory pointed by '''
    def process(self):
        self.prep_n_download("scenarios")
        return
class ppNMB2BAddenda(AbsPostProcess):
    '''download the addenda file et put it in directory pointed by '''
    def process(self):
        self.prep_n_download("addenda")
        return
    
def usage(arg0):
    fmt = '''Usage: {:s} [--help] [--debug] [--nodownload] --dir=<dir> --conf=<file> 
dir should be a directory with write auth , if it does not exist we attempt to create it 
conf file defines where to find NMB2B definition, certificates stuff 
--nodownload allows to extract list of files ONLY and stop before downloading file (testing purposes)'''
    print( fmt.format(arg0))

def mygetopt(cmd,largs):
    ''' process argument and if success, return	'''
    lpathitem = sys.argv[0].split('/')
    dopt = {}
    sacmd = lpathitem[-1]
    bdebug,bnodownload = False, False
    sconffn = None     
    sdir = "" 
    stlist, laddfield = "", [] 
    try:
        optlist, lrargs = getopt.getopt(
            largs,'', ['help','debug','nodownload','conf=','dir='])
    except :
        print_exc()
        print("a wrong parameters has been found")
        usage(sacmd)
        sys.exit(1)
    for k,v in optlist :
        dopt[k] = v
    if '--help' in dopt :
        usage(sacmd)
        sys.exit(1)
    if '--debug' in dopt:
        bdebug = True
    if '--nodownload' in dopt:
        bnodownload = True
    if '--conf' in dopt :
        sconffn = dopt['--conf']
    else:
        print("--conf option is compulsory ")
        usage(sacmd)
        sys.exit(1)
    if '--dir' in dopt :
        sdir = dopt['--dir']
    else:
        if not bnodownload : 
            print("--dir option is compulsory to download file ")
            usage(sacmd)
            sys.exit(1)
    if bdebug :
        print( dopt, len(lrargs))
    return sacmd,bdebug,bnodownload,sconffn,sdir

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
    sacmd,bdebug,bnodownload,sconffilename,sdirname = mygetopt(sys.argv[0],sys.argv[1:])
    ologger.debug("%s %s %s %s %s",sacmd,bdebug,bnodownload,sconffilename,sdirname)
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

    if not bnodownload : # we do check download dir 
        # test si repertoire existe et est writable sinon on tente de le créer et sinon on stop
        if os.access(sdirname,os.F_OK) :
            if os.access(sdirname,os.W_OK) :
                print("[%s] directory is writable" % sdirname )
            else:
                print("[%s] directory exists BUT is not writable. exit. " % sdirname )
                sys.exit(1)
        else:  # it does not exist, we attemps to create it
            try:
                os.mkdir(sdirname, mode=0o755)
            except FileExistsError :
                print("[%s] directory exists yet . exit." % sdirname )
                sys.exit(1)
            except FileNotFoundError :
                print("[%s] upstream path does not exist yet . exit." % sdirname )
                sys.exit(1)
            except:
                print("[%s] cannot be created . exit." % sdirname )
                sys.exit(1)
            else:
                print("[%s] has been created " % sdirname)
        if sdirname[-1] != '/' :
            sdirname += '/'
    # we test swsdlpath and capture timestamp 
    if 'swsdlpath'  in dmconf : 
        swsdlservice =  dmconf['swsdlpath']
        if not os.access(swsdlservice, os.R_OK) : 
            logging.error("wsdlfile at %s should be readable , not the case. exit." % swsdlservice )
            sys.exit(1)
        timewsdlservice = os.path.getmtime(swsdlservice)
        datewsdlservice = datetime.datetime.fromtimestamp(timewsdlservice)
    else:
        print(" swsdlpath should be defined in dmconf ")
        sys.exit(1)
    # print(type(datewsdlservice))        
    suserdef = "howto04na"
    dparam_default = dnawalnyparam(suserdef) ## we shall copy and complete by required info 
    # creation d'un accesseur 
    try:
       onmaccess = NawalnyAccess(
           dmconf['swsdlpath'], suserdef, dmconf['nmapiindex'], dmconf['nmcontext'])
    except:
        print("error when building nmaccess")
        print_exc()
        sys.exit(1)
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
    # performing a std test 
    lret = onmaccess.test_std()        
    if lret[0] : 
        print(success("Soap Acces to nmb2b is valid"))
    else:
        print(failure("Soap Acces to nmb2b is INVALID"))
        sys.exit(1)       
    ogeninfoservice = onmaccess.get_asetofservice("GeneralinformationServices") 
    # ologger.info(onmaccess.drrservices)
    ologger.info(ogeninfoservice)
    ologger.info(dmconf)
    try:
        snmversion = dmconf['nmapiindex']
    except:
        print("nmapiindex key should be defined in conf dict parameters ")
        sys.exit(1)
        
    lqueryset = [
        ['retrieveNMReleaseInformation', None , None ] ,
        ['queryNMB2BReferenceManuals', { 'version': snmversion } , ppNMB2BReferenceManuals() ] ,
        ['queryNMB2BWSDLs', { 'version': snmversion } , ppNMB2BWSDLs() ] , 
        ['queryNMB2BScenarios', { 'version': snmversion }, ppNMB2BScenarios()  ], 
        ['queryNMB2BAddendaErrata', { 'version': snmversion }, ppNMB2BAddenda()  ] ,
    ]
    print(dparam_default)
    lpostproccess = []

    for (squery, daddpar, opostp) in lqueryset:   # lqueryset[2:3] :
        dparam = dparam_default.copy() # level 1 copy
        if daddpar : # we add specific option 
            for (k,v) in daddpar.items():
                dparam[k] = v             
        try: 
            dresult = ogeninfoservice('NMB2BInfoService', squery, **dparam) 
        except:
            logging.error("*** request %s triggers an error " % squery)    
            print_exc()
            sys.exit(1)            
        else:
            print("[%s]" % squery)
            ologger.debug(pprint.pformat(dresult))
            if opostp :
                    opostp.set_name(squery)
                    opostp.set_load(dresult)
                    lpostproccess.append(opostp)
    # endfor 
    # sys.exit(0)
    if bnodownload : # we print only what would be processed and quit 
        print("Here is what we would do but we stop here (nodownload selected)")
        for oaproc in lpostproccess :
            print(oaproc.name,oaproc.load)
        # print(pprint.pformat(lpostproccess))
        sys.exit(0)
    # else we do the real job 
    for opostp in lpostproccess:
        print(opostp.name)
        opostp.process()
    print("the real end.")
    sys.exit(0)