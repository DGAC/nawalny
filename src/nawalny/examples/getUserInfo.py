#!/usr/bin/env python3 
# -*-mode: Python; coding: utf-8; -*-
'''getUserInfo.py: use GeneralInformationServices::retrieveUserInformation to obtain 
user information related to the cert defined by --conf option: 
basic information, profil/BUC exports info in raw mode or csv, xlsx  '''

import datetime,getopt,logging,os,pprint,requests,ssl,sys,time,zeep
from termcolor import colored 
from nawalny import NawalnyAccess,NawalnyRRService,SSLAdapter,snawalnytoday,snawalnytodaytime,dnawalnyparam 
from xlsxwriter.workbook import Workbook

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
    
def usage(arg0):
    fmt = '''Usage: {:s} [--help] [--debug] --conf=<file> [--fmt=(csv,xlsx)]
by default userinformation are extracted in raw text ;
fmt option allowes to obtain these info in a csv or xlsx file '''
    print( fmt.format(arg0))

def mygetopt(cmd,largs):
    ''' process argument and if success, return	'''
    lpathitem = sys.argv[0].split('/')
    dopt = {}
    sacmd = lpathitem[-1]
    bdebug = False
    sconffn = None     
    sfmt = None 
    stlist, laddfield = "", [] 
    try:
        optlist, lrargs = getopt.getopt(
            largs,'', ['help','debug','conf=','fmt='])
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
    if '--conf' in dopt :
        sconffn = dopt['--conf']
    else:
        print("--conf option is compulsory ")
        usage(sacmd)
        sys.exit(1)
    if '--fmt' in dopt :
        sfmt = dopt['--fmt']
    else:
        if not sfmt not in ('csv','xlsx') : 
            print("--fmt limited to csv or xlsx")
            usage(sacmd)
            sys.exit(1)
    if bdebug :
        print( dopt, len(lrargs))
    return sacmd,bdebug,sconffn,sfmt

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
def csv_thing(sthing):
    ''' voir si besoin de tester le type numérique sans cote, autre chose " " '''
    return '"%s";' % sthing 
def file_open2write(sfile_path):
    ''' open a file to write the result '''
    try:
        oofile = open(sfile_path, 'w' )
    except:
        print_exc()
        print("cannot open %s to write " %  sfile_path)
        sys.exit(1)
    else:
        return oofile
class NmUserProfil : 
    def __init__(self):
        self.lcolhead = []
        self.lline = []
    def __sui2list(self, suiline):
        ''' convertit 
        | RESOURCE_TYPE      | RESOURCE_URI  | VERSION | QUERY_PARAMETER | SCOPE     |
    en 
        ["RESOURCE_TYPE","RESOURCE_URI","VERSION","QUERY_PARAMETER","SCOPE"]  
        retourne None si aucun champ '''
        linfo = suiline.split('|')
        if len(linfo) > 0 : 
            return list(map(str.strip,linfo[1:-1]))
        else:
            return None 
    def parse(self,stextreport):
        '''parse textreport profil, populate and enriched object''' 
        lline = stextreport.split('\n') 
        il = 0 
        for saline in lline:
            if il in [0,1,3] :
                pass
            elif il == 2:
                self.lcolhead = self.__sui2list(saline)
            else: #  il == 2: ## column header 
                lfield = self.__sui2list(saline)
                if lfield :
                    self.lline.append(lfield)
            il += 1 
    def __repr__(self):
        return ( pprint.pformat(self.lcolhead) + '\n' + pprint.pformat(self.lline))
    def as_csv(self,ofout):
        ofout.write("".join(map(csv_thing,self.lcolhead)) + '\n')
        for laline in self.lline :
            ofout.write("".join(map(csv_thing,laline)) + '\n')
    def as_xlsx(self,oworkbook):
        owsheet = oworkbook.add_worksheet("profile")
        irow = 0 
        icol = 0 
        for sacel in self.lcolhead :
            owsheet.write(irow,icol,sacel)
            icol += 1  
        irow += 1 
        for laline in self.lline :
            icol = 0 
            for sacel in laline :
                owsheet.write(irow,icol,sacel)
                icol += 1 
            irow +=1 

if __name__ == '__main__':
    tnow = datetime.datetime.now()

    sacmd,bdebug,sconffilename,sfmt= mygetopt(sys.argv[0],sys.argv[1:])
    ologger.debug("%s %s %s %s",sacmd,bdebug,sconffilename,sfmt)
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
    if 'nmcertname' not in dmconf :
        print("conf %s shoudl define an nmcertname " % sconffilename )
        sys.exit(1)

    ologger.info("dmconf is %s", pprint.pformat(dmconf))
    # print(type(datewsdlservice))        
    suserdef = "getUserInfo"
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
    onmuserprofil = NmUserProfil()

    try: 
        dresult = ogeninfoservice('NMB2BInfoService',"retrieveUserInformation",
            **dnawalnyparam(suserdef))
        # print(pprint.pformat(dresult))
    except:
        print("retrieveUserInformation triggers an error ")
        print_exc()
        sys.exit(1)
    print(pprint.pformat(dresult))

    if sfmt : 
        try:
            stextreport = dresult['data']['textReport']
        except:
            print("retrieveUserInformation has no textReport")
            sys.exit(1)
        onmuserprofil.parse(stextreport)
        # print(onmuserprofil)
        # forging a filename to write  
        scurdir = os.getcwd()
        sday = tnow.strftime("%Y%m%d")
        spathout = scurdir + '/' + dmconf['nmcertname'] + "_profile_" + sday + "." + sfmt 
        # print(spathout)
        if os.access(spathout,os.F_OK) : 
            ologger.warning("file %s will be overwritten" % spathout)
        # ofileout: a file or a workbook 
        if sfmt == "csv" :
            ofileout = file_open2write(spathout)
            ologger.info("writing %s csv file " % spathout)
            onmuserprofil.as_csv(ofileout)
        elif sfmt == "xlsx":
            ofileout = Workbook(spathout) 
            ologger.info("writing %s xlsx file " % spathout)
            onmuserprofil.as_xlsx(ofileout)            
        ofileout.close()
        # print(tnow.strftime("%Y-%m-%d %H:%M:%S"))
    print("the real end.")
    sys.exit(0)