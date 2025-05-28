# -*-mode: Python; coding: utf-8; -*-
'''nawalny_rr.py : Nawalny 
main library giving an access to Eurocontrol/NMB2B services see:
nawalny_rr addresses Request Reply in Soap/Xml protocol and uses Zeep inner library. 
https://docs.python-zeep.org/en/master/ 
A second library extends the service
to Publish / Subscribes feature based on Amqp 1.0 protocol  '''

''' Nawalny
    Copyright (C) 2025  PAVET Didier DSNA/DO

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
    USA
    Didier PAVET for this matter can be touched preferably at dpa4aviation@gmail.com '''

import copy,datetime,getopt,logging,os,pprint,re,requests,socket,ssl,sys,time
from zeep.cache import SqliteCache
from zeep import Client,Settings
from zeep.transports import Transport
# from zeep.utils import get_base_class 

from traceback import print_exc
nawalnyv = "0.0.3" # version 
ologger = logging.getLogger("nawalny")

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.poolmanager import PoolManager

## facilities functions forging parameters to poll NMB2B poll Soap/Xml api
suseriddefault = "nawalny"
def dnawalnyparam(suser: str=None) -> dict :
    if suser :
        sretuser = suser 
    else:
        sretuser = suseriddefault  
    return {'endUserId':sretuser,'sendTime': snawalnytodaytime()}
def snawalnytodaytime() -> str :
    return  datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S") 
def snawalnytoday() -> str :
    return  datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
def snawalnydate(adatetime: datetime.datetime) -> str : 
    ''' convert a datime in the string awaited by NM '''
    return( adatetime.strftime("%Y-%m-%d %H:%M") )

def dnawalnywindow(dstart:datetime.datetime, dend:datetime.datetime) -> dict:
    ''' forge a dict as awaited by NM as a traffic window '''
    return {'wef': snawalnydate(dstart), 'unt':snawalnydate(dend) }

#  -- service methods -- derived from nmb2b construction principles 
ORE_SERVICE = re.compile('(.*)Service$' ) 
def forgeSubscription(stopic: str,smode: str) -> str :
    return "{:s}{:s}Subscription".format(smode,stopic)   
def simplifyService(slongname: str) -> str :
    ''' de   AirspaceStructureService  renvoit AirspaceStructure '''
    ore_service_m =  ORE_SERVICE.match(slongname) 
    if ore_service_m :
        return ore_service_m.group(1)
    else:
        print_exc()
        serror = "Nawalny::simplifyService error in {:s}".format(slongname) 
        ologger.error(serror)
        raise Exception(serror)
def forgePort(sname: str) -> str :
    ''' de AirspaceStructureService renvoit AirspaceStructurePort '''
    return "{:s}Port".format(simplifyService(sname))
def is_resolved(shostname:str) -> bool:
    try:
        socket.gethostbyname(shostname)
    except socket.error:
        print_exc()
        return False
    else:
        return True
# ORE_URL = re.compile('^http.?:\/\/(.*)') 
ORE_URL = re.compile('^http.?://(.*)') 
def url2host(surl:str) -> str: 
    ''' de https://www.b2b.preops.nm.eurocontrol.int/B2B_PREOPS/gateway/spec/27.0.0  renvoie
    www.b2b.preops.nm.eurocontrol.int  '''
    ore_url_m = ORE_URL.match(surl)
    if ore_url_m : 
        lurl = ore_url_m.group(1).split('/')
        return lurl[0]
    else:
        print_exc()
        serror = "Nawalny::url2host {:s} is not a valide http URL ".format(surl) 
        ologger.error(serror)
        raise Exception(serror)
# technical cyphering class managing client cert authentication and protection 
class SSLAdapter(HTTPAdapter):
    '''an adapter bringing ssl authentication;  '''
    def __init__(self, certfile: str, keyfile: str=None, password: str=None, *args, **kwargs):
        self._certfile = certfile
        self._keyfile = keyfile
        self._password = password
        self._context = self._create_ssl_context()
        try: 
            self._context.load_cert_chain(certfile=self._certfile, keyfile=self._keyfile,
                                         password=self._password)
        except ssl.SSLError as  err  :
            serror = "authen by certificate in error with %s" % err 
            ologger.error(serror)
            raise Exception(serror)            
        except:
            print_exc()
            sys.exit(1)
        # print(self._context.__dir__())
        # kwargs['ssl_context'] = self._context    
        super().__init__( *args, **kwargs)
    def _create_ssl_context(self):
        ctx = ssl.create_default_context()  # ssl.Purpose.CLIENT_AUTH)
        return ctx  
    def init_poolmanager(self, *args, **kwargs):
        ologger.info('Nawalny SSLAdapter.init_poolmanager')
        kwargs['ssl_context'] = self._context
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)
    def proxy_manager_for(self, *args, **kwargs):
        ologger.info('Nawalny SSLAdapter.proxy_manager_for')
        kwargs['ssl_context'] = self._context
        return super(SSLAdapter, self).proxy_manager_for(*args, **kwargs)
        
class NawalnyAccess :  
    ldefaultwsdl = ['GeneralinformationServices','NMB2BInfoService','NMB2BInfoPort']
    sdefaultreq  =  "retrieveNMReleaseInformation"
    snmurlpattern = "https://www.b2b.{:s}nm.eurocontrol.int/FILE_{:s}/gateway/spec/" 
    
    def  __init__(self,swsdlpath: str,suserid: str=None, sapiindex: str="27.0.0",snmcontext: str="PREOPS", 
                  brawmode: bool=False ) :
        ''' construit l'accesseur et un 1er set of service définit par ldefaultwsdl  
          definit attribut osession,ocache,otransport,ozsettings ''' 
        self.suserid = suserid
        if os.access(swsdlpath, os.R_OK) : 
            self.wsdlpath = swsdlpath      
        else :
            serror = "swsdlpath %s is not readable" % swsdlpath 
            ologger.error(serror)
            raise Exception(serror)
        self.sapiindex = sapiindex
        if snmcontext in ["PREOPS","OPS"] :
            self.snmcontext = snmcontext
        else:
            serror = "Nawalny context [%s] should be OPS or PREOPS " % snmcontext
            ologger.error(serror)
            raise Exception(serror)
        szeepcache = os.environ['HOME'] + "/zeep_cache.db" 
        try: 
            try: 
                self.ozcache = SqliteCache(path=szeepcache) 
            except:
                self.ozcache = None 
            self.osession = requests.Session()             
            self.otransport = Transport(session=self.osession, cache=self.ozcache)
            self.oadapter = None # défini par set_credential  
        except:
            print_exc()
            serror = "Nawalny erreur dans la création des fondamentaux de NawalnyAccess"
            ologger.error(serror)
            raise Exception(serror) 
        # IMPORTANT : raw_response : determine si Zeep renvoit du xml raw ou un objet Python
        # - defaut:False : zeep renvoit un objet matchant une classe declare par wsdl/xsd
        #    exemple :  la requete queryFlightsByAerodrome  ->  zeep.objects.FlightListByAerodromeReply
        # - True: zeep renvoit un objet générique requests.models.Response         
        if brawmode :
            self.ozsettings = Settings(strict=False, xml_huge_tree=True, raw_response=True )
        else:
            self.ozsettings = Settings(strict=False, xml_huge_tree=True)  
        
        self.drrservices = {} # dictionnaire des services "GeneralinformationServices" : NawalnyRRService()
        
        # DRY solution is ok 
        self.odefaultservs  = self.get_asetofservice(NawalnyAccess.ldefaultwsdl[0])
        
    def set_credential(self,scertpath: str,skeypath: str=None,spassphrase: str=None,oaltadapter: object=None) -> None:
        '''enregistre les ids de connexion = éléments du certificat accroché à un objet 
        self.odapter  de type HTTPadapter ou un objet alternatif (compatible HTTPadapter) 
        pour régler des problèmes de chiffrement 
        retourne un tuple  ( booleen, reponse_requete_std  ) '''
        nmax_retries = 0 
        if not os.access(scertpath, os.R_OK) : 
            serror = " %s is not readable" % scertpath
            ologger.error(serror)
            raise Exception(serror)
        self.scertpath = scertpath
        if skeypath and not os.access(skeypath, os.R_OK) : 
            serror = " %s is not readable" % skeypath
            ologger.error(serror)
            raise Exception(serror)         
        self.skeypath = skeypath
        self.spassphrase = spassphrase   
        if oaltadapter : 
            self.oadapter = oaltadapter 
        else:
            # construit l'adapter d'accès http standard 
            # print(scertpath,skeypath, spassphrase)
            self.oadapter = SSLAdapter(scertpath,skeypath, spassphrase, max_retries=nmax_retries)        
        # associe pour http/https cet adapteur à l'objet session 
        self.osession.mount('https://', self.oadapter)            
        self.osession.mount('http://', self.oadapter)
        
    def test_std(self) -> tuple :
        ''' interroge nmb2b sur la méthode std = retrieveNMReleaseInformation et retourne un tuple '''
        if self.oadapter : 
            try:
                oreq = self.odefaultservs.t_get_req(NawalnyAccess.ldefaultwsdl[1],NawalnyAccess.ldefaultwsdl[2],
                                                NawalnyAccess.sdefaultreq)
                dresult = oreq(**dnawalnyparam(self.suserid))
            except:
                serror = "an error occured polling standard request %s " % NawalnyAccess.sdefaultreq 
                print_exc()
                ologger.error(serror)
                raise Exception(serror)            
            else :
                bret = False
                try :
                    bret = dresult['status'] == "OK" 
                except :
                    print_exc()
                    bret = False 
                return bret , dresult 
        else:
            ologger.error("oadapter is not defined. set_credential should be called first")
            return False , None 
                
    def test_tech(self, saltbackend: str=None ) -> tuple :
        '''verifie la résolution de nom, interroge le backend et test l'authentification techniquement
        peut être utile pour vérifier problème de proxy Internet ou dns  
        on fait une hypthèse technique coté backend (_discutable_)
        Hypothèse: nmb2b répond <400> : l'autentification est "success"
                                <autre chose> : authentification en erreur (exploiter le content) 
        retourne boolean, code http, content 
        on peut forcer une URL alternative en http?://xxxx avec saltbackend ''' 
        
        if saltbackend : 
            sstdurl = saltbackend
        else: 
            sstdurl = self.get_backend()
        ologger.info("getting stdurl [%s] to check technical connection" % sstdurl)
        if not is_resolved(url2host(sstdurl)) : 
            ologger.error("%s hostname is not resolved in your configuration - DNS problem! " % sstdurl )
            return False, None
        else:
            ologger.info("Name resolution is ok")             
        if self.oadapter : 
            try:
                oans1 = self.osession.get(sstdurl) # ,cert=scertpath)
            except:
                serror = "an error occured in connection "
                ologger.error(serror)
                print_exc()
                raise Exception(serror) 
            else:
                # analyser la réponse : si [400] ok,   si autre chose alors erreur 
                # ologger.info("connection is ok; check credential ")
                # print("ans1", oans1, type(oans1), oans1.content)
                bret = oans1.status_code == 400 
                return bret, oans1.status_code, oans1.content     
        else:
            ologger.error("oadapter is not defined. set_credential_n_test should be called first")
            return False, None                     

    def get_asetofservice(self,sservicesname: str, breadagain: bool=False) -> object:
        ''' return an existing NawalnyRRService or create a new one parsing wsdl definition 
breadagain allowes to force the parsing of wsdl '''
        if breadagain :
            ologger.warning("NawalnyAccess: force the parsing of %s wsdl" % sservicesname)
        if sservicesname in self.drrservices and not breadagain :
            return self.drrservices[sservicesname]
        else:
            try:
                onewrrservice = NawalnyRRService(self,sservicesname,self.snmcontext)
            except:
                print_exc()
                serror = "Nawalny erreur dans la création du set de services %s" % sservicesname
                ologger.error(serror)
                raise Exception(serror)
            else:
                self.drrservices[sservicesname] = onewrrservice
                return onewrrservice           
    def service2wsdl(self,sservice: str) -> str :
        return self.wsdlpath + "{:s}_{:s}_{:s}.wsdl".format(sservice,self.snmcontext,self.sapiindex) 
    def service2urlfile(self, sothercontext: str=None) -> str :
        '''highly dependant of NMB2B architecture ; 
build a back_end for downloading NMB2B file ressources : sothercontext could be :
ENVPREVAL ENVPREVAL_NEXT        '''
        if sothercontext : 
            snmurlfile = NawalnyAccess.format("", sothercontext)
        else :    
            if self.snmcontext == "PREOPS" :
                snmurlfile = NawalnyAccess.snmurlpattern.format("preops.", self.snmcontext)
            elif self.snmcontext == "OPS" :
                snmurlfile = NawalnyAccess.format("", self.snmcontext)
            else:
                serror = "Nawalny context [%s] should be OPS or PREOPS " %  self.snmcontext
                ologger.error(serror)
                raise Exception(serror)
        return snmurlfile     
    def get_backend(self) -> str :
        # print(pprint.pformat(self.oclient.wsdl.services))
        if self.odefaultservs:
            try:
                sbackend =  self.odefaultservs.oclient.wsdl.services[NawalnyAccess.ldefaultwsdl[1]].\
                    ports[NawalnyAccess.ldefaultwsdl[2]].binding_options['address'] 
            except: 
                serror = "cannot found a binding address in default client"
                ologger.error(serror)
                print_exc()    
                raise Exception(serror)
            return sbackend
        return None
       
# additional tech method     
    def download(self, surl:str, slocalfilepath:str, nchunksize:int=100*1024, npace:float=0.5)->None:
        '''use session and credential to download tech or operational file : 
smoothdownload by chunck defined by nchuncksize, pace defined by npace '''
        oreadstream = self.osession.get(surl,stream=True,allow_redirects=True)
        try:
            flocal = open(slocalfilepath, 'wb')
        except:
            serror = "%s local file cannot be write" %  slocalfilepath
            ologger.error(serror)
        else:
            for chunk in oreadstream.iter_content(chunk_size=nchunksize):
                flocal.write(chunk)
                time.sleep(npace)
            flocal.close()

class NawalnyRRService:
    def __init__(self, onawalnyaccess: NawalnyAccess, servicesname: str, nmcontext: str="PREOPS"):
        '''class to manage a set of request/reply services as defined by a wsdl definition 
method starting with t_ are technical , the other represents the normal 'api' to the user 
now a callable object: '''
        self.onawalnyaccess= onawalnyaccess     # access to NM 
        self.servicesn = servicesname
        self.nmcontext = nmcontext         
        self.snmwsdlpath = self.onawalnyaccess.service2wsdl(self.servicesn)
        if not os.access(self.snmwsdlpath, os.R_OK) : 
            serror = "nawalnyrrservice: wsdl service path %s is not readable" % self.snmwsdlpath
            ologger.error(serror)
            raise Exception(serror)
        self.oclient = Client(wsdl=self.snmwsdlpath,transport=self.onawalnyaccess.otransport,
                              settings=self.onawalnyaccess.ozsettings)
        if False :
            ologger.debug(self.oclient)
            ologger.debug("oclient.settings %s", self.oclient.settings)
            ologger.debug(self.oclient.wsdl.__dir__())
            ologger.debug(self.oclient.wsdl.services)
            ologger.debug("firstservice is " + str(self.oclient.service))
        # datastructure v2 to memcache zeep, and request object 
        # we keep the 'portlevel' though it is meaningless 
        # oclient.bind used to build a set of subservice  
        # self.dnmserv dict with this type of element  
        # 'asubservice: {'aport': { 'oport': zeep.proxy.ServiceProxy,
        #                        'dreq' : {} },
        # dreq: 'sreqname': orequest each requested will be populated by t_get_req on demand 
        self.dnmserv = {}  
        for (kaser,oaserv) in self.oclient.wsdl.services.items():
            # print(kaser,oaserv.name,len(oaserv.ports))
            self.dnmserv[oaserv.name] = {}
            for (kp,op) in oaserv.ports.items():
                self.dnmserv[oaserv.name][kp] = {'oport' : self.oclient.bind(oaserv.name,kp),
                                                 'dreq' : {},
                                                 'sbackendnative': None } # pour restaurer 

    def __repr__(self):
        return pprint.pformat(self.dnmserv)
    def doc(self) -> int :
        '''extrait et print pour tous les (service,port) le backend, les méthodes/requètes proposées et 
signature, retourne le nombre de requètes  '''
        slinesep = "-" * 10 
        nrrreq = 0  
        for (saserv,oaport) in self.dnmserv.items():
            for (saport, dazproxy) in oaport.items():
                print(slinesep)
                print("[%s, %s]" %  (saserv,saport) )
                print("backend: %s" % self.get_backend(saserv))
                for (sanope,oope) in  dazproxy['oport']._operations.items():
                    print(sanope, oope.__doc__,"\n")
                    nrrreq += 1 
        return nrrreq 
        
    def t_get_req(self,subservice: str,sport: str,srequestname: str) -> object :
        '''technical: if it is already cached return the method otherwise 
search through the binding the suited method and cached it in dnmserv data '''
        if srequestname in self.dnmserv[subservice][sport]['dreq'] :
            return  self.dnmserv[subservice][sport]['dreq'][srequestname]
        else:  
            try:
                oport =  self.dnmserv[subservice][sport]['oport'] 
            except:
                serror = "%s %s is not a port of %s" % ( subservice,sport,self.servicen ) 
                ologger.error(serror)
                raise Exception(serror)
            try:
                oreq = getattr(oport,srequestname)
            except:
                serror = "%s is not recognized as a function of %s %s %s " % (srequestname,self.servicen,subservice,sport) 
                ologger.error(serror)
                raise Exception(serror)
            else:     
                self.dnmserv[subservice][sport]['dreq'][srequestname] = oreq 
                return oreq
## attemp to be more pythonic / straightforward 
    def __call__(self,subservice: str,srequestname: str, **dargs) -> None :
        ''' call the method of the subservice, port is derived, with parameters 
thus:  this syntaxe is recommended on this exemple 
ogeninfoservs =  onmaccess.get_asetofservice("GeneralinformationServices") # a NawalnyRRService object
ogeninfoservs("NMB2BInfoService","retrieveNMReleaseInformation",dparam)   '''
        return self.t_get_req(subservice,forgePort(subservice),srequestname)(**dargs) 

    def get_backend(self,subservice: str) -> str :
        '''retourne le backend associé précisément au (service,port) '''
        sport = forgePort(subservice) 
        try:
            sbackend =  self.oclient.wsdl.services[subservice].ports[sport].binding_options['address'] 
        except: 
            serror = "cannot get backend address for [%s,%s]" % (subservice,sport)
            ologger.debug(serror)
            print_exc()    
            raise Exception(serror)
        else:  
            return sbackend 
    def set_backend(self,subservice: str,surl: str ) -> None :
        '''permet de changer le backend ; cela peut être utile pour accèder à des services spéciaux :
NMVP , ENVPREVAL, ENVPREVAL_NEXT 
ENVPREVAL_NEXT : https://www.b2b.nm.eurocontrol.int:443/FILE_ENVPREVAL_NEXT/gateway/spec/ 

Attention: des droits spécifiques peuvent être requis 
sauvegarde du backendnative pour restauration ultérieure '''
        sport = forgePort(subservice) 
        try:
            sbackendnative =  self.oclient.wsdl.services[subservice].ports[sport].binding_options['address'] 
            self.dnmserv[subservice][sport]['sbackendnative'] = sbackendnative
            self.oclient.wsdl.services[subservice].ports[sport].binding_options['address'] = \
                surl 
        except: 
            serror = "cannot set backend address for [%s,%s]" % (subservice,sport)
            ologger.debug(serror)
            print_exc()    
            raise Exception(serror)
    def restore_backend(self,subservice: str,surl: str ) -> None :
        sport = forgePort(subservice) 
        sbackendnative = self.dnmserv[subservice][sport]['sbackendnative']
        if sbackendnative : 
            try:
                self.oclient.wsdl.services[subservice].ports[sport].binding_options['address'] = sbackendnative  
            except: 
                serror = "cannot restore backend address for [%s,%s]" % (subservice,sport)
                ologger.debug(serror)
                print_exc()    
                raise Exception(serror)        
        else:
            ologger.warning("backendnative is None. No restore")

'''    def sett(self,**narg):
        " propagate to client.settings "
        print("NmRRService::sett",narg)
        try:
            # self.oclient.settings(**narg)
            print("NmRRService::sett",self.oclient.settings)
            self.oclient.settings(raw_response=True)
        except:
            print_exc()
            print("error in NmRRService::sett, so ignore ")
    def sett2(self,**narg):
        self.oclient.settings(**narg) 
'''