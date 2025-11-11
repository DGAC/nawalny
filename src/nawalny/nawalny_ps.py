# -*-mode: Python; coding: utf-8; -*-
'''nawalny_ps.py : 
This module provides an access to Publish / Subscribes feature based on Amqp 1.0 protocol
using Apache qpid proton inner library, see :
https://qpid.apache.org/releases/qpid-proton-0.37.0/proton/python/docs/index.html
nawalny_ps.py relies on nawalny_rr.py since the api Publish/Subscribe is driven by 
the api request/reply, especially the set of services 
SubscriptionManagementService, MessagingService proposed by the CommonServices wsdl definition  '''

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
    
import copy,datetime,gzip,logging,os,pprint,re,sys,time

from .nawalny_rr import NawalnyAccess,NawalnyRRService,forgeSubscription,simplifyService,\
    forgePort,dnawalnyparam,snawalnytodaytime

# for p/s in amqp mode using qpid-proton 
from proton.reactor import Container
from proton import Message, SSLDomain, Connection
from proton.handlers import MessagingHandler

from traceback import print_exc
nawalnyv = "0.0.3" # version 
ologger = logging.getLogger("nawalny")
suserdef = "Nawalnytest"
## utilities to test subscription release 
NMSUBREF = 27 
def subscribtionrelease_as_num(srelease):
    '''convert release string in major number to check we are manipulating 
subscription above 27/NMSUBREF '''
    nrelease = None 
    try:
        lrelease = srelease.split('.')
        nrelease = int(lrelease[0])
    except:
        serror = "subrelease_as_number %s is not well formatted"             
        ologger.error(serror)
        raise Exception(serror)   
    return nrelease 
def is_validsub(srelease):
    return subscribtionrelease_as_num(srelease) >= NMSUBREF 

class NawalnyPSService :
    ''' DP Facade proposing the set of method steering the lifecycle of an Amqp subscription :
create,update,retrieve,resume,pause,delete,pull,list,
The service relies on a master object NawalnyAccess, delegating to this element the burden 
to manage R/R Soap/Xml interface . 
This class attemps to brings a unified access to generic SubscriptionManagementService, MessagingService
and  subscription management embodied in each specialized domain interface 

To be implemented: 'subscriptionHistory', 'synchroniseSubscription', 'abortSubscriptionSynchronisation'

The facade proposes additional method related to subscription: resumeall, getstatus, 

It proposes the mecanism to launch  amqp mecanisms : 
backendurl, amqpregister, amqprun  
amqpregister registers an NAHandler, abstract class supposed to be subclassed 
and customized by the caller 
amqprun is an infinite loop handling the receipt of amqp messages.  
Note: functional version of subscription ''' 
## EXPLIQUER CES VALEURS DE CLASSE : sujet: la cle ci-dessous correspond bien à la méthode idoine
#  FlightData => une methode createFlightDataSubscription existe dans FlightService****.wsdl
#  mais le topic associé est formatté comme suit 'FLIGHT_DATA' ; liste des valeurs dans
#  <xs:simpleType name="SubscriptionTopic">  de CommonServices_27***.xsd  

    dsubscription = {
        "AirspaceData" : ["AirspaceServices" ,"AirspaceStructureService"],
        "EAUP":  ["AirspaceServices" ,"AirspaceAvailabilityService"],
        "GNSSInterference": ["AirspaceServices" ,"AirspaceStructuresService"],
        "FficeFlightFiling": ["FficeServices", "FilingServiceService"],
        "FficePublication": ["FficeServices","PublicationServiceService"],
        "FlightData": ["FlightServices", "FlightManagementService"],
        "FlightFilingResult": ["FlightServices", "FlightFilingService"],
        "FlightPlan": ["FlightServices", "FlightManagementService"],
        "MCDM": ["FlowServices", "McdmService"],
        "Regulation": ["FlowServices", "MeasuresService"],
        "Rerouting": ["FlowServices", "MeasuresService"],
        "AIMS": [ "GeneralinformationServices", "AIMsService"],
        "Passengers": ["FlightServices", "PassengerDemandPredictionService"]}

    d26_27equiv = {
        "ATM_INFORMATION" : "AIMS" ,  # a vérifier 
        "AIRSPACE_DATA" : "AirspaceData" ,
        "REGULATIONS" : "Regulation" ,
        "REROUTINGS" : "Rerouting" ,
        "EAUP" : "EAUP" , 
        "GNSS_INTERFERENCE" : "GNSSInterference", 
        "FLIGHT_PLANS" : "FlightPlan" , 
        "FLIGHT_DATA" :  "FlightData" , 
        "FLIGHT_FILING_RESULT" : "FlightFilingResult" , 
        "FFICE_PUBLICATION" : "FficePublication" , 
        "FFICE_FLIGHT_FILING" : "FlightFilingResult" ,
        "MCDM" : "MCDM",
# apparait en NM28         
        "PASSENGERS": "Passengers" }
#        ore_service = re.compile('(.*)Service$' )
    samqpendpoint = "amqps://pubsub.{:s}nm.eurocontrol.int:5671"
        
    def __init__(self, onawalnyaccess: NawalnyAccess,suserid: str=None ) :
        self.suserid = suserid 
        # we delegate to this object all the responsability to manage Soap/Xml connection to NM 
        self.onawalnyaccess = onawalnyaccess 
        # direct accessor to these two set of services 
        self.ocommonserv  =   onawalnyaccess.get_asetofservice("CommonServices")
        # "SubscriptionManagement", "Messaging"
    def create(self,**param):
        if 'topic' in param :
            stopic = param['topic']
            if stopic not in NawalnyPSService.d26_27equiv :
                serror = "NawalnyPSService: topic: %s is not among the valid set of topic, see NMB2B doc." % stopic 
                ologger.error(serror)
                raise Exception(serror) 
                return None            
            stopicalias = NawalnyPSService.d26_27equiv[stopic]
            ## mise au point de param
            del param['topic']
            ologger.info(pprint.pformat(param))
            return self.callTopicMethod(stopicalias,"create",\
                                        endUserId=self.suserid,sendTime=snawalnytodaytime(),**param )
            # omethtopic = self.__getTopicMethod(stopicalias,"create")
            
            # try:
            #     dresult = omethtopic(endUserId=self.suserid,sendTime=stodaytime,**param )
            # except:
            #     print_exc()
            #     return None
            # return dresult
        else:
            serror = "Subscription defintion/parameter does not define a 'topic' " 
            ologger.error(serror)
            raise Exception(serror)            
    def retrieve(self,suuid):
        '''retrieve cannot request a subscription created before NMSUBREF = 27 '''
        sstatus,sqname,stopic,srelease = self.getstatus(suuid)
        if sstatus :
            if is_validsub(srelease) :
                stopicalias = NawalnyPSService.d26_27equiv[stopic]
                return self.callTopicMethod(stopicalias,"retrieve",\
                                            endUserId=self.suserid,sendTime=snawalnytodaytime(),uuid=suuid )
            else:
                print("subscription [{:s}] has been created with api release {:s}".format(suuid,srelease))
                print("it cannot be retrieve by this program")
                return None 
        else:
            print("no subscription has been found with uuid {:s}".format(suuid)) 
    def update(self,**param):
        '''update cannot be applied on a request a subscription created before NMSUBREF = 27 '''
        if 'subscriptionUuid' in param :
            suuid= param['subscriptionUuid']
            sstatus,sqname,stopic,srelease = self.getstatus(suuid)
            if is_validsub(srelease) :
                stopicalias = NawalnyPSService.d26_27equiv[stopic]
                return self.callTopicMethod(stopicalias,"update",\
                                            endUserId=self.suserid,sendTime=snawalnytodaytime(),**param )
                
            else:
                print("subscription [{:s}] has been created with api release {:s}".format(suuid,srelease))
                print("it cannot be updated by this program")
                return None 
        else:
            print("subconf does not defined a subscriptionUuid field")
            return None                
    def callTopicMethod(self,stopic: str,smode: str, **dargs):
        ''' directly call the "forge" method deduced from stopic,smode '''
        if smode in ("create","retrieve","update") :
            if stopic in NawalnyPSService.dsubscription :
                stopicwsdl , stopicservicelong =  NawalnyPSService.dsubscription[stopic]
                try:
                    osubserve = self.onawalnyaccess.get_asetofservice(stopicwsdl)
                except:
                    print_exc()
                    serror = "Nawalny::callTopicMethod error in accessing %s" % stopicwsdl
                    ologger.error(serror)
                    raise Exception(serror) 
                else:                    
                    smeth = forgeSubscription(stopic,smode)
                    return osubserve(stopicservicelong,smeth,**dargs)               
            else:
                serror = "Nawalny::callTopicMethod Topic [%s] is not valid " % stopic 
                ologger.error(serror)
                raise Exception(serror)   
        else:
            serror = "Nawalnyp::_getTopicMethod [%s] mode is not among the valid ones" % smode
            ologger.error(serror)
            raise Exception(serror)                  
                        
##  #2 management method depending of the original SubscriptionManagementService
    def llist(self):
        ologger.info("NawalnyPSService::llist")
        try:
            return self.ocommonserv("SubscriptionManagementService","listSubscriptions",**dnawalnyparam())
        except:
            serror = "NawalnyPSService error in llist"
            ologger.error(serror)
            raise Exception(serror)  
    def delete(self,suuid):
        ologger.info("NawalnyPSService::delete")
        try:
            return self.ocommonserv("SubscriptionManagementService","deleteSubscription",
                                    sendTime=snawalnytodaytime(),uuid=suuid )
        except:
            serror = "NawalnyPSService error in delete"
            ologger.error(serror)
            raise Exception(serror)        
        
    def resume(self,suuid):
        ologger.info("NawalnyPSService::resume")
        sstatus,sqname,stopic,srelease = self.getstatus(suuid)
        if sstatus == 'PAUSED' : 
            try:
                return self.ocommonserv("SubscriptionManagementService","resumeSubscription",
                                        sendTime=snawalnytodaytime(),uuid=suuid )
            except:
                serror = "NawalnyPSService error in delete"
                ologger.error(serror)
                raise Exception(serror)    
        else :
            print("subscription is not in PAUSED status, do nothing")       
    def pause(self,suuid):
        ologger.info("NawalnyPSService::pause")
        sstatus,sqname,stopic,srelease = self.getstatus(suuid)
        if sstatus != 'PAUSED' :  
            try:
                return self.ocommonserv("SubscriptionManagementService","pauseSubscription",
                                        sendTime=snawalnytodaytime(),uuid=suuid )
            except:
                serror = "NawalnyPSService error in pause"
                ologger.error(serror)
                raise Exception(serror)            
        else: 
            print("subscription %s is already in PAUSED status, do nothing" % suuid )
    def resumeall(self,lsubscription):
        '''resume all valid subscription ie. NMSUBREF >= 27, return a list of queueName'''
        lactivequeue = []
        for dsubitem in lsubscription :
            if is_validsub( dsubitem['release'] )  : # and dsubitem['uuid'] == "a80af22e-8847-48ba-85ca-c20326bbd3aa": 
                print("resumeall:: %s %s %s " % \
                      (dsubitem['queueName'],dsubitem['release'], dsubitem['state'] ))
                if dsubitem['state'] == 'PAUSED':
                    bresume = False
                    try:
                        dres1  = self.ocommonserv("SubscriptionManagementService","resumeSubscription",
                                                  sendTime=snawalnytodaytime(),uuid=dsubitem['uuid'] )
                        bresume =  dres1['status'] == "OK"
                    except:
                        print_exc()
                    if bresume : 
                        print("activatiion for queue [%s] is OK" % dsubitem['queueName'])
                        lactivequeue.append(dsubitem['queueName'])
                    else:
                        print("activatiion for queue [%s] is NOT OK" % dsubitem['queueName'])
                elif dsubitem['state'] == 'ACTIVE':
                    print("queue [%s] is yet ACTIVE" % dsubitem['queueName'])
                    lactivequeue.append(dsubitem['queueName'])
        return lactivequeue 

    def getstatus(self,suuid):
        '''in NM27, we found this method on sibling list method (et donc listSubscrition )
and we return [ status, queueName, topic, release ] '''
        sstatus,sqname,stopic,srelease = None,None,None,None
        dlistsub = self.llist()
        lsubscription = None
        if dlistsub['data']['subscriptions'] : # not None 
            try:
                lsubscription = dlistsub['data']['subscriptions']['item']  # is a list ;-( and not a dict  
            except:
                print_exc()
                serror = "NawalnyPSService getstatus list does not return a valid list"
                ologger.error(serror)
                raise Exception(serror)            
            if lsubscription:
                for dasub in lsubscription :
                    try:
                        if dasub['uuid'] == suuid :
                            sstatus  = dasub['state']
                            sqname   = dasub['queueName']
                            stopic   = dasub['topic']
                            srelease = dasub['release']
                            break # exit for 
                    except:
                        print_exc()
                        serror = "NawalnyPSService getstatus subscription does not have the suited format: %s" % str(dasub)
                        ologger.error(serror)
                        raise Exception(serror)            
        else: # dlistsub['data']['subscriptions'] is None 
            ologger.warning("NawalnyPSService getstatus list returns an empty list")            
        return sstatus,sqname,stopic,srelease          
    def pull(self,squeuename,srelease):
        '''if everything is ok, pull/fetch from NMAX last messages, non desctructives, 
 based on MessagingService '''
        # pullMessages(endUserId: ns1:endUserId, sendTime: ns1:DateTimeSecond, queueName: ns1:QueueName, maxSize: ns0:maxSize, destructive: ns0:destructive)
        # print(self.obinding.pullMessages.__doc__)
        dresult = None 
        NMAX = 100
        if is_validsub(srelease) :
            try:
                return self.ocommonserv("MessagingService","pullMessages",
                                        endUserId=self.suserid , sendTime=snawalnytodaytime(),
                                        queueName=squeuename,maxSize=NMAX,destructive=False )
            except:
                serror = "NawalnyPSService error in pull"
                ologger.error(serror)
                raise Exception(serror) 
        else:
            print("subscription queue [{:s}] has been created with api release {:s}".format(squeuename,srelease))
            print("it cannot be activate/pull by this program")
            return None
# --- technical stuff / check 
    def t_check(self):
        ''' possibility to check NMB2B API : main idea is to check value
contains in zeep internal value <xs:simpleType name="SubscriptionTopic">  with
d26_27equiv keys   BUT : 
zeep does not ease the acces to such a xsd type , though making the parsing .
see :
https://github.com/mvantellingen/python-zeep/issues/513 
https://stackoverflow.com/questions/70715236/get-simpletype-with-xsdrestriction-using-zeep-python '''

        print("t_check: do nothing ")
# --- amqp stuff 
    def backendurl(self):   # ,snmcontext: str): 
        if self.onawalnyaccess.snmcontext == "PREOPS" :
            ssuffice  = "preops."
        elif self.onawalnyaccess.snmcontext == "OPS" :
            ssuffice = ""
        else:
            serror = "NawalnyPSService nmcontext %s should be among PREOPS, OPS" % self.onawalnyaccess.snmcontext
            ologger.error(serror)
            raise Exception(serror) 
        return NawalnyPSService.samqpendpoint.format(ssuffice)

    def amqpregister(self,oamqphandler: object):
        ''' register the amqp handler, container  '''
        ologger.debug("NawalnyPSService amqpregister")
        try:
            self.oamqphandler = oamqphandler 
            self.ocontainer = Container( self.oamqphandler )
        except:
            print_exc()
            serror = "NawalnyPSService amqpregister in error "
            ologger.error(serror)
            raise Exception(serror)         
    def amqprun(self):
        ''' run the container forever ; should be inserted in a thread by the caller''' 
        try:
            self.ocontainer.run()
        except Exception as error:
            #   print_exc()
            # serror = "NawalnyPSService amqprun in error : " + str(error)
            # print(serror)
            # ologger.error(serror)
            raise 
class NASubRep: 
    '''Nawalny Amqp Subscription Replica: Nawalny ps library proposes to reify - make an object - 
of each NMB2B subscription; the goal is to copy/cache, ease management, manipulation of subscription, through
their suuid or queueName '''
    def __init__(self,onawalnyps: object, dsubscription: dict): 
        self.onawalnyps = onawalnyps # keep the orchestrateur 
        
                
class NAHandler(MessagingHandler):
    '''for Nawalny Amqp handler; this class is an abstract class; it is intended to be 
subclassed by the user of Nawalny library;  it defines: a sender , a receiver by activequeue '''
    def __init__(self,onawalnyps: object,senduserid,lactivequeue ):
        ologger.debug("NAHandler __init__")
        super(NAHandler, self).__init__()
        # we rely on self.onawalnyps, self.onawalnyps.onawalnyaccess 
        self.onawalnyps = onawalnyps 
        self.onawalnyaccess  = self.onawalnyps.onawalnyaccess   # shortcut 
        self.surl = self.onawalnyps.backendurl()
        self.senduserid = str(senduserid)
        self.lactivequeue = lactivequeue
        self.dqueue = {}
        self.sender = None 
    def on_start(self, event):
        '''qpid::proton  MessagingHandler n'est pas multi-flux (thread) une approche
de type TCP/select est probablement a l oeuvre derriere tout cela  '''
        ologger.debug("NawalnyPSService NAHandler  start")
        event.container.ssl.client.set_peer_authentication(SSLDomain.VERIFY_PEER)
        # event.container.ssl.client.set_peer_authentication(SSLDomain.ANONYMOUS_PEER)
        event.container.ssl.client.set_credentials(self.onawalnyaccess.scertpath, 
                                                   self.onawalnyaccess.skeypath, 
                                                   self.onawalnyaccess.spassphrase)
        try: 
            conn = event.container.connect(url=self.surl)
# create_sender(
# context: Union[str, proton._url.Url, proton._endpoints.Connection],
# target: Optional[str] = None,
# source: Optional[str] = None,
# name: Optional[str] = None,
# handler: Optional[proton._events.Handler] = None,
# tags: Optional[Callable[[], bytes]] = None,
# options: Optional[Union[SenderOption, List[SenderOption], LinkOption, List[LinkOption]]] = None) → Sender[source] 
            self.sender = event.container.create_sender(conn,None,None,self.senduserid,"default")
            #           context,target,source,name,handler,tags,options
            # _IMPORTANT_ handler doit valoir "default" avec solace pub/sub+ 
            for aqueuename in self.lactivequeue :
                # we create a receiver with these parameter: context,source,target,name 
                ologger.debug("NAHandler::start create_receiver ")
                self.dqueue[aqueuename] = \
                    event.container.create_receiver(self.sender.connection,aqueuename,
                                                    None,aqueuename)
                ologger.info("NAHandler::start receiver %s ", self.dqueue[aqueuename].name )
        except:
            print_exc()
            serror = "NawalnyPSService NAHandler on_start in error "
            ologger.error(serror)
            raise Exception(serror)  
    def on_message(self, event):
        '''supposed to be overwritten '''
        squeuename = event.link.name
        if  event.message.content_encoding == 'gzip' :
            try:
                smsg = gzip.decompress(event.message.body)
            except:
                smsg = "***error decompressing message ***"
                ologger.error(smsg)
                raise Exception(smsg) 
        else:
            smsg = event.message.body 
        ologger.info("NawalnyPSService NAHandler on_message: [%s]: %s" % (squeuename, smsg ) )
# all these meth are default prototype 
    def on_connection_error(self, event):
        ologger.info("NawalnyPSService NAHandler on_connection_error: [%s] " % event )
    def on_session_error(self, event):
        ologger.info("NawalnyPSService NAHandler on_session_error: [%s] " % event )
    def on_transport_error(self, event):
        ologger.info("NawalnyPSService NAHandler on_transport_error: [%s] " % event )
    def on_link_close(self, event):
        ologger.info("NawalnyPSService NAHandler on_link_close: [%s] " % event )
    def on_link_error(self, event):
        ologger.info("NawalnyPSService NAHandler on_link_error: [%s] " % event )
    def on_link_closing(self, event):
        ologger.info("NawalnyPSService NAHandler on_link_closing: [%s] " % event )

## meth "for debugging purposes"
    def describe_event1(self, event):
        print("describe event: ") #  + self.currentqueue)
        print(event.link.__dir__())
        print(event.__dir__())
        print(type(event))
        litem = ['context', 'handler', 'reactor', 'container',  'transport', 'connection',
                 'session', 'link', 'sender', 'receiver', 'delivery', 'type']
        for sitem in litem:
            sintr = "event."+sitem+".name"
            try:
                sitemname = eval(sintr)
            except:
                print("%s cannot be evaluated" % sintr)
            else:
                print("%s name is %s" % (sitem,sitemname))
        print(event.message.content_encoding)
        print(event.message.body)
        return # 
    
    def describe_error_event(self, event):
        print("describe_error_event: ") 
        # print(event.link.__dir__())
        print(event)
        print(event.__dir__())
        litem = ['context', 'handler', 'reactor', 'container',  'transport', 'connection', 'session', 'link', 'sender', 'receiver', 'delivery', 'type', 'wrap', '__init__', 'clazz', 'context', 'handler', 'reactor', 'container', '__getattr__', 'transport', 'connection', 'session', 'link', 'sender', 'receiver', 'delivery', 'type', 'dispatch']
        litem = ['_type', '_clsname', '_context', '_delivery', '_link', '_session', '_connection', '_transport', '__module__', '__doc__', 'TIMER_TASK', 'CONNECTION_INIT', 'CONNECTION_BOUND', 'CONNECTION_UNBOUND', 'CONNECTION_LOCAL_OPEN', 'CONNECTION_LOCAL_CLOSE', 'CONNECTION_REMOTE_OPEN', 'CONNECTION_REMOTE_CLOSE', 'CONNECTION_FINAL', 'SESSION_INIT', 'SESSION_LOCAL_OPEN', 'SESSION_LOCAL_CLOSE', 'SESSION_REMOTE_OPEN', 'SESSION_REMOTE_CLOSE', 'SESSION_FINAL', 'LINK_INIT', 'LINK_LOCAL_OPEN', 'LINK_LOCAL_CLOSE', 'LINK_LOCAL_DETACH', 'LINK_REMOTE_OPEN', 'LINK_REMOTE_CLOSE', 'LINK_REMOTE_DETACH', 'LINK_FLOW', 'LINK_FINAL', 'DELIVERY', 'TRANSPORT', 'TRANSPORT_ERROR', 'TRANSPORT_HEAD_CLOSED', 'TRANSPORT_TAIL_CLOSED', 'TRANSPORT_CLOSED', 'REACTOR_INIT', 'REACTOR_QUIESCED', 'REACTOR_FINAL', 'SELECTABLE_INIT', 'SELECTABLE_UPDATED', 'SELECTABLE_READABLE', 'SELECTABLE_WRITABLE', 'SELECTABLE_EXPIRED', 'SELECTABLE_ERROR', 'SELECTABLE_FINAL', 'wrap', '__init__', 'clazz', 'context', 'handler', 'reactor', 'container', '__getattr__', 'transport', 'connection', 'session', 'link', 'sender', 'receiver', 'delivery', 'type', 'dispatch', '__repr__', '__dict__', '__weakref__', '__hash__', '__str__', '__getattribute__', '__setattr__', '__delattr__', '__lt__', '__le__', '__eq__', '__ne__', '__gt__', '__ge__', '__new__', '__reduce_ex__', '__reduce__', '__subclasshook__', '__init_subclass__', '__format__', '__sizeof__', '__dir__', '__class__']
        for sitem in litem:
            sintr = "event."+sitem+".name"
            try:
                sitemname = eval(sintr)
            except:
                pass
                # print("%s cannot be evaluated" % sintr)
            else:
                print("%s name is %s" % (sitem,sitemname))
        return 
