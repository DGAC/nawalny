# Next AWesome Access Library to NMB2B in pYthon 

In the aviation world, Eurocontrol is a European pluri-state organization with more than 6 decades of expertise in the field of securing, organizing, optimizing civile aviation operations. 

The Network Management Operation Centre is one of the key elements to smooth trafic flows, manage congestion in finding alternative paths in cooperatiion with Air Navigation Service Providers. 
For 15 years, NMOC has developped NMB2B, a business-to-business online data service. 
It is strongly advised to learn what NMB2B is all about. 
It is required to have an account and an access via a certificate to this set of services, 
before diving into the use of Nawalny. 

See [www.eurocontrol.int/service/network-manager-business-business-b2b-web-services](https://www.eurocontrol.int/service/network-manager-business-business-b2b-web-services)

# Intend 

Nawalny is a python framework easing the access to NMB2B api in Soap/Xml - synchronous request/reply mode , and Amqp1.0 - publish / subscribe.  It aims at :

* delivering an easy access to required resources as authentication, via X509 certificate, in all the situation: testing access, connection, request, download of file, coupling r/r and p/s principles ...
* easing the forging of input parameters to the different set of requests, 
* addressing expert features as requesting alternative backend to the two "preops" and "ops" environment. 

# Prerequisite 

The user will have to obtain a X509 certificate with "preops" access to begin with and dipose of 
API definition file proposed by Eurocontrol.
CommonServices_27.0.0.xsd  GeneralinformationServices_27.0.0.xsd GeneralinformationServices_PREOPS_27.0.0.wsdl  in "27" are the minimum set of file to bootstrap the mecanism. 
See (file:INSTALL.txt) to learn how to install nawalny library. 

# High level principles 

The Nawalny library takes advantage of Python idioms to be elegant and readable-by-human as far as possible. As a simplistic example this sequence showcases the access to request / reply service.  
More than 190 named requests are available through the 6 definitions of interface, so a lot of data services exposed, and to be explored ! 

``` 
# create a main object and configure it with wsdl location,version, choice preops/ops, an app name
# it supposes that you populate a python dict dmconf with your info ... 
onmaccess = NawalnyAccess(
           dmconf['swsdlpath'], "myappname", dmconf['nmapiindex'], dmconf['nmcontext']) 
# configure with credential (X509 cert): cert_key bundled, cert/key, cert/key_cyphered/password
onmaccess.set_credential(dmconf['nmcertpath'],dmconf['nmkeypath'],dmconf['nmpassword'])

# mount a set of services eg. "FlightServices"            
oflightservs =  onmaccess.get_asetofservice("FlightServices") 

# fire a Soap/Request with input parameters and get the results  
dsetofflight = oflightservs('FlightManagementService','queryFlightsByAerodrome',
                            **dnawalnyparam(suserdef)) 
                            
``` 
A Facade class - NawalnyPSService - proposes the different methods required to manage Publish/Subscribe 
subscriptions: create,update,retrieve,delete,pause,resume, ...
2 methods are devoted to register an object of abstract class NAHandler and then trigger the listening 
principle and the callback to the handler. The app - the caller - should subclass NAHandler and create a real instance doing what is intended by the app. 

``` 
# create an object NawalnyPSService pointing on an NawalnyAccess object (in charge of credential) 
onmpsservice = NawalnyPSService(onmaccess,"myappname")

# create a subscription 
dsubscription =  {'subscription': {
    'topic' : "FLIGHT_DATA",
    'onBehalfOfUnit' : "FMPLFFF",
    'description' : "fmplff all",
    'messageFilter' : {
        'includeProposalFlights': True, 
        'flightSet': {
           'item': [
               { 'anuIds': { 'item': ['FMPLFFF'] }                 } ] } },
    'payloadConfiguration':{
        'concernedUnits': True, 
        'flightFields': {
            'item': []  #meaning all fields
            } } } }
dnmsub1 =  onmpsservice.create(**dsubscription) 
snmsub1uuid = dnmsub1['data']['subscription']['summary']['uuid'] 
snmsub1queueName = dnmsub1['data']['subscription']['summary']['queueName'] 

# subclass NAHandler 
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
        print("[%s]: %s" % (squeuename, smsg ))

# new Nawalny handler  
omyhandler = MyHandler(onmpsservice,sapp,[snmsub1queueName])
onmpsservice.amqpregister(omyhandler)

# resuming one queue which is now supposed ACTIVE
lactivequeue = onmpsservice.resume(snmsub1uuid) 

# listening incoming messages (forever) and do what the app must do 
onmpsservice.amqprun()

``` 

# Other projects 

Several projects have addressed this topic, especially in Python; we can mention:

*  [nmb2b-soapy] (https://github.com/DGAC/nmb2b-soapy) ;
*  [pyb2b] (https://github.com/xoolive/pyb2b) ; 

Browsing https://github.com/DGAC/ you will discover software doing the same job in other languages, Php, Javascript, Cobol (no, just kidding) ... 

# About the name 

Navalnoy was a tiny village near Verdun which was completly devastated during WWI. Its inhabitants, brave and fullish have tried to rebuilt it but without success.  
