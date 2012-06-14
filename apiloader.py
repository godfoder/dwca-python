#!/usr/bin/env python

from __future__ import division
import optparse
import os
import sys
import dwca
import json
import base64
import zlib
import pprint
import urllib2

from Queue import Queue
from threading import Thread

parser = optparse.OptionParser()
parser.add_option("-f", "--file", help="DWC-a file to read")
parser.add_option("-s", "--server", help="Server to load to (hostname:port)", default="dev.idigbio.org:7191")
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()

base_url = "http://{0}/v1/".format(options.server)


def send_to_api(url_frag,data):
    jo = json.dumps(data,separators=(',',':'))    
    req = urllib2.Request(base_url + url_frag, jo, {'Content-Type': 'application/json'})  
    response = None
    try:
        r = urllib2.urlopen(req)
        response = json.loads(r.read())
    except urllib2.HTTPError, e:
        print e
    return response
    
#num_worker_threads = 2
#def worker():
    #while True:        
        #item = q.get()
        #send_to_api(item[0],item[1])
        #q.task_done()

#q = Queue()
#for i in range(num_worker_threads):
     #t = Thread(target=worker)
     #t.daemon = True
     #t.start()

    
if options.file == None:
    parser.print_help()
    sys.exit(1)
else:   
    dwcaobj = dwca.Dwca(options.file)
    metadata = dwcaobj.archdict["#metadata"]
    rawxml = ""
    with dwcaobj.archive.open(metadata,'r') as mf:
        for l in mf:
            rawxml += l
            
    packedXML = base64.b64encode(zlib.compress(rawxml))    

    with open("morphbank.json","w") as of:
        provid = ""
        try:
            provid = dwcaobj.metadata["{eml://ecoinformatics.org/eml-2.1.1}eml"]["#packageId"]
        except:
            pass
        data = { "idigbio:data": dwcaobj.metadata, "idigbio:providerId": provid }
        data["idigbio:data"]["idigbio:packedXML"] = packedXML
        del data["idigbio:data"]["!namespaces"];        
        #of.write(json.dumps(data))
        response = send_to_api('recordsets/',data)        
        if response:
            parentUuid = response['idigbio:uuid']
        else:
            print "Failed to set RecordSet"
            sys.exit()
            
        qu = { "idigbio:items": [], "idigbio:parentUuid": parentUuid }

        pcount = 0
        
        for record in dwcaobj.core:
            data = { "idigbio:data": record,
                     "idigbio:providerId": record["id"],                     
            }
            qu["idigbio:items"].append(data)
            if len(qu["idigbio:items"]) >= 500:
                pcount += len(qu["idigbio:items"])
                print pcount
                #of.write(json.dumps(qu))
                send_to_api("records/",qu)
                qu["idigbio:items"] = []                
            #q.put(("http://dev.idigbio.org:8191/v1/records",data))            
        if len(qu["idigbio:items"]) >= 0:  
            pcount += len(qu["idigbio:items"])
            print pcount
            send_to_api("records/",qu)
            qu["idigbio:items"] = []
            
        for dwcrf in dwcaobj.extensions:
            for record in dwcrf:
                data = { "idigbio:data": record, }
                qu["idigbio:items"].append(data)
                if len(qu["idigbio:items"]) >= 500:
                    send_to_api("mediarecords/",qu)
                    qu["idigbio:items"] = []                    
                #q.put(("http://dev.idigbio.org:8191/v1/mediarecords",data))                
        if len(qu["idigbio:items"]) >= 0:    
            send_to_api("mediarecords/",qu)
            qu["idigbio:items"] = []       
#q.join()       # block until all tasks are done
