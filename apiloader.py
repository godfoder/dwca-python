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
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()

def send_to_api(url,data):
    req = urllib2.Request(url)
    req.add_header('Content-type', 'application/json')
    req.add_data(json.dumps(data,separators=(',',':')))
    try:
        r = urllib2.urlopen(req)
        response = json.loads(r.read())
    except urllib2.HTTPError, e:
        print e
    return response
    
num_worker_threads = 10
def worker():
    while True:
        item = q.get()
        send_to_api(item[0],item[1])
        q.task_done()

q = Queue()
for i in range(num_worker_threads):
     t = Thread(target=worker)
     t.daemon = True
     t.start()

    
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
        of.write(json.dumps(data,separators=(',',':')))
        response = send_to_api('http://dev.idigbio.org:8191/v1/recordsets/',data)
        parentUuid = response['idigbio:uuid']
            
        for record in dwcaobj.core:
            data = { "idigbio:data": record,
                     "idigbio:providerId": record["id"],
                     "idigbio:parentUuid": parentUuid
            }
            q.put(("http://dev.idigbio.org:8191/v1/records",data))
            #of.write(json.dumps(data,separators=(',',':')))        
        for dwcrf in dwcaobj.extensions:
            for record in dwcrf:
                data = { "idigbio:data": record, "idigbio:parentUuid": parentUuid }
                q.put(("http://dev.idigbio.org:8191/v1/mediarecords",data))
                #of.write(json.dumps(data,separators=(',',':')))        
        
q.join()       # block until all tasks are done
