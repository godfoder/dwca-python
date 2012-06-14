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
from uuid import uuid4

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

set_size = 200

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

def send_to_api(url_frag,data):
    jo = json.dumps(data,separators=(',',':'))
    print len(jo)
    req = urllib2.Request(base_url + url_frag, jo, {'Content-Type': 'application/json'})  
    response = None
    try:
        r = urllib2.urlopen(req)
        response = json.loads(r.read())        
    except urllib2.HTTPError, e:
        print e    
    return response
    
ids_to_uuid = {}

field_map = {
    "ac:thumbnailAccessURI": ("thumbnail", "ac:accessURI"),
    "dcterms:thumbnailFormat": ("thumbnail", "dcterms:format"),
    "ac:mediumQualityAccessURI": ("mediumQuality", "ac:accessURI"),
    "dcterms:mediumQualityFormat": ("mediumQuality", "dcterms:format"),    
    "ac:bestQualityAccessURI": ("bestQuality", "ac:accessURI"),
    "dcterms:bestQualityFormat": ("bestQuality", "dcterms:format"),    
    "dcterms:bestQualityExtent": ("bestQuality", "dcterms:extent"),
    "ac:bestQualityFurtherInformationURL": ("bestQuality", "ac:furtherInformationURL"),
}

fullrun = True
      
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
        parentUuid = str(uuid4())
        data = { "idigbio:data": dwcaobj.metadata,
                 "idigbio:providerId": provid,
                 "idigbio:uuid": parentUuid,
               }
        data["idigbio:data"]["idigbio:packedXML"] = packedXML
        del data["idigbio:data"]["!namespaces"];        
        response = send_to_api('recordsets/',data)        
        if response:
            assert parentUuid == response['idigbio:uuid']            
        else:
            print "Failed to set RecordSet"
            sys.exit()
            
        qu = { "idigbio:items": [], "idigbio:parentUuid": parentUuid }
        for record in dwcaobj.core:
            if fullrun or record["id"] == "http://www.morphbank.net/99554":
                rid = str(uuid4())
                data = { "idigbio:data": record,
                        "idigbio:providerId": record["id"],
                        "idigbio:parentUuid": parentUuid,
                        "idigbio:uuid": rid,
                }
                
                qu["idigbio:items"].append(data)
                ids_to_uuid[record["id"]] = rid
                if len(qu["idigbio:items"]) >= set_size:
                    q.put(("records/",qu))
                    #send_to_api("records/",qu)
                    qu = { "idigbio:items": [], "idigbio:parentUuid": parentUuid }                            
        if len(qu["idigbio:items"]) >= 0:
            q.put(("records/",qu))
            #send_to_api("records/",qu)
            qu = None
            
        q.join() # Phase 1 - Records
               
        for dwcrf in dwcaobj.extensions:
            if dwcrf.rowtype == "http://rs.tdwg.org/ac/terms/multimedia":
                qu = { "idigbio:items": [], "idigbio:parentUuid": parentUuid }
                mapqu_list = []
                for record in dwcrf:
                    if fullrun or record["coreid"] == "http://www.morphbank.net/99554":
                        uuid = str(uuid4())
                        data = { "idigbio:uuid": uuid, "idigbio:data": record }
                        if record["coreid"] in ids_to_uuid:
                            data["idigbio:relationships"] = { "record": ids_to_uuid[record["coreid"]] }
                            
                        if "dcterms:identifier" in record:
                            data["idigbio:providerId"] = record["dcterms:identifier"]
                        
                        qu["idigbio:items"].append(data)
                        #if "dcterms:identifier" in record:
                            #ids_to_uuid[record["dcterms:identifier"]] = uuid
                            
                        maps = {}        
                        for f in record:
                            if f in field_map:
                                if "&amp;" in record[f]:
                                    record[f] = record[f].replace("&amp;","&")
                                    
                                if field_map[f][0] in maps:
                                    maps[field_map[f][0]][field_map[f][1]] = record[f]
                                else:
                                    maps[field_map[f][0]] = { "ac:variant": field_map[f][0], field_map[f][1]: record[f] }
                                    
                        mapqu = { "idigbio:items": [], "idigbio:parentUuid": uuid }
                        for mp in maps:
                            item = { "idigbio:data": maps[mp] }
                            mapqu["idigbio:items"].append(item)
                        mapqu_list.append(mapqu)                        
                        
                        if len(qu["idigbio:items"]) >= set_size:
                            #send_to_api("mediarecords/",qu)
                            q.put(("mediarecords/",qu))
                            qu = { "idigbio:items": [], "idigbio:parentUuid": parentUuid }
                if len(qu["idigbio:items"]) >= 0:    
                    #send_to_api("mediarecords/",qu)
                    q.put(("mediarecords/",qu))
                    qu = None
                    
                q.join() # Phase 2 - Media Records
                
                for mq in mapqu_list:
                    q.put(("mediaaps/",mq))
                    
                q.join() # Phase 3 - Media APs
