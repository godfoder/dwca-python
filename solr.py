#!/usr/bin/env python

from __future__ import division
import optparse
import os
import sys
import dwca
import json
import pprint
import sunburnt

parser = optparse.OptionParser()
parser.add_option("-f", "--file", help="DWC-a file to read")
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()

si = sunburnt.SolrInterface("http://localhost:8080/solr/")
fieldList = { "dwc:scientificName": "dwc_scientific_name_t", "dwc:locality": "dwc_locality_t", "dwc:verbatimLocality": "dwc_verbatim_locality_t", "dwc:recordedBy": "dwc_recorded_by_t" }

def solrize(record):
    r = { "text": ""}
    for f in fieldList:
        if f in record:
            r[fieldList[f]] = record[f]
            if fieldList[f].endswith("_t"):
                r["text"] += " " + record[f]
    if "coreid" in record:
        r["id"] = record["coreid"]
    else:
        r["id"] = record["id"]
    try:    
        si.add(r)
    except:     
        pprint.pprint(r)

if options.file == None:
    parser.print_help()
    sys.exit(1)
else:       
    dwcaobj = dwca.Dwca(options.file)
    for record in dwcaobj.core:
        solrize(record)        
    #for dwcrf in dwcaobj.extensions:
        #for record in dwcrf:
            #solrize(record)
    si.commit()
    
        
