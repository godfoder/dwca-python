#!/usr/bin/env python

from __future__ import division
import optparse
import os
import sys
import dwca
import json
import pprint

parser = optparse.OptionParser()
parser.add_option("-f", "--file", help="DWC-a file to read")
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")
parser.add_option("-n", "--nocount", dest="count", action="store_false", default=True,
                  help="print out record counts")
parser.add_option("-j", "--json", action="store_true", default=False,
                  help="print out record JSON") 

(options, args) = parser.parse_args()

coverage = { "Taxon": {
                    "Scientific Name": { "divisior": 1, "fields": ["dwc:scientificName"]},
                    "Taxon Rank": { "divisior": 1, "fields": ["dwc:taxonRank"]},
                    "Higher Order Taxa": { "divisior": 1, "fields": ["dwc:kingdom","dwc:family"]},
                    "Scientific Name Status": { "divisior": 1, "fields": ["dwc:scientificNameAuthorship","dwc:nomenclaturalStatus","dwc:dateIdentified","dwc:identifiedBy" ]},
                    "Remarks": { "divisior": 1, "fields": ["dwc:identificationRemarks"]},
                },
             "Locality": {
                 "Decimal Coordiantes": { "divisior": 2, "fields": ["dwc:decimalLatitude","dwc:decimalLongitude"]},
                 "Verbatim Coordinates": { "divisior": 2, "fields": ["dwc:verbatimLatitude","dwc:verbatimLongitude"]},
                 "Administrative Boundaries": { "divisior": 4, "fields": ["dwc:continent","dwc:country","dwc:stateProvince","dwc:county"]},
                 "Water Body": { "divisior": 1, "fields": ["dwc:waterBody"]},
                 "Locality": { "divisior": 1, "fields": ["dwc:locality"]},
                 "Verbatim Locality": { "divisior": 1, "fields": ["dwc:verbatimLocality"]},
                 "Depth": { "divisior": 1, "fields": ["dwc:maximumDepthInMeters","dwc:maximumDepthInMeters"]},
                 "Elevation": { "divisior": 1, "fields": ["dwc:maximumElevationInMeters",""]},
                 "Identifier": { "divisior": 1, "fields": ["dwc:locationID"]},
                },
              "Temporal": {
                  "Event Date": { "divisior": 1, "fields": ["dwc:eventDate"]},
                  "Verbatim Event Date": { "divisior": 1, "fields": ["dwc:verbatimEventDate"]},
                },
              "Provenance": {
                  "Collector": { "divisior": 1, "fields": ["dwc:recordedBy","dwc:recordNumber"]},
                  "Basis of Record": { "divisior": 1, "fields": ["dwc:basisOfRecord"]},
                  "DwC Triple": { "divisior": 3, "fields": ["dwc:institutionCode","dwc:collectionCode","dwc:catalogNumber"]},
                },
           }

coverageInversion = {}
for level in coverage:
    l = coverage[level]
    for group in l:
        g = l[group]
        for key in g['fields']:
            if key in coverageInversion:
                coverageInversion[key].append((level,group))
            else:
                coverageInversion[key] = [(level,group)]

if options.file == None:
    parser.print_help()
    sys.exit(1)
else:   
    dwcaobj = dwca.Dwca(options.file)
    keycounts = {"core": {}}    
    reccount = {"core": 0}
    coverageIndex = {}
    for record in dwcaobj.core:
        reccount["core"] += 1
        for key in record:
            if key in keycounts["core"]:
                keycounts["core"][key] += 1
            else:
                keycounts["core"][key] = 1
            
            if key in coverageInversion:
                for (level,group) in coverageInversion[key]:
                    if level not in coverageIndex:
                        coverageIndex[level] = {}                        
                    if group not in coverageIndex[level]:
                        coverageIndex[level][group] = 0
                    coverageIndex[level][group] += 1
        if options.json:
            print(json.dumps(record,separators=(',',':')))        
    for dwcrf in dwcaobj.extensions:
        keycounts[dwcrf.name] = {}
        reccount[dwcrf.name] = 0
        for record in dwcrf:
            reccount[dwcrf.name] += 1
            for key in record:
                if key in keycounts[dwcrf.name]:
                    keycounts[dwcrf.name][key] += 1
                else:
                    keycounts[dwcrf.name][key] = 1
            if options.json:
                print(json.dumps(record,separators=(',',':')))
        pass
    if options.json:
        print(json.dumps(dwcaobj.metadata,separators=(',',':')))
    if options.count:
        
        for key in reccount:
            cci = float(0)
            ccc = 0
            count = reccount[key]
            for reckey in keycounts[key]:                
                total = keycounts[key][reckey]
                pct = total / count
                cci += pct
                ccc += 1
                print "{0}/{1}: {2}".format(key,reckey,pct * 100)    
            if key == "core":
                for level in coverageIndex:
                    print "Coverages for {0}".format(level)
                    for group in coverageIndex[level]:
                        ci = coverageIndex[level][group]
                        cic = coverage[level][group]['divisior']
                        cipct = ci/(count*cic) * 100 
                        print "Coverage Index for {0}/{1}: {2}".format(level,group,cipct)
            print "Composite coverage index for {0}: {1}".format(key,cci / ccc * 100)
    
        
