#!/bin/python

import zipfile
from lxml import etree
import optparse
import os
import pprint
import sys
import dwca
import HTMLParser

parser = optparse.OptionParser()
parser.add_option("-f", "--file", help="DWC-a file to read")
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()

def stripBlanks(rec):
    nrec = {}
    for key in rec.keys():
        if len(rec[key]) != 0:
            nrec[key.split('/')[-1]] = rec[key]
    return nrec

if options.file == None:
    parser.print_help()
    sys.exit(1)
else:
    dwcaobj = dwca.Dwca(options.file)
    specimens = {}
    for record in dwcaobj.core:
        rec = stripBlanks(record)
	if 'verbatimLongitude' in rec and 'verbatimLatitude' in rec and 'nomenclaturalStatus' in rec and rec['nomenclaturalStatus'] != "not accepted":
            specimens[record["id"]] = rec
    for dwcrf in dwcaobj.extensions:
        for record in dwcrf:
            if record["coreid"] in specimens:
                if dwcrf.rowtype == "http://rs.tdwg.org/ac/terms/multimedia":
                    specimens[record["coreid"]]["iDigBioImageUrl"] = HTMLParser.HTMLParser().unescape(record["http://rs.tdwg.org/ac/terms/bestQualityAccessURI"])
                
    keys_c = {}
    for spec in specimens:
        for k in specimens[spec].keys():
            if k not in keys_c:
                keys_c[k] = 1
            else:
		keys_c[k] += 1
    keys = []
    for k in keys_c:
        if keys_c[k] > 50000:
	    keys.append(k)
    
    with open("dump.csv","w") as of:
        of.write("\"" + "\",\"".join(keys) + "\"\n")
        for spec in specimens:
            rec = []
            for k in keys:
                if k in specimens[spec]:
                    rec.append(specimens[spec][k])
                else:
                    rec.append("")
            try:
                of.write("\"" + "\",\"".join(rec) + "\"\n")
            except UnicodeDecodeError:
                pass 
