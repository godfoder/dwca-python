#!/usr/bin/env python

import zipfile
from lxml import etree
import optparse
import os
import pprint
import sys
import dwca

parser = optparse.OptionParser()
parser.add_option("-f", "--file", help="DWC-a file to read")
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()

if options.file == None:
    parser.print_help()
    sys.exit(1)
else:
    dwcaobj = dwca.Dwca(options.file)
    specimens = {}
    for record in dwcaobj.core:
        specimens[record["id"]] = record
    for dwcrf in dwcaobj.extensions:
        for record in dwcrf:
            if record["coreid"] in specimens:
                if "!extensiondata" in specimens[record["coreid"]]:
                    if dwcrf.rowtype in specimens[record["coreid"]]["!extensiondata"]:
                        specimens[record["coreid"]]["!extensiondata"][dwcrf.rowtype].append(record)
                    else:
                        specimens[record["coreid"]]["!extensiondata"][dwcrf.rowtype] = [record]
                else:
                    specimens[record["coreid"]]["!extensiondata"] = {dwcrf.rowtype: [record]}
                
    pprint.pprint(specimens[specimens.keys()[0]])
        
