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

(options, args) = parser.parse_args()

if options.file == None:
    parser.print_help()
    sys.exit(1)
else:   
    dwcaobj = dwca.Dwca(options.file)
    for record in dwcaobj.core:
        print(json.dumps(record,separators=(',',':')))        
    for dwcrf in dwcaobj.extensions:
        for record in dwcrf:
            print(json.dumps(record,separators=(',',':')))
    print(json.dumps(dwcaobj.metadata,separators=(',',':')))
    
        
