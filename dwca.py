import zipfile
from lxml import etree
import sys
from collections import deque
import xmlDictTools

import pprint

FNF_ERROR = "File {0} not present in archive."

NAMESPACES = { "http://rs.tdwg.org/dwc/terms/": "dwc:",
               "http://purl.org/dc/terms/": "dcterms:",
               "http://rs.tdwg.org/ac/terms/": "ac:",
               "http://ns.adobe.com/xap/1.0/rights/": "xmpRights:",
               "http://ns.adobe.com/xap/1.0/": "xmp:",
               "http://iptc.org/std/Iptc4xmpExt/1.0/xmlns/": "Iptc4xmpExt:",
             }

class Dwca:
    """
        Internal representation of a Darwin Core Archive file.
    """
    
    archdict = None
    archive = None   
    metadata = None
    core = None
    extensions = None

    def __init__(self,name="dwca.zip"):
        self.archive = zipfile.ZipFile(name, 'r')

        meta = self.archive.open('meta.xml','r')
        root = etree.parse(meta).getroot()
        rdict = xmlDictTools.xml2d(root)
                
        self.archdict = rdict["archive"]

        metadata = self.archdict["#metadata"]
        mf = self.archive.open(metadata,'r')            
        mdtree = etree.parse(mf).getroot()           
        self.metadata = xmlDictTools.xml2d(mdtree)      

        corefile = self.archdict["core"]["files"]["location"]
        self.core = DwcaRecordFile(self.archdict["core"], self.archive.open(corefile,'r'))
        
        self.extensions = []
        if "extension" in self.archdict:
            if isinstance(self.archdict["extension"],list):
                for x in self.archdict["extension"]:
                    extfile = x["files"]["location"]
                    self.extensions.append(DwcaRecordFile(x, self.archive.open(extfile,'r')))
            else:            
                extfile = self.archdict["extension"]["files"]["location"]
                self.extensions.append(DwcaRecordFile(self.archdict["extension"], self.archive.open(extfile,'r')))

class DwcaRecordFile:
    """
        Internal representation of a darwin core archive record data file.
    """

    name = ""
    filehandle = None
    closed = True
    namespace = ""
    filetype = ""
    rowtype = ""
    encoding = "utf-8"
    linesplit = "\n"
    fieldsplit = "\t"
    fieldenc = ""
    ignoreheader = 0
    defaults = None
    # Don't instantiate objects here
    fields = None 
    linebuf = None
    def __init__(self,filedict,fh):
        """
            Construct a DwcaRecordFile from a xml tree pointer to the <location> tag containing the data file name
            and a file handle pointing to the data file.
        """
               
        self.name = filedict['files']['location']
        self.filehandle = fh
        
        # Instantiate objects here or we get cross talk between classes.
        self.fields = {}
        self.linebuf = deque()
        closed = False

        idtag = "id"
        if 'id' in filedict:
            self.filetype = "core"
        else:
            idtag = "coreid"
            self.filetype = "extension"
        self.rowtype = filedict["#rowType"]
        self.encoding = filedict["#encoding"]
        self.linesplit = filedict["#linesTerminatedBy"].decode('string_escape') 
        self.fieldsplit = filedict["#fieldsTerminatedBy"].decode('string_escape') 
        self.fieldenc = filedict["#fieldsEnclosedBy"].decode('string_escape') 
        self.ignoreheader = int(filedict["#ignoreHeaderLines"])


        idfld = filedict[idtag]
        self.fields[int(idfld['#index'])] = idtag
        self.defaults = {}
        for fld in filedict['field']:
            term = fld['#term']
            for ns in NAMESPACES:
                if term.startswith(ns):
                    term = term.replace(ns,NAMESPACES[ns])
                    break
            if '#index' in fld:
                self.fields[int(fld['#index'])] = term
            elif '#default' in fld:
                self.defaults[term] = fld['#default']
            else:
                raise Exception("Field {0} has neither index nor default in {1}".format(term,self.name))

    def __iter__(self):
        """
            Returns the object itself, as per spec.
        """
        return self

    def close(self):
        """
            Closes the internally maintained filehandle
        """
        self.filehandle.close()
        closed = filehandle.closed

    def next(self):
        """
            Returns the next line in the record file, used for iteration
        """
        r = self.readline()
        if len(r) > 0:       
            return r
        else:
            raise StopIteration

    def readline(self,size=None):
        """
            Returns a parsed record line from a DWCA file as an dictionary.
        """
        while len(self.linebuf) == 0:            
            fileLine = self.filehandle.readline().decode(self.encoding)
            if len(fileLine) == 0:
                return {}
            else:
                fileLineArr = fileLine.split(self.linesplit)
                for potLine in fileLineArr:
                    if len(potLine) > 0:
                        if self.ignoreheader == 0:
                            self.linebuf.append(potLine)
                        else:
                            self.ignoreheader -= 1
                        
        
        line = self.linebuf.popleft()  
        lineArr = line.split(self.fieldsplit)
        lineDict = {}
        for k in self.fields:
            try:
                if lineArr[k] != "":
                    lineDict[self.fields[k]] = lineArr[k]
            except IndexError:
                print "Line missing fields: ", line
        lineDict.update(self.defaults)
        return lineDict

    def readlines(self,sizehint=None):
        """
            Returns all lines in the file. Cheats off readline.
        """
        lines = []
        for line in self:
            lines.append(self.readline())
        return lines
    
