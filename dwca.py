import zipfile
from lxml import etree
import sys
from collections import deque

FNF_ERROR = "File {0} not present in archive."

class Dwca:
    """
        Internal representation of a Darwin Core Archive file.
    """
    
    archtree = None
    archive = None
    namespace = ""
    metadata = None
    core = None
    extensions = None

    def __init__(self,name="dwca.zip"):
        self.archive = zipfile.ZipFile(name, 'r')
        try:
            meta = self.archive.open('meta.xml','r')
        except Exception:
            print(FNF_ERROR.format('meta.xml'))
            sys.exit(1)
    
        self.archtree = etree.parse(meta)      
        root = self.archtree.getroot()

        try:
            ns = root.nsmap[None]
            self.namespace = "{{{0}}}".format(ns)
        except:
            pass

        metadata = root.attrib["metadata"]
        try:
            mf = self.archive.open(metadata,'r')
            self.metadata = etree.parse(mf)
        except Exception:
            print(FNF_ERROR.format(metadata))

        files = self.archtree.findall(".//{0}files/{0}location".format(self.namespace))
       
        self.extensions = []
        for f in files:
            fname = f.text
            fgp = f.getparent().getparent()
            try:
                rfh = self.archive.open(fname,'r')
                rf = DwcaRecordFile(f,rfh)
                if fgp.tag == "{0}core".format(self.namespace):
                    self.core = rf
                else:
                    self.extensions.append(rf)               
            except Exception:
                print(FNF_ERROR.format(fname))
                sys.exit(1)

        if self.core == None:
            print("Core file definition not found in meta.xml")
            sys.exit(1)

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
    # Don't instantiate objects here
    fields = None 
    linebuf = None
    def __init__(self,filetree,fh):
        """
            Construct a DwcaRecordFile from a xml tree pointer to the <location> tag containing the data file name
            and a file handle pointing to the data file.
        """
        self.name = filetree.text
        self.filehandle = fh
        
        # Instantiate objects here or we get cross talk between classes.
        self.fields = {}
        self.linebuf = deque()
        closed = False

        try:
            ns = filetree.nsmap[None]
            self.namespace = "{{{0}}}".format(ns)
        except:
            pass

        fgp = filetree.getparent().getparent()
        self.filetype = fgp.tag
        self.rowtype = fgp.attrib["rowType"]
        self.encoding = fgp.attrib["encoding"]
        self.linesplit = fgp.attrib["linesTerminatedBy"].decode('string_escape') 
        self.fieldsplit = fgp.attrib["fieldsTerminatedBy"].decode('string_escape') 
        self.fieldenc = fgp.attrib["fieldsEnclosedBy"].decode('string_escape') 
        self.ignoreheader = int(fgp.attrib["ignoreHeaderLines"])

        idtag = "id"
        if fgp.tag != "{0}core".format(self.namespace):
            idtag = "coreid"

        idfld = fgp.find("{0}{1}".format(self.namespace,idtag))
        self.fields[int(idfld.attrib['index'])] = idtag
        for fld in fgp.findall("{0}field".format(self.namespace)):
            self.fields[int(fld.attrib['index'])] = fld.attrib['term']

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
            fileLine = self.filehandle.readline()
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
                lineDict[self.fields[k]] = lineArr[k]
            except IndexError:
                print "Line missing fields: ", line
        return lineDict

    def readlines(self,sizehint=None):
        """
            Returns all lines in the file. Cheats off readline.
        """
        lines = []
        for line in self:
            lines.append(self.readline())
        return lines
    
