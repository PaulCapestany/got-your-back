# Functions that make IMAP behave more Gmail-ish

import imaplib
import re
import shlex
import sys
import cStringIO
import zlib

import gyb

maxRead = 1000000
class MySSL (imaplib.IMAP4_SSL):

  def __init__(self, host=None, port=imaplib.IMAP4_SSL_PORT):
      self.compressor = None
      self.decompressor = None
      self.raw_in = 0
      self.raw_out = 0
      self.full_in = 0
      self.full_out = 0
      imaplib.IMAP4_SSL.__init__(self, host, port)

  def start_compressing(self):
      """start_compressing()
      Enable deflate compression on the socket (RFC 4978)."""
  
      # rfc 1951 - pure DEFLATE, so use -15 for both windows
      self.decompressor = zlib.decompressobj(-15)
      self.compressor = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, -15)
  
  def read(self, size):
      """Read 'size' bytes from remote."""
      # sslobj.read() sometimes returns < size bytes
      chunks = cStringIO.StringIO()
      read = 0
      while read < size:
        data = self.read2(min(size-read, 16384))
        read += len(data)
        #chunks.append(data)
        chunks.write(data)

      chunks.seek(0)
      return chunks.read()

  def read2(self, size):
      """data = read(size)
      Read at most 'size' bytes from remote."""
      if self.decompressor is None:
        data = self.sslobj.read(size)
        self.raw_in += len(data)
        self.full_in += len(data)
        return data
  
      if self.decompressor.unconsumed_tail:
        data = self.decompressor.unconsumed_tail
      else:
        data = self.sslobj.read(8192)
        self.raw_in += len(data)
      data = self.decompressor.decompress(data, size)
      self.full_in += len(data)
      return data

  def readline(self):
      """Read line from remote."""
      line = cStringIO.StringIO()
      while 1:
        char = self.read(1)
        #line.append(char)
        line.write(char)
        #if char in ("\n", ""): return ''.join(line)
        if char in ("\n", ""): 
          line.seek(0)
          return line.read()
  
  def send(self, data):
      """send(data)
      Send 'data' to remote."""
      self.full_out += len(data)
      if self.compressor is not None:
        data = self.compressor.compress(data)
        data += self.compressor.flush(zlib.Z_SYNC_FLUSH)
      self.raw_out += len(data)
      self.sslobj.sendall(data)

  def display_stats(self):
      if self.compressor is not None:
        print "raw_in   ", self.raw_in
        print "full in  ", self.full_in
        print "ratio = %d%%" % (100*(self.full_in-self.raw_in)/self.full_in)
        print "raw_out  ", self.raw_out
        print "full out ", self.full_out
        print "ratio = %d%%" % (100*(self.full_out-self.raw_out)/self.full_out)
        print "Compression efficiency: %d%%" % (100*(self.full_in+self.full_out-self.raw_in-self.raw_out)/(self.full_in+self.full_out))

def GImapHasExtensions(imapconn):
  '''
  Args:
    imapconn: object, an authenticated IMAP connection
  
  Returns:
    boolean, True if Gmail IMAP Extensions defined at:
             http://code.google.com/apis/gmail/imap
             are supported, False if not.
  '''
  t, d = imapconn.capability()
  if t != 'OK':
    raise GImapHasExtensionsError('GImap Has Extensions could not check server capabilities: %s' % t)
  return bool(d[0].count('X-GM-EXT-1'))

def GImapSendID(imapconn, name, version, vendor, contact):
  '''
  Args:
    imapconn: object, an authenticated IMAP connection
    name: string, IMAP Client Name
    version: string, IMAP Client Version
    vendor: string, IMAP Client Vendor
    contact: string, email address of contact

  Returns:
    list of IMAP Server ID response values
  '''
  commands = {'ID' : ('AUTH',)}
  imaplib.Commands.update(commands)
  id = '("name" "%s" "version" "%s" "vendor" "%s" "contact" "%s")' % (name, version, vendor, contact)
  t, d = imapconn._simple_command('ID', id)
  r, d = imapconn._untagged_response(t, d, 'ID')
  if r != 'OK':
    raise GImapSendIDError('GImap Send ID failed to send ID: %s' % t)
  return shlex.split(d[0][1:-1])

def ImapConnect(xoauth_string, debug, compress=False):
  #imap_conn = imaplib.IMAP4_SSL('imap.gmail.com')
  imap_conn = MySSL('imap.gmail.com')
  if debug:
    imap_conn.debug = 4
  imap_conn.authenticate('XOAUTH', lambda x: xoauth_string)
  if not GImapHasExtensions(imap_conn):
    print "This server does not support the Gmail IMAP Extensions."
    sys.exit(1)
  if compress:
    t, d = imap_conn.xatom("COMPRESS", "DEFLATE")
    if t == 'OK':
      imap_conn.start_compressing()

  GImapSendID(imap_conn, gyb.__program_name__, gyb.__version__, gyb.__author__, gyb.__email__)
  return imap_conn

def GImapSearch(imapconn, gmail_search):
  '''
  Args:
    imapconn: object, an authenticated IMAP connection to a server supporting the X-GM-EXT1 IMAP capability (imap.gmail.com)
    gmail_search: string, a typical Gmail search as defined at:
                     http://mail.google.com/support/bin/answer.py?answer=7190

  Returns:
    list, the IMAP UIDs of messages that match the search

  Note: Only the IMAP Selected folder is searched, it's as if 'in:<current IMAP folder>' is appended to all searches. If you wish to search all mail, select '[Gmail]/All Mail' before performing the search.
  '''
  #t, d = imapconn.search(None, 'X-GM-RAW', gmail_search)
  gmail_search = gmail_search.replace('\\', '\\\\').replace('"', '\\"')
  gmail_search = '"' + gmail_search + '"'
  t, d = imapconn.uid('SEARCH', 'X-GM-RAW', gmail_search)
  if t != 'OK':
    raise GImapSearchError('GImap Search Failed: %s' % t)
  return d[0].split()

def GImapGetMessageLabels(imapconn, uid):
  '''
  Args:
    imapconn: object, an authenticated IMAP connection to a server supporting the X-GM-EXT1 IMAP capability (imap.gmail.com)
    uid: int, the IMAP UID for the message whose labels you wish to learn.

  Returns:
    list, the Gmail Labels of the message
  '''
  t, d = imapconn.uid('FETCH', uid, '(X-GM-LABELS)')
  if t != 'OK':
    raise GImapGetMessageLabelsError('GImap Get Message Labels Failed: %s' % t)
  if d[0] != None:
    labels = re.search('^[0-9]* \(X-GM-LABELS \((.*?)\) UID %s\)' % uid, d[0]).group(1)
    labels_list = shlex.split(labels)
  else:
    labels_list = []
  return labels_list
  
def GImapSetMessageLabels(imapconn, uid, labels):
  '''
  Args:
    imapconn: object, an authenticated IMAP connection to a server supporting the X-GM-EXT1 IMAP capability (imap.gmail.com)
    uid: int, the IMAP UID for the message whose labels you wish to learn.
    labels: list, names of labels to be applied to the message
    
  Returns:
    null on success or Error on failure
  
  Note: specified labels are added but the message's existing labels that are not specified are not removed.
  '''
  labels_string = '"'+'" "'.join(labels)+'"'
  t, d = imapconn.uid('STORE', uid, '+X-GM-LABELS', labels_string)
  if t != 'OK':
    print 'GImap Set Message Labels Failed: %s' % t
    exit(33)

def GImapGetFolder(imapconn, foldertype='\AllMail'):
  '''
  Args:
    imapconn: object, an authenticated IMAP connection
    foldertype: one of the Gmail special folder types
  
  Returns:
    string,  selectable IMAP name of folder
  '''
  list_response_pattern = re.compile(r'\((?P<flags>.*?)\) "(?P<delimiter>.*)" (?P<name>.*)')
  t, d = imapconn.xatom('xlist', '""', '*')
  if t != 'OK':
    raise GImapHasExtensionsError('GImap Get Folder could not check server XLIST: %s' % t)
  xlist_data = imapconn.response('XLIST') [1]
  for line in xlist_data:
    flags, delimiter, mailbox_name = list_response_pattern.match(line).groups()
    if flags.count(foldertype) > 0:
      return mailbox_name
  return None
