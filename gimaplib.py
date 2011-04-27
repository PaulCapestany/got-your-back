# Functions that make IMAP behave more Gmail-ish

import imaplib
import re
import shlex
import sys


maxRead = 1000000
class MySSL (imaplib.IMAP4_SSL):
  def read (self, n):
    #print "..Attempting to read %d bytes" % n
      if n <= maxRead:
        return imaplib.IMAP4_SSL.read (self, n)
      else:
        soFar = 0
        result = ""
        while soFar < n:
          thisFragmentSize = min(maxRead, n-soFar)
          #print "..Reading fragment size %s" % thisFragmentSize
          fragment =\
            imaplib.IMAP4_SSL.read (self, thisFragmentSize)
          result += fragment
          soFar += thisFragmentSize # only a few, so not a tragic o/head
        return result

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

def ImapConnect(xoauth_string):
  #imap_conn = imaplib.IMAP4_SSL('imap.gmail.com')
  imap_conn = MySSL('imap.gmail.com')
  imap_conn.debug = 0
  imap_conn.authenticate('XOAUTH', lambda x: xoauth_string)
  if not GImapHasExtensions(imap_conn):
    print "This server does not support the Gmail IMAP Extensions."
    sys.exit(1)
  GImapSendID(imap_conn, 'Jay Test IMAP Client', '0.000001', 'jhltechservices.com', 'support@jhltechservices.com')
  imap_conn.select('[Gmail]/All Mail', readonly=True)
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
    labels = re.search('^[0-9]* \(X-GM-LABELS \((.*?)\) UID %s\)' % uid, d[0]).group(1).replace('\\\\', '\\')
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
  t, d = imapconn.store(uid, '+X-GM-LABELS', labels_string)
  if t != 'OK':
    raise GImapSetMessageLabelsError('GImap Set Message Labels Failed: %s' % t)