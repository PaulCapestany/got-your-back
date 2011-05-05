#!/usr/bin/env python
#
# Got Your Back
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Got Your Back (GYB) is a command line tool which allows users to backup and restore their Gmail.

For more information, see http://code.google.com/p/got-your-back/
"""

#global __name__, __author__, __email__, __version__, __license__
__program_name__ = 'Got Your Back: Gmail Backup'
__author__ = 'Jay Lee'
__email__ = 'jay@jhltechservices.com'
__version__ = '0.03 Alpha'
__license__ = 'Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)'

import imaplib
from optparse import OptionParser
import webbrowser
import sys
import os
import os.path
import random
import time
import urllib
import StringIO
import socket
import datetime
import sqlite3
import email
import re
import shlex
import urlparse

import atom.http_core
import gdata
import gdata.gauth
import gdata.service
import gdata.auth
import gdata.apps.service

import gimaplib

def SetupOptionParser():
  # Usage message is the module's docstring.
  parser = OptionParser(usage=__doc__)
  parser.add_option('-e', '--email',
    dest='email',
    help='Full email address of user to backup')
  parser.add_option('-a', '--action',
    dest='action',
    default='backup',
    help='Optional: Action to perform, backup (default) or restore')
  parser.add_option('-f', '--folder',
    dest='folder',
	  help='Optional: Folder to use for backup or restore. Default is ./gmail-backup/',
	  default= 'gmail-backup')
  parser.add_option('-s', '--search',
    dest='gmail_search',
    help='Optional: Gmail search to perform, matching messages are backed up. Text like *7d* will be replaced by the date 7 days ago. For example, -s "after:*3d*" would search for "after:%s".' % (datetime.datetime.now() - datetime.timedelta(3)).strftime('%Y/%m/%d'))
  parser.add_option('-v', '--version',
    action='store_true',
    dest='version',
    help='just print GYB Version and then quit')
  parser.add_option('-d', '--debug',
    action='store_true',
    dest='debug',
    help='Turn on verbose debugging and connection information (for troubleshooting purposes only)')
  parser.add_option('-l', '--label-restored',
    dest='label_restored',
    help='Optional: Used on restore only. If specified, all restored messages will recieve this label. For example, -l "3-21-11 Restore" will label all uploaded messages with that label.')
  return parser

def getProgPath():
  if os.path.abspath('/') != -1:
    divider = '/'
  else:
    divider = '\\'
  return os.path.dirname(os.path.realpath(sys.argv[0]))+divider

def getOAuthFromConfigFile(email):
  cfgFile = '%s%s.cfg' % (getProgPath(), email)
  if os.path.isfile(cfgFile):
    f = open(cfgFile, 'r')
    key = f.readline()[0:-1]
    secret = f.readline()
    f.close()
    return (key, secret)
  else:
    return (False, False)

def requestOAuthAccess(email, debug=False):
  domain = email[email.find('@')+1:]
  scopes = ['https://mail.google.com/',                        # IMAP/SMTP client access
            'https://www.googleapis.com/auth/userinfo#email']  # Email address access (verify token authorized by correct account
  s = gdata.service.GDataService()
  s.debug = debug
  s.source = 'GotYourBack %s / %s / ' % (__version__,
                   'Python %s.%s.%s %s' % (sys.version_info[0], 
                   sys.version_info[1], sys.version_info[2], sys.version_info[3]))
  s.SetOAuthInputParameters(gdata.auth.OAuthSignatureMethod.HMAC_SHA1, consumer_key='anonymous', consumer_secret='anonymous')
  fetch_params = {'xoauth_displayname':'Got Your Back - Gmail Backup'}
  try:
    request_token = s.FetchOAuthRequestToken(scopes=scopes, extra_parameters=fetch_params)
  except gdata.service.FetchingOAuthRequestTokenFailed, e:
    if str(e).find('Timestamp') != -1:
      print "In order to use GYB, your system time needs to be correct.\nPlease fix your time and try again."
      sys.exit(5)
    else:
      print 'Error: %s' % e
  if domain.lower() != 'gmail.com' and domain.lower() != 'googlemail.com':
    url_params = {'hd': domain}
  else:
    url_params = {}
  url = s.GenerateOAuthAuthorizationURL(request_token=request_token, extra_params=url_params)
  raw_input('GYB will now open a web browser page in order for you to grant GYB access to your Gmail. Please make sure you\'re logged in to the correct Gmail account before granting access. Press enter to open the browser. Once you\'ve granted access you can switch back to GYB.')
  try:
    webbrowser.open(str(url))
  except Exception, e:
    pass
  raw_input("You should now see the web page. If you don\'t, you can manually open:\n\n%s\n\nOnce you've granted GYB access, press the Enter key.\n" % url)
  try:
    final_token = s.UpgradeToOAuthAccessToken(request_token)
  except gdata.service.TokenUpgradeFailed:
    print 'Failed to upgrade the token. Did you grant GYB access in your browser?'
    sys.exit(4)
  cfgFile = '%s%s.cfg' % (getProgPath(), email)
  f = open(cfgFile, 'w')
  f.write('%s\n%s' % (final_token.key, final_token.secret))
  f.close()
  return (final_token.key, final_token.secret)

def generateXOAuthString(token, secret, email):
  request = atom.http_core.HttpRequest(
    'https://mail.google.com/mail/b/%s/imap/' % email, 'GET')
  nonce = str(random.randrange(2**64 - 1))
  timestamp = str(int(time.time()))
  signature = gdata.gauth.generate_hmac_signature(
        http_request=request, consumer_key='anonymous', consumer_secret='anonymous', timestamp=timestamp,
        nonce=nonce, version='1.0', next=None, token=token, token_secret=secret)
  return '''GET https://mail.google.com/mail/b/%s/imap/ oauth_consumer_key="anonymous",oauth_nonce="%s",oauth_signature="%s",oauth_signature_method="HMAC-SHA1",oauth_timestamp="%s",oauth_token="%s",oauth_version="1.0"''' % (email, nonce, urllib.quote(signature), timestamp, urllib.quote(token, safe=''))

def getMessagesToBackupList(imapconn, gmail_search=None):
  if gmail_search != None:
    if gmail_search.find('*'):
      search_parts = gmail_search.split('*')
      gmail_search = ''
      for search_part in search_parts:
        try:
          value = int(search_part[:-1])
          time_unit = search_part[-1:]
          if time_unit == 'd':
            days = value
          elif time_unit == 'w':
            days = value * 7
          elif time_unit == 'm':
            days = value * 30
          elif time_unit == 'y':
            days = value * 365
          date = (datetime.datetime.now() - datetime.timedelta(days)).strftime('%Y/%m/%d')
          gmail_search = gmail_search + date
        except ValueError:
          gmail_search = gmail_search+search_part
          continue
    messages_to_backup = gimaplib.GImapSearch(imapconn, gmail_search)
  else:
    #We'll just do an IMAP Search for all mail
    t, d = imapconn.uid('SEARCH', 'ALL')
    if t != 'OK':
      print 'Problem getting all mail!'
      sys.exit(1)
    messages_to_backup = d[0].split()
  return messages_to_backup

def message_is_backed_up(message_num, sqlcur, sqlconn, backup_folder):
    try:
      sqlcur.execute('SELECT message_filename FROM messages where message_num = \'%s\'' % (message_num))
    except sqlite3.OperationalError, e:
      if e.message == 'no such table: messages':
        #print "no messages table... creating one now..."
        sqlcur.execute('CREATE TABLE messages (message_num INTEGER PRIMARY KEY, message_filename TEXT, message_to TEXT, message_from TEXT, message_subject TEXT, message_internaldate TEXT)')
        sqlcur.execute('CREATE TABLE labels (message_num INTEGER, label TEXT)')
        sqlcur.execute('CREATE TABLE flags (message_num INTEGER, flag TEXT)')
        sqlconn.commit()
        return False
    sqlresults = sqlcur.fetchall()
    for x in sqlresults:
      filename = x[0]
      if os.path.isfile(os.path.join(backup_folder, filename)):
        return True
    return False

def doesTokenMatchEmail(cli_email, key, secret, debug=False):
  s = gdata.apps.service.AppsService(source=__program_name__+' '+__version__)
  s.debug = debug
  s.SetOAuthInputParameters(gdata.auth.OAuthSignatureMethod.HMAC_SHA1, consumer_key='anonymous', consumer_secret='anonymous')
  oauth_input_params = gdata.auth.OAuthInputParams(gdata.auth.OAuthSignatureMethod.HMAC_SHA1, consumer_key='anonymous', consumer_secret='anonymous')
  #oauth_token = gdata.auth.OAuthToken(key=key, secret=secret, oauth_input_parameters=oauth_input_parameters)
  s.SetOAuthToken(gdata.auth.OAuthToken(key=key, secret=secret, oauth_input_params=oauth_input_params))
  server_response = s.request('GET', 'https://www.googleapis.com/userinfo/email')
  result_body = server_response.read()
  if server_response.status == 200:
    param_dict = urlparse.parse_qs(result_body)
    authed_email = param_dict['email'][0]
    if authed_email.lower() == cli_email.lower():
      return True
  return False

def main(argv):
  options_parser = SetupOptionParser()
  (options, args) = options_parser.parse_args()
  if options.version:
    print 'Got Your Back %s' % __version__
    sys.exit(0)
  if not options.email:
    options_parser.print_help()
    print "ERROR: --email or -e is required."
    return
  key, secret = getOAuthFromConfigFile(options.email)
  if not key:
    key, secret = requestOAuthAccess(options.email, options.debug)
  if not doesTokenMatchEmail(options.email, key, secret, options.debug):
    print "Error: you did not authorize the OAuth token in the browser with the %s Google Account. Please make sure you are logged in to the correct account when authorizing the token in the browser." % options.email
    cfgFile = '%s%s.cfg' % (getProgPath(), options.email)
    os.remove(cfgFile)
    sys.exit(9)
  imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email), options.debug) # dynamically generate the xoauth_string since they expire after 10 minutes
  if not os.path.isdir(options.folder):
    if options.action == 'backup':
      os.mkdir(options.folder)
    else:
      print 'Error: Folder %s does not exist. Cannot restore.' % options.folder
      sys.exit(3)
  sqldbfile = os.path.join(options.folder, 'msg-db.sqlite')
  sqlconn = sqlite3.connect(sqldbfile)
  sqlconn.text_factory = str
  sqlcur = sqlconn.cursor()
  global ALL_MAIL
  ALL_MAIL = '[Gmail]/All Mail'
  r, d = imapconn.select(ALL_MAIL, readonly=True)
  if r == 'NO':
    ALL_MAIL = '[Google Mail]/All Mail'
    r, d = imapconn.select(ALL_MAIL, readonly=True)
    if r == 'NO':
      print "Error: Cannot select the Gmail \"All Mail\" folder. Please make sure it is not hidden from IMAP."
      sys.exit(3)
  if options.action == 'backup':
    imapconn.select(ALL_MAIL, readonly=True)
    messages_to_process = getMessagesToBackupList(imapconn, options.gmail_search)
    backup_path = options.folder
    if not os.path.isdir(backup_path):
      os.mkdir(backup_path)
    messages_to_backup = []
    #Determine which messages from the search (or all messages) we haven't processed before.
    print "GYB needs to examine %s messages" % len(messages_to_process)
    for message_num in messages_to_process:
      if message_is_backed_up(message_num, sqlcur, sqlconn, options.folder):
        continue
      else:
        messages_to_backup.append(message_num)
    print "GYB already has a backup of %s messages" % (len(messages_to_process) - len(messages_to_backup))
    print "GYB needs to backup %s messages" % len(messages_to_backup)
    backup_count = len(messages_to_backup)
    current = 1
    for message_num in messages_to_backup:
      print "backing up message %s of %s (num: %s)" % (current, backup_count, message_num) 
      #Save message content
      while True:
        try:
          r, full_message_data = imapconn.uid('FETCH', message_num, '(X-GM-LABELS INTERNALDATE FLAGS BODY.PEEK[])')
          if r != 'OK':
            print 'Error: %s' % r
            sys.exit(5)
          break
        except imaplib.IMAP4.abort:
          print 'imaplib.abort error, retrying...'
          imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email), options.debug)
          imapconn.select(ALL_MAIL, readonly=True)
        except socket.error:
          print 'socket.error, retrying...'
          imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email), options.debug)
          imapconn.select(ALL_MAIL, readonly=True)
      
      full_message = full_message_data[0][1]
      everything_else_string = full_message_data[0][0]
      search_results = re.search('^[0-9]* \(X-GM-LABELS \((.*)\) UID [0-9]* (INTERNALDATE \".*\") (FLAGS \(.*\))', everything_else_string)
      labels = shlex.split(search_results.group(1).replace('\\', '\\\\'))
      message_date_string = search_results.group(2)
      message_flags_string = search_results.group(3)
      message_date = imaplib.Internaldate2tuple(message_date_string)
      message_flags = imaplib.ParseFlags(message_flags_string)
      message_rel_filename = os.path.join(str(message_date.tm_year), str(message_date.tm_mon), str(message_date.tm_mday), str(message_num)+'.eml')
      message_full_path = os.path.join(options.folder, str(message_date.tm_year), str(message_date.tm_mon), str(message_date.tm_mday))
      message_full_filename = os.path.join(options.folder, str(message_date.tm_year), str(message_date.tm_mon), str(message_date.tm_mday), str(message_num)+'.eml')
      if not os.path.isdir(message_full_path):
        os.makedirs(message_full_path)
      f = open(message_full_filename, 'wb')
      f.write(full_message)
      f.close()
      m = email.message_from_string(full_message)
      message_from = m.get('from')
      message_to = m.get('to')
      message_subj = m.get('subject')
      sqlcur.execute("INSERT INTO messages (message_num, message_filename, message_to, message_from, message_subject, message_internaldate) VALUES (?, ?, ?, ?, ?, ?)", (message_num, message_rel_filename, message_to, message_from, message_subj, message_date_string))
      for label in labels:
        sqlcur.execute("INSERT INTO labels (message_num, label) VALUES (?, ?)", (message_num, label))
      for flag in message_flags:
        sqlcur.execute("INSERT INTO flags (message_num, flag) VALUES (?, ?)", (message_num, flag))
      sqlconn.commit()
      current = current + 1
  elif options.action == 'restore':
    imapconn.select(ALL_MAIL)  # read/write!
    messages_to_restore = sqlcur.execute('SELECT message_num, message_internaldate, message_filename FROM messages') # All messages
    messages_to_restore_results = sqlcur.fetchall()
    restore_count = len(messages_to_restore_results)
    current = 1
    for x in messages_to_restore_results:
      print "restoring message %s of %s" % (current, restore_count)
      message_num = x[0]
      message_internaldate = x[1]
      message_filename = x[2]
      if not os.path.isfile(os.path.join(options.folder, message_filename)):
        print 'WARNING! file %s does not exist for message %s' % (os.path.join(options.folder, message_filename), message_num)
        print '  this message will be skipped.'
        continue
      f = open(os.path.join(options.folder, message_filename), 'rb')
      full_message = f.read()
      f.close()
      labels_query = sqlcur.execute('SELECT label FROM labels WHERE message_num = ?', (message_num,))
      labels_results = sqlcur.fetchall()
      labels = []
      for l in labels_results:
        labels.append(l[0])
      if options.label_restored:
        labels.append(options.label_restored)
      flags_query = sqlcur.execute('SELECT flag FROM flags WHERE message_num = ?', (message_num,))
      flags_results = sqlcur.fetchall()
      flags = []
      for f in flags_results:
        flags.append(f[0])
      flags_string = ' '.join(flags)
      while True:
        try:
          r, d = imapconn.append(ALL_MAIL, flags_string, imaplib.Internaldate2tuple(message_internaldate), full_message)
          if r != 'OK':
            print 'Error: %s' % r
            sys.exit(5)
          restored_uid = int(re.search('^[APPENDUID [0-9]* ([0-9]*)] \(Success\)$', d[0]).group(1))
          if len(labels) > 0:
            labels_string = '("'+'" "'.join(labels)+'")'
            r, d = imapconn.uid('STORE', restored_uid, '+X-GM-LABELS', labels_string)
            if r != 'OK':
              print 'GImap Set Message Labels Failed: %s' % r
              sys.exit(33)
          break
        except imaplib.IMAP4.abort:
          print 'imaplib.abort error, retrying...'
          imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email), options.debug)
          imapconn.select(ALL_MAIL)
        except socket.error:
          print 'socket.error, retrying...'
          imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email), options.debug)
          imapconn.select(ALL_MAIL)
      current = current + 1
  sqlconn.close()
  imapconn.logout()
  
if __name__ == '__main__':
  main(sys.argv)
