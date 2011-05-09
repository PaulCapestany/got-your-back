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
__version__ = '0.08 Alpha'
__license__ = 'Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)'
__db_schema_version__ = '2'

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
from itertools import islice, chain
import math

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
    type='choice',
    choices=['backup','restore','estimate'],
    dest='action',
    default='backup',
    help='Optional: Action to perform. backup, restore or estimate.')
  parser.add_option('-f', '--folder',
    dest='folder',
	  help='Optional: Folder to use for backup or restore. Default is ./gmail-backup/',
	  default='XXXuse-email-addessXXX')
  parser.add_option('-s', '--search',
    dest='gmail_search',
    default='in:anywhere',
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
    help='Optional: Used on restore only. If specified, all restored messages will receive this label. For example, -l "3-21-11 Restore" will label all uploaded messages with that label.')
  parser.add_option('-t', '--two-legged',
    dest='two_legged',
    help='Google Apps Business and Education accounts only. Use administrator two legged OAuth to authenticate as end user.')
  return parser

def getProgPath():
  if os.path.abspath('/') != -1:
    divider = '/'
  else:
    divider = '\\'
  return os.path.dirname(os.path.realpath(sys.argv[0]))+divider

def batch(iterable, size):
  sourceiter = iter(iterable)
  while True:
    batchiter = islice(sourceiter, size)
    yield chain([batchiter.next()], batchiter)

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

def generateXOAuthString(token, secret, email, two_legged=False):
  nonce = str(random.randrange(2**64 - 1))
  timestamp = str(int(time.time()))
  if two_legged:
    request = atom.http_core.HttpRequest('https://mail.google.com/mail/b/%s/imap/?xoauth_requestor_id=%s' % (email, urllib.quote(email)), 'GET')
    signature = gdata.gauth.generate_hmac_signature(
        http_request=request, consumer_key=token, consumer_secret=secret, timestamp=timestamp,
        nonce=nonce, version='1.0', next=None)
    return '''GET https://mail.google.com/mail/b/%s/imap/?xoauth_requestor_id=%s oauth_consumer_key="%s",oauth_nonce="%s",oauth_signature="%s",oauth_signature_method="HMAC-SHA1",oauth_timestamp="%s",oauth_version="1.0"''' % (email, urllib.quote(email), token, nonce, urllib.quote(signature), timestamp)
  else:
    request = atom.http_core.HttpRequest('https://mail.google.com/mail/b/%s/imap/' % email, 'GET')
    signature = gdata.gauth.generate_hmac_signature(
        http_request=request, consumer_key='anonymous', consumer_secret='anonymous', timestamp=timestamp,
        nonce=nonce, version='1.0', next=None, token=token, token_secret=secret)
    return '''GET https://mail.google.com/mail/b/%s/imap/ oauth_consumer_key="anonymous",oauth_nonce="%s",oauth_signature="%s",oauth_signature_method="HMAC-SHA1",oauth_timestamp="%s",oauth_token="%s",oauth_version="1.0"''' % (email, nonce, urllib.quote(signature), timestamp, urllib.quote(token))

def getMessagesToBackupList(imapconn, gmail_search='in:anywhere'):
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
  return gimaplib.GImapSearch(imapconn, gmail_search)

def message_is_backed_up(message_num, sqlcur, sqlconn, backup_folder):
    try:
      sqlcur.execute('SELECT message_filename FROM messages where message_num = \'%s\'' % (message_num))
    except sqlite3.OperationalError, e:
      if e.message == 'no such table: messages':
        print "\n\nError: your backup database file appears to be corrupted."
        sys.exit(8)
    sqlresults = sqlcur.fetchall()
    for x in sqlresults:
      filename = x[0]
      if os.path.isfile(os.path.join(backup_folder, filename)):
        return True
    return False

def check_db_settings(sqlcur, sqlconn, action, user_email_address):
  try:
    sqlcur.execute('SELECT value FROM settings WHERE name = \'db_version\'')
    db_version = str(sqlcur.fetchone()[0])
    sqlcur.execute('SELECT value FROM settings WHERE name = \'email_address\'')
    db_email_address = str(sqlcur.fetchone()[0])
    
    if db_version != __db_schema_version__:
      print "\n\nSorry, this backup folder is only compatible with version %s of the database schema while GYB %s is only compatible with version %s" % (db_version, __version__, __db_schema_version__)
      sys.exit(4)
    
    # Only restores are allowed to use a backup folder started with another account (can't allow 2 Google Accounts to backup/estimate from same folder)
    if action != 'restore':
      if user_email_address.lower() != db_email_address.lower():
        print "\n\nSorry, this backup folder should only be used with the %s account that it was created with for incremental backups. You specified the %s account" % (db_email_address, user_email_address)
        sys.exit(5)
  except sqlite3.OperationalError, e:
    if e.message == 'no such table: settings':
      print "\n\nSorry, this version of GYB requires version %s of the database schema. Your backup folder database does not have a version." % (__db_schema_version__)
      sys.exit(6)
        
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

def restart_line():
  sys.stdout.write('\r')
  sys.stdout.flush()

def initializeDB(sqlcur, sqlconn, email):
  sqlcur.execute('CREATE TABLE messages (message_num INTEGER PRIMARY KEY, message_filename TEXT, message_to TEXT, message_from TEXT, message_subject TEXT, message_internaldate TIMESTAMP)')
  sqlcur.execute('CREATE TABLE labels (message_num INTEGER, label TEXT)')
  sqlcur.execute('CREATE TABLE flags (message_num INTEGER, flag TEXT)')
  sqlcur.execute('CREATE TABLE settings (name TEXT PRIMARY KEY, value TEXT)')
  sqlconn.commit()
  sqlcur.execute('INSERT INTO settings (name, value) VALUES (\'email_address\', ?)', [email])
  sqlcur.execute('INSERT INTO settings (name, value) VALUES (\'db_version\', ?)', [__db_schema_version__])
  sqlconn.commit()

def get_message_size(imapconn, uids):
  if type(uids) == type(int()):
    uid_string == str(uid)
  elif type(uids) == type(list()):
    uid_string = ','.join(uids)
  t, d = imapconn.uid('FETCH', uid_string, '(RFC822.SIZE)')
  if t != 'OK':
    print "Failed to retrieve size for message %s" % uid
    exit(9)
  total_size = 0
  for x in d:
    message_size = int(re.search('^[0-9]* \(UID [0-9]* RFC822.SIZE ([0-9]*)\)$', x).group(1))
    total_size = total_size + message_size
  return total_size
  
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
  if options.folder == 'XXXuse-email-addessXXX':
    options.folder = "GYB-GMail-Backup-%s" % options.email
  if options.two_legged: # 2-Legged OAuth (Admins)
    if os.path.isfile(options.two_legged):
      f = open(options.two_legged, 'r')
      key = f.readline()[0:-1]
      secret = f.readline()
      f.close()
    else:
      f = open(options.two_legged, 'w')
      key = raw_input('Enter your domain\'s OAuth consumer key: ')
      secret = raw_input('Enter your domain\'s OAuth consumer secret: ')
      f.write('%s\n%s' % (key, secret))
      f.close()
  else:  # 3-Legged OAuth (End Users)
    key, secret = getOAuthFromConfigFile(options.email)
    if not key:
      key, secret = requestOAuthAccess(options.email, options.debug)
    if not doesTokenMatchEmail(options.email, key, secret, options.debug):
      print "Error: you did not authorize the OAuth token in the browser with the %s Google Account. Please make sure you are logged in to the correct account when authorizing the token in the browser." % options.email
      cfgFile = '%s%s.cfg' % (getProgPath(), options.email)
      os.remove(cfgFile)
      sys.exit(9)
  imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email, options.two_legged), options.debug) # dynamically generate the xoauth_string since they expire after 10 minutes
  if not os.path.isdir(options.folder):
    if options.action == 'backup':
      os.mkdir(options.folder)
    elif options.action == 'restore':
      print 'Error: Folder %s does not exist. Cannot restore.' % options.folder
      sys.exit(3)
  sqldbfile = os.path.join(options.folder, 'msg-db.sqlite')
  # Do we need to initialize a new database?
  newDB = (not os.path.isfile(sqldbfile)) and (options.action == 'backup')
  
  #If we're not doing a estimate or if the db file actually exists we open it (creates db if it doesn't exist)
  if options.action != 'estimate' or os.path.isfile(sqldbfile):
    print "\nUsing backup folder %s" % options.folder
    global sqlconn
    global sqlcur
    sqlconn = sqlite3.connect(sqldbfile, detect_types=sqlite3.PARSE_DECLTYPES)
    sqlconn.text_factory = str
    sqlcur = sqlconn.cursor()
    if newDB:
      initializeDB(sqlcur, sqlconn, options.email)
    check_db_settings(sqlcur, sqlconn, options.action, options.email)
  global ALL_MAIL
  ALL_MAIL = gimaplib.GImapGetFolder(imapconn)
  if ALL_MAIL == None:
    # Last ditched best guess but All Mail is probably hidden from IMAP...
    ALL_MAIL = '[Gmail]/All Mail'
  r, d = imapconn.select(ALL_MAIL, readonly=True)
  if r == 'NO':
    print "Error: Cannot select the Gmail \"All Mail\" folder. Please make sure it is not hidden from IMAP."
    sys.exit(3)
  
  # BACKUP #
  if options.action == 'backup':
    imapconn.select(ALL_MAIL, readonly=True)
    messages_to_process = getMessagesToBackupList(imapconn, options.gmail_search)
    backup_path = options.folder
    if not os.path.isdir(backup_path):
      os.mkdir(backup_path)
    messages_to_backup = []
    #Determine which messages from the search we haven't processed before.
    print "GYB needs to examine %s messages" % len(messages_to_process)
    for message_num in messages_to_process:
      if newDB:
        # short circuit the db and filesystem checks to save unnecessary DB and Disk IO
        messages_to_backup.append(message_num)
        continue
      if message_is_backed_up(message_num, sqlcur, sqlconn, options.folder):
        continue
      else:
        messages_to_backup.append(message_num)
    print "GYB already has a backup of %s messages" % (len(messages_to_process) - len(messages_to_backup))
    backup_count = len(messages_to_backup)
    print "GYB needs to backup %s messages" % backup_count
    messages_at_once = 100
    backed_up_messages = 0
    for working_messages in batch(messages_to_backup, messages_at_once):
      #Save message content
      batch_string = ','.join(working_messages)
      bad_count = 0
      while True:
        try:
          r, d = imapconn.uid('FETCH', batch_string, '(X-GM-LABELS INTERNALDATE FLAGS BODY.PEEK[])')
          if r != 'OK':
            bad_count = bad_count + 1
            if bad_count > 7:
              print "Error: failed to retrieve messages."
              sys.exit(5)
            sleep_time = math.pow(2, bad_count)
            sys.stdout.write("\nServer responded with %s, will retry in %s seconds" % (r, str(sleep_time)))
            time.sleep(sleep_time) # sleep 2 seconds, then 4, 8, 16, 32, 64, 128
            imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email, options.two_legged), options.debug)
            imapconn.select(ALL_MAIL, readonly=True)
            continue
          break
        except imaplib.IMAP4.abort:
          print 'imaplib.abort error, retrying...'
          imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email, options.two_legged), options.debug)
          imapconn.select(ALL_MAIL, readonly=True)
        except socket.error:
          print 'socket.error, retrying...'
          imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email, options.two_legged), options.debug)
          imapconn.select(ALL_MAIL, readonly=True)
      for x in d:
        if x[0] == ')':
          continue
        try:
          full_message = x[1]
          everything_else_string = x[0]
        except IndexError:
          print "skipped '%s'" % x[0]
          continue
        search_results = re.search('^[0-9]* \(X-GM-LABELS \((.*)\) UID ([0-9]*) (INTERNALDATE \".*\") (FLAGS \(.*\))', everything_else_string)
        labels = shlex.split(search_results.group(1).replace('\\', '\\\\'))
        message_num = search_results.group(2)
        message_date_string = search_results.group(3)
        message_flags_string = search_results.group(4)
        message_date = imaplib.Internaldate2tuple(message_date_string)
        time_seconds_since_epoch = time.mktime(message_date)
        message_internal_datetime = datetime.datetime.fromtimestamp(time_seconds_since_epoch)
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
        sqlcur.execute("INSERT INTO messages (message_num, message_filename, message_to, message_from, message_subject, message_internaldate) VALUES (?, ?, ?, ?, ?, ?)", (message_num, message_rel_filename, message_to, message_from, message_subj, message_internal_datetime))
        for label in labels:
          sqlcur.execute("INSERT INTO labels (message_num, label) VALUES (?, ?)", (message_num, label))
        for flag in message_flags:
          sqlcur.execute("INSERT INTO flags (message_num, flag) VALUES (?, ?)", (message_num, flag))

      sqlconn.commit()
      if backed_up_messages+messages_at_once < backup_count:
        backed_up_messages = backed_up_messages + messages_at_once
      else:
        backed_up_messages = backup_count
      restart_line()
      sys.stdout.write("backed up %s of %s messages" % (backed_up_messages, backup_count))
      sys.stdout.flush()
    print "\n"

  
  # RESTORE #
  elif options.action == 'restore':
    imapconn.select(ALL_MAIL)  # read/write!
    messages_to_restore = sqlcur.execute('SELECT message_num, message_internaldate, message_filename FROM messages') # All messages
    messages_to_restore_results = sqlcur.fetchall()
    restore_count = len(messages_to_restore_results)
    current = 1
    for x in messages_to_restore_results:
      restart_line()
      sys.stdout.write("restoring message %s of %s" % (current, restore_count))
      sys.stdout.flush()
      message_num = x[0]
      message_internaldate = x[1]
      message_internaldate_seconds = time.mktime(message_internaldate.timetuple())
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
          r, d = imapconn.append(ALL_MAIL, flags_string, message_internaldate_seconds, full_message)
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
          imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email, options.two_legged), options.debug)
          imapconn.select(ALL_MAIL)
        except socket.error:
          print 'socket.error, retrying...'
          imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email, options.two_legged), options.debug)
          imapconn.select(ALL_MAIL)
      current = current + 1
  
  # ESTIMATE #
  elif options.action == 'estimate':
    imapconn.select(ALL_MAIL, readonly=True)
    messages_to_process = getMessagesToBackupList(imapconn, options.gmail_search)
    messages_to_estimate = []
    #if we have a sqlcur , we'll compare messages to the db
    #otherwise just estimate everything
    for message_num in messages_to_process:
      try:
        sqlcur
        if message_is_backed_up(message_num, sqlcur, sqlconn, options.folder):
          continue
        else:
          messages_to_estimate.append(message_num)
      except NameError:
        messages_to_estimate.append(message_num)
    estimate_count = len(messages_to_estimate)
    total_size = float(0)
    list_position = 0
    messages_at_once = 10000
    loop_count = 0
    print "Messages to estimate: %s" % estimate_count
    while list_position < len(messages_to_estimate)-1:
      loop_count = loop_count + 1
      if (list_position+messages_at_once) < len(messages_to_estimate):
        working_messages = messages_to_estimate[list_position:list_position+messages_at_once-1]
        list_position = list_position+messages_at_once
      else:
        working_messages = messages_to_estimate[list_position:len(messages_to_estimate)-1]
        list_position = len(messages_to_estimate)-1
      messages_size = get_message_size(imapconn, working_messages)
      total_size = total_size + messages_size
      if total_size > 1048576:
        math_size = total_size/1048576
        print_size = "%.2fM" % math_size
      elif total_size > 1024:
        math_size = total_size/1024
        print_size = "%.2fK" % math_size
      else:
	    print_size = "%.2fb" % total_size
      restart_line()
      sys.stdout.write('                                                            ')
      restart_line()
      sys.stdout.write("Messages estimated: %s  Estimated size: %s" % (loop_count+list_position, print_size))
      sys.stdout.flush()
      time.sleep(1)
    print ""
  try:
    sqlconn.close()
  except NameError:
    pass
  imapconn.logout()
  
if __name__ == '__main__':
  try:
    main(sys.argv)
  except KeyboardInterrupt:
    try:
      sqlconn.commit()
      sqlconn.close()
    except NameError:
      pass
    sys.exit(4)