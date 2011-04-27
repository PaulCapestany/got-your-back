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

__author__ = 'jlee@pbu.edu (Jay Lee)'
__version__ = '0.01'
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
import tarfile
import StringIO
import socket

import atom.http_core
import gdata.gauth
import gdata.service
import gdata.auth

import gimaplib

def SetupOptionParser():
  # Usage message is the module's docstring.
  parser = OptionParser(usage=__doc__)
  parser.add_option('-e', '--email',
    dest='email',
    help='Full email address of user to backup')
#  parser.add_option('-a', '--action',
#    dest='action',
#    default='backup',
#    help='Optional: Action to perform, backup (default) or restore')
  parser.add_option('-s', '--search',
    dest='gmail_search',
    help='Optional: Gmail search to perform, matching messages are backed up',
    default='subject:-NNN-Str1ng-th4t-n3v3r-sh0uld-exist-1n-th3-r3a1-w0r1d-NNN')  # should return all Gmail messages
#  parser.add_option('-f', '--file',
#    dest='filename',
#    help='Optional: file name to use for backup or restore. Default is gmail.tar.bz2',
#    default='gmail')
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

def requestOAuthAccess(email):
  domain = email[email.find('@')+1:]
  scopes = ['https://mail.google.com/']
  s = gdata.service.GDataService()
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
      exit(5)
    else:
      print 'Error: %s' % e
  if domain.lower() != 'gmail.com':
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
    exit(4)
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

def main(argv):
  options_parser = SetupOptionParser()
  (options, args) = options_parser.parse_args()
  if not options.email:
    options_parser.print_help()
    print "ERROR: --email is required."
    return
  key, secret = getOAuthFromConfigFile(options.email)
  if not key:
    key, secret = requestOAuthAccess(options.email)
  imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email)) # dynamically generate the xoauth_string since they expire after 10 minutes
  imapconn.select('[Gmail]/All Mail', readonly=True)
  #if options.action == 'backup':
  if True:
    messages_to_backup = gimaplib.GImapSearch(imapconn, options.gmail_search)
    count = len(messages_to_backup)
    current = 1
    backup_path = '%s-backup' % options.email
    if not os.path.isdir(backup_path):
      os.mkdir(backup_path)
    for message_num in messages_to_backup:
          base_filename = os.path.join(backup_path, str(message_num))
          message_filename = base_filename+'.eml'
          label_filename = base_filename+'.labels'
          flags_filename = base_filename+'.flags'
          
          if not os.path.isfile(message_filename):
            print "backing up message %s of %s" % (current, count) 
            #Save message content
            while True:
              try:
                full_message = imapconn.uid('FETCH', message_num, '(RFC822)')[1][0][1]
                break
              except imaplib.IMAP4.abort:
                print 'imaplib.abort error, retrying...'
                imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email))
              except socket.error:
                print 'socket.error, retrying...'
                imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email))
            f = open(message_filename, 'wb')
            f.write(full_message)
            f.close()

          if not os.path.isfile(label_filename):          
            #Save message labels
            while True:
              try:
                labels = gimaplib.GImapGetMessageLabels(imapconn, message_num)
                break
              except imaplib.IMAP4.abort:
                print 'imaplib.abort error, retrying...'
                imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email))
              except socket.error:
                print 'socket.error, retrying...'
                imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email))
            f = open(label_filename, 'wb')
            for label in labels:
              f.write("%s\n" % label)
            f.close()

          #Save message flags
          if not os.path.isfile(flags_filename):
            while True:
              try:
                flags = imapconn.uid('FETCH', message_num, '(FLAGS)')[1][0]
                #print flags
                flags = imaplib.ParseFlags(flags)
                #print flags
                break
              except imaplib.IMAP4.abort:
                print 'imaplib.abort error, retrying...'
                imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email))
              except socket.error:
                print 'socket.error, retrying...'
                imapconn = gimaplib.ImapConnect(generateXOAuthString(key, secret, options.email))
            f = open(flags_filename, 'wb')
            for flag in flags:
              f.write("%s\n" % flag)
            f.close()

          current = current + 1
    imapconn.logout()
  
if __name__ == '__main__':
  main(sys.argv)
