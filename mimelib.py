# Functions to work with the MIME content of messages

import email
import re

def ExtractAttachment(msg, file_extension, file_type, replace_string):
  '''
  Args:
    message: string, an email message
	  file_extension: string, the message extension to look for (.pdf, .doc, .xls, etc)
	  file_type: string, the mime file type to look for (application/pdf, application/msword, application/vnd.ms-excel, etc)
    replace_string: string, the string that will be inserted in place of the attachment
                    has variables %(filename) and %(url)
	
  Returns:
    data, the extracted binary file
  '''
  match_fn = re.compile(r'(\.%s)$' % file_extension)
  msg_fn = msg.get_filename()
  msg_ct = msg.get_content_type()
  if msg_ct.lower() == file_type.lower() or (msg_fn and match_fn.search(msg_fn)):
    params = msg.get_params()[1:]
    params = ', '.join([ '='.join(p) for p in params ])
    data = msg.get_payload(decode=1)
    return data, msg_fn
  else:
    if msg.is_multipart():
      payload = [ ExtractAttachment(x, file_extension, file_type, replace_string) for x in msg.get_payload() ]
      msg.set_payload(payload)
    