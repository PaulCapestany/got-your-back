from Tkinter import *
import tkFileDialog
import os

master = Tk()
master.title("Got Your Back: Gmail Backup")
master.wm_iconbitmap('gyb.ico')

action = StringVar()
debug = IntVar()

def askdirectory():
  return tkFileDialog.askdirectory(parent=master,initialdir="/",title='Please select a backup folder')

def askopenfile():
  return tkFileDialog.askopenfile(parent=master,initialdir="/", title='Please select your two-legged OAuth File')

def asksaveasfile():
  return tkFileDialog.asksaveasfile(parent=master,initialdir="/", title='Please select your two-legged OAuth File')

def backup_selected():
  restore_label.delete(0, END)
  restore_label.config(state=DISABLED)
  gmail_search.config(state=NORMAL)

def restore_selected():
  restore_label.config(state=NORMAL)
  gmail_search.delete(0, END)
  gmail_search.config(state=DISABLED)

def estimate_selected():
  restore_label.delete(0, END)
  restore_label.config(state=DISABLED)
  gmail_search.config(state=NORMAL)

def check_email():
  email_address = email.get().lower()
  if email_address.find('@gmail.com') != -1 or email_address.find('@googlemail.com') != -1:
    two_legged.config(state=DISABLED)
  else:
    two_legged.config(state=NORMAL)
  return True

def execute_gyb():
  gmail_search_argument = ' '
  if gmail_search.get() != '' and gmail_search.get() != None:
    gmail_search_argument = ' -s "%s"' % gmail_search.get()
  debug_argument = ' '
  if debug.get():
    debug_argument = ' --debug'
  folder_argument = ' '
  gyb_command = str('cmd /k gyb.exe -a %s -e %s%s%s' % (action.get(), email.get(), gmail_search_argument, debug_argument))
  print gyb_command
  os.system(gyb_command)

Label (text='Action to perform:').pack(side=TOP,padx=10,pady=10)
estimate_button = Radiobutton(master, text="Estimate", variable=action, value='estimate', command=estimate_selected)
estimate_button.pack(anchor=W)
backup_button = Radiobutton(master, text="Backup", variable=action, value='backup', command=backup_selected)
backup_button.pack(anchor=W)
restore_button = Radiobutton(master, text="Restore", variable=action, value='restore', command=restore_selected)
restore_button.pack(anchor=W)
backup_button.select()

Label (text='Email address:').pack(side=TOP,padx=10,pady=10)
email = Entry(master, width=25, validate="all", validatecommand=check_email)
email.pack(side=TOP,padx=10,pady=10)
Label (text='Two-Legged OAuth (Google Apps Admins Only):').pack(side=TOP,padx=10,pady=10)
two_legged = Button(master, text='Two-Legged File', command=asksaveasfile)
two_legged.pack(side=TOP,padx=10,pady=10)
two_legged.config(state=DISABLED)
Label (text='Optional Gmail Search String:').pack(side=TOP,padx=10,pady=10)
gmail_search = Entry(master, width=25)
gmail_search.pack(side=TOP,padx=10,pady=10)
folder = Button(master, text='Choose Backup Directory', command=askdirectory)
folder.pack(side=TOP,padx=10,pady=10)
Checkbutton(master, text="Debug", variable=debug).pack()
Label (text='Restore Only Option: Label all restored messages:').pack(side=TOP,padx=10,pady=10)
restore_label = Entry(master, width=25, state=DISABLED)
restore_label.pack(side=TOP,padx=10,pady=10)
execute = Button(master, text='Execute GYB', command=execute_gyb)
execute.pack(side=TOP,padx=10,pady=10)

mainloop()