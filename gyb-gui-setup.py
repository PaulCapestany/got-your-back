from distutils.core import setup
import py2exe, sys, os

sys.argv.append('py2exe')

setup(
  windows = ['gyb-gui.py'],

  zipfile = None,
  options = {'py2exe': 
              {'optimize': 2}
            }
  )