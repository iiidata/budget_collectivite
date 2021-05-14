import os
import sys

from sqlalchemy import create_engine

if getattr(sys, 'frozen', False):
    basedir = os.path.dirname(sys.executable)
elif __file__:
    basedir = os.path.abspath(os.path.join(os.path.dirname(__file__)))

engine = create_engine('sqlite:///dwh.db', echo=False)
sql_scripts_folder = os.path.join(basedir, "scripts_database")