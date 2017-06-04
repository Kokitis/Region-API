import os, sys
#Get the folder that contains this file, and add the parent folder to PATH.
#Assumes pytools is in that folder.
print("Importing the parent directory...")
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))