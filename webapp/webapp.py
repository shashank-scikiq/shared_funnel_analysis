import pandas as pd
import numpy as np
import os
import sys
# import logging
from dotenv import load_dotenv

lib_loc = "D:\\ONDC_Project\\Funnel_Analysis\\webapp\\"

if os.path.exists(lib_loc):
  try:
    sys.path.append("D:\\ONDC_Project\\Funnel_Analysis\\webapp\\")
  except:
    print(FileNotFoundError)
  else:
    print("Library location loaded.")

import urls
import models

try:
  load_dotenv(".env")
except:
  print("Unable to load the Envitonment Variables. Exiting.")
  sys.exit()
else:
  print("Environment Variables loaded. Proceeding.")



tgt_conn = models.connect_pg_src()