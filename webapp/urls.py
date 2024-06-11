# import logging
import sys
import os
from dotenv import load_dotenv

lib_loc = "D:\\ONDC_Project\\Funnel_Analysis\\webapp\\"

if os.path.exists(lib_loc):
  try:
    sys.path.append("D:\\ONDC_Project\\Funnel_Analysis\\webapp\\")
  except:
    print(FileNotFoundError)
  else:
    print("Library location loaded.")
 


try:
  load_dotenv(".env")
except:
  print("Unable to load the Envitonment Variables. Exiting.")
  sys.exit()
else:
  print("Environment Variables loaded. Proceeding.")


# AWS ENVs
# ==================================================================

aws_schema = os.getenv('SCHEMA_NAME')
aws_tbl = os.getenv('TABLE_NAME')
aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')
aws_region = os.getenv('AWS_REGION')
aws_staging_dir = os.getenv('S3_STAGING_DIR')
aws_db = os.getenv('DATABASE_NAME')


# TGT DB Envs
# ================================================================== 

tgt_user = os.getenv("POSTGRES_USER")
tgt_pwd = os.getenv("POSTGRES_PASSWORD")
tgt_host = os.getenv("POSTGRES_HOST")
tgt_port = os.getenv("POSTGRES_PORT") 
tgt_schema = os.getenv("POSTGRES_SCHEMA")
tgt_db = os.getenv("POSTGRES_DB")

