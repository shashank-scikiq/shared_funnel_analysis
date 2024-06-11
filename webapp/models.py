import urls
import pandas as pd

from sqlalchemy import create_engine
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

# import logging

# Global ENVs
# ====================================================================================================

row_limiter = 25000


# Functions
# ====================================================================================================

def connect_pg_src():   
  try:
    pg_conn = create_engine(f"postgresql+psycopg://{urls.tgt_user}:{urls.tgt_pwd}@{urls.tgt_host}:{urls.tgt_port}/{urls.tgt_db}")
  except Exception as e:
    print(e)
    print("Unable to connect to the Target Datbase. Falling back to File System.")
  else:
    print("Connected to the Database. Proceeding.")
    return pg_conn

def connect_src_athena():
  try:
      pandas_ath_cursor = connect(
          aws_access_key_id=urls.aws_access_key,
          aws_secret_access_key=urls.aws_secret_key,
          s3_staging_dir=urls.aws_staging_dir,
          region_name=urls.aws_region,
          schema_name=urls.aws_schema,
          cursor_class=PandasCursor).cursor()
  except Exception as e:
      print(e.args[0])
      print("Falling to file system if available." )
  else:
      print("Connected to Athena Database using pandas' connector.")
      return pandas_ath_cursor
  
def run_sql_athena(query: str, db_cursor, size: int=0):
	df = pd.DataFrame()
	try:
		print("Executing the query on AWS Athena.")
		if size != 0:
			df = db_cursor.execute(query).fetchmany(size).as_pandas()
		else:
			df = db_cursor.execute(query).as_pandas()
	except Exception as e:
		print(e.args[0])
		return
	else:
		print("Successfully executed the query.")

	return df

def write_to_pg(schema_name: str, table_name: str, db_conn:str, df_tgt: pd.DataFrame,chunk: int = 50000):
  try:
    df_tgt.to_sql(con=db_conn,
              chunksize=chunk,if_exists="replace", 
                            name=table_name, schema=schema_name)
  except Exception as e:
    print(e.args[0])
    return False
  else:
    print("Write Successful")
    return True

def pair_mismatch(tgt_df: pd.DataFrame, src: str, tgt: str, srch_col:str = "transaction_id") -> list[str]:
  """
  Returns a list of Transaction IDs where there is a pair mismatch for the given pair. 
  df = Pandas Dataframe. This should be a crosstab format dataframe. 
  src = Source Column. 
  tgt = Target Column.
  srch_col = Column to return. Default is transaction_id.
    """
  list_missing = list(tgt_df[tgt_df[src] != tgt_df[tgt]][srch_col])
  return list_missing

def mulitple_calls(tgt_df: pd.DataFrame, col_to_srch: str, srch_col: str = "transaction_id", occur: int = 1) -> list[str]:
  """
  Returns a list of Transaction IDs if they have multiple instances.   
  df = Pandas Dataframe. 
  col_to_srch = The column to perform the search operation in.
  srch_col = The column to return. The default value is transaction_id. 
  occur = The number of occurances to search. Default value is 1. This will always search '> occur' instances. 
    """
  return list(tgt_df[tgt_df[col_to_srch] > occur][srch_col])
