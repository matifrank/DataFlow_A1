# Importing all functions from the `utils.py` module 
from modules.utils import *
import logging
import os
import mysql.connector
from mysql.connector import Error

scriptDir = os.path.dirname(os.path.realpath(__file__)) + os.sep
logging.basicConfig(filename='scripts/logs/sql-leads-a1.log',level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

# Read the JSON config file
with open('scripts/config/query.json') as f:
    config_json = json.load(f)

# Extract the queries
queries = config_json['A1queries']

# read the configuration file to get credentials and db access
config = configparser.ConfigParser()
config.read('scripts/credentials/db_access.ini')

# Setup MySQL connection
conn = mysql.connector.connect(
    host=config['DB']['host'],
    database=config['DB']['db'],
    user=config['readonly']['username'],
    password=config['readonly']['pass']
)
 
# Calculate the first and last day of the previous month
todayDate = datetime.now().date()
firstDateOfMonth = todayDate.replace(day=1)
nextMonth = firstDateOfMonth.replace(month=firstDateOfMonth.month % 12 + 1, day=1)
lastDateOfMonth = nextMonth - timedelta(days=1)
start = (firstDateOfMonth - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
end = (firstDateOfMonth - timedelta(days=1)).strftime('%Y-%m-%d')


def main():
    logging.info('Generating SQL Cursor')
    cursor = conn.cursor(dictionary=True)
    dfs = []

    try:
        logging.info('Executing queries')
        for query_info in queries:
            query_name, query = query_info.popitem()
            cursor.execute("SET @start_date = %s", (start,))
            cursor.execute("SET @end_date = %s", (end,))
            cursor.execute(query)
            result = cursor.fetchall()
            logging.info('Generating dataframe')
            df = pd.DataFrame(result)
            time.sleep(1)
            dfs.append(df)

        conn.commit()

    except mysql.connector.Error as err:
        logging.error("MySQL Error: {}".format(err))
        # Log the exception traceback
        logging.exception(err)

    except Exception as e:
        logging.error("An unexpected error occurred: {}".format(e))
        # Log the exception traceback
        logging.exception(e)

    finally:
        cursor.close()
        conn.close()
        
    # Access each DataFrame using indices 
    for i, df in enumerate(dfs):
        globals()[f'df{i}'] = df
    # Merge the DataFrames
    df_merged = df0.copy()  # Initialize with df0

    for i in range(1, len(dfs)):
        df_i = globals()[f'df{i}']
        df_i = df_i.drop(columns=['yearMonth'])  # Drop the yearMonth column
        df_merged = pd.merge(df_merged, df_i, on='site_id', how='left').fillna(0)
    
    # Load the new config from a JSON file
    with open('scripts/config/filespath.config.json', 'r') as f:
        configpath = json.load(f)
    
    a1_leadsreport(df_merged, configpath, start, end)

if __name__ == '__main__':
    main()