#from google.analytics.data_v1beta import BetaAnalyticsDataClient 
# Importing all functions from the `utils.py` module
from modules.utils import *
import logging
import os

scriptDir = os.path.dirname(os.path.realpath(__file__)) + os.sep
logging.basicConfig(filename='scripts/logs/analytics-site-sessions.log',level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

# Load the new config from a JSON file or use the config dictionary directly
with open('scripts/config/analytics.config.json', 'r') as f:
    config = json.load(f)

# Initialize the analyticsreporting service client credentials 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "scripts/credentials/credentials.json"
client = BetaAnalyticsDataClient()

# Calculate the first and last day of the previous month 
todayDate = datetime.now().date()
firstDateOfMonth = todayDate.replace(day=1)
nextMonth = firstDateOfMonth.replace(month=firstDateOfMonth.month % 12 + 1, day=1)
lastDateOfMonth = nextMonth - timedelta(days=1)
previousMonth = firstDateOfMonth.replace(month=(firstDateOfMonth.month - 2) % 12 + 1, day=1)
start = (firstDateOfMonth - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
end = (firstDateOfMonth - timedelta(days=1)).strftime('%Y-%m-%d')
 # Calculate the previous previous month 
firstDateOfBBMonth = (firstDateOfMonth - timedelta(days=1)).replace(month=(previousMonth.month - 2) % 12 + 1, day=1)
lastDateOfBBMonth = (firstDateOfMonth - timedelta(days=1)).replace(month=(previousMonth.month - 1) % 12 + 1, day=1) - timedelta(days=1)
bbstart = firstDateOfBBMonth.strftime('%Y-%m-%d')
bbend = lastDateOfBBMonth.strftime('%Y-%m-%d')

def main():
    # process batch request to analytics
    resulting_response = batch_report(config, client,start,end)
 
    # formate into DataFrame and A1 report and export into  csv file by config
    df = df_report(resulting_response)
    df_sessions = a1_sessionsreport(df, config, start, end, bbstart, bbend)

if __name__ == '__main__':
	main()