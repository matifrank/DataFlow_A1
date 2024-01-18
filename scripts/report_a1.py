from modules.utils import *
import logging
import os      


scriptDir = os.path.dirname(os.path.realpath(__file__)) + os.sep
logging.basicConfig(filename='scripts/logs/report-a1.log', level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%d/%m/%Y %H:%M:%S')

def load_data(csv_path):
    try:
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        logging.error(f"Error loading CSV file '{csv_path}': {e}")
        # Log the exception traceback
        logging.exception(e)
        return None

def main():
    try:
        todayDate = datetime.now().date()
        firstDateOfMonth = todayDate.replace(day=1)
        nextMonth = firstDateOfMonth.replace(month=firstDateOfMonth.month % 12 + 1, day=1)
        lastDateOfMonth = nextMonth - timedelta(days=1)
        start = (firstDateOfMonth - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d')
        end = (firstDateOfMonth - timedelta(days=1)).strftime('%Y-%m-%d')

        with open('scripts/config/filespath.config.json', 'r') as f:
            configpath = json.load(f)

        csv_sessions = configpath['ROOT_PATH'] + '\\' + (configpath['FILEPATH_SESSIONS'] % (start, end))
        csv_leads = configpath['ROOT_PATH'] + '\\' + (configpath['FILEPATH_LEADS'] % (start, end))

        df_sessions = load_data(csv_sessions)
        df_leads = load_data(csv_leads)

        if df_sessions is not None and df_leads is not None:
            a1_report(df_sessions, df_leads, configpath, start, end)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        # Log the exception traceback
        logging.exception(e)

if __name__ == '__main__':
    main()