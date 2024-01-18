# utils.py
import numpy as np
import pandas as pd
import json
import configparser
import datetime
from datetime import datetime, timedelta
import os
import time
import shutil
import logging
import mysql.connector
from mysql.connector import Error
import csv 
from google.analytics.data_v1beta import BetaAnalyticsDataClient 
from google.analytics.data_v1beta.types import (
    BatchRunReportsRequest,
    RunReportRequest,
    Filter,
    FilterExpression,
    Dimension,
    Metric,
    DateRange,
    OrderBy,
)


# Format batch responses into df
def df_report(resulting_response):
    # Create a DataFrame to store the response data
    table_data = []

    # Process each response and add data to the DataFrame
    for site_id, report_name, response in resulting_response:
        for row in response.rows:
            row_data = [site_id, report_name]
            for dim_value in row.dimension_values:
                row_data.append(dim_value.value)
            for metric_value in row.metric_values:
                row_data.append(metric_value.value)
            table_data.append(row_data)

    # Define column names
    columns = ['IDSITE', 'ORIGEN']
    for dim in resulting_response[0][2].dimension_headers:
        columns.append(dim.name)
    for metric in resulting_response[0][2].metric_headers:
        columns.append(metric.name)

    # Create the DataFrame
    df = pd.DataFrame(table_data, columns=columns)
    return df


#  custom batch analytic report by parameters in config file
def batch_report(config, client, startdate, endate):
    """Runs batch reports on a Google Analytics 4 property for each site in the config."""  
    responses = [] 

    for report in config['REPORTS']:
        for site in report['SITES']:
            property_id = site['PROPID']
            site_id = site['IDSITE']
            report_name = report['NAME']

            dimensions = [Dimension(name=dim['name']) for dim in report['DIMENSIONS']]

            metrics = [Metric(name=met['name']) for met in report['METRICS']]

            date_range = DateRange(
            start_date=startdate,
            end_date=endate,)

            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=dimensions,
                metrics=metrics,
                date_ranges=[date_range],
            )
            
            # Add the filter condition if present in the report
            if 'FILTER' in report:
                filter_field_name = report['FILTER'][0]['filter']['fieldName']
                filter_match_type = report['FILTER'][0]['filter']['stringFilter']['matchType']
                filter_value = report['FILTER'][0]['filter']['stringFilter']['value']
                filter_expression = FilterExpression(
                    filter=Filter(
                        field_name=filter_field_name,
                        string_filter=Filter.StringFilter(
                            match_type=filter_match_type,
                            value=filter_value,
                        ),
                    ),
                )
                request.dimension_filter = filter_expression

            response = client.run_report(request=request)
            
            # Append the site ID, report name, and response as a tuple
            responses.append((site_id, report_name, response))

    return responses

# Format df response with calculations to A1 report
def a1_sessionsreport(df, config, startlastmonth, endlastmonth, bstartlastmonth, bendlastmonth):
        logging.info('Creating A1 sessions DataFrame')
        # Pivot the DataFrame with 'ORIGEN' as columns and  
        pivot_df = df.pivot(index=df.iloc[:, [0, 2]], columns=df.columns[1], values=df.columns[3]).fillna(0).astype(int).reset_index()
        pivot_df['SEO'] = pivot_df['TOTAL'] - pivot_df['GADS'] - pivot_df['LADS']- pivot_df['FB'] - pivot_df['EMAIL'] - pivot_df['EMC'] 
        pivot_df['PAID'] = pivot_df['TOTAL'] - pivot_df['SEO'] - pivot_df['EMAIL'] - pivot_df['EMC']
        pivot_df.iloc[:, 1]  = pd.to_datetime(pivot_df.iloc[:, 1] , format='%Y%m')#yearMonth
        pivot_df.iloc[:, 0] = pivot_df.iloc[:, 0].astype(int) #IDSITE 
        pivot_df = pivot_df.set_index(pivot_df.columns[1])  #set date index
        
        logging.info('Generating location files path..')
        
        file_path_delete = config['ROOT_PATH'] + '\\' + (config['FILEPATH'] % (bstartlastmonth, bendlastmonth))
        filepath = config['ROOT_PATH'] + '\\' + (config['FILEPATH'] % (startlastmonth, endlastmonth)) 
        
        try:
            os.makedirs(config['ROOT_PATH'] , exist_ok=True)

            logging.info('Removing file generated last month if it already exists (optional)..') 
            if bstartlastmonth != startlastmonth and os.path.exists(file_path_delete):
                os.remove(file_path_delete)

            logging.info('Exporting DataFrame to CSV into location..')  
            pivot_df.to_csv(filepath, index=False ) 

        except Exception as e:
            logging.error(f"Error while exporting A1 DataFrame to CSV: {str(e)}")
            #continue


# Format df with calculations to A1 report
def a1_leadsreport(df, config, startlastmonth, endlastmonth):
        logging.info('Creating A1 leads DataFrame')
        # Calculate columns 
        df['enviados'] = df['enviados'] + df['reprocesados']
        df['seo'] = df['total'] - df['paid'] - df['emc']- df['muex']
        df['seo_monetizados'] = df['monetizados'] - df['paid_monetizados'] - df['emc_monetizados']- df['muex_monetizados']
        df['seo_enviados'] = df['enviados'] - df['paid_enviados'] - df['emc_enviados'] - df['muex_enviados']
        df['fb'] =  df['paid'] - df['gads']
        df['fb_monetizados'] = df['paid_monetizados'] - df['gads_monetizados']
        df['fb_enviados'] = df['paid_enviados'] - df['gads_enviados'] 
        
        logging.info('Generating location files path..')
        
        filepath = config['ROOT_PATH'] + '\\' + (config['FILEPATH_LEADS'] % (startlastmonth, endlastmonth)) 
        
        try:
            logging.info('Exporting DataFrame to CSV into location..')  
            df.to_csv(filepath, index=False ) 

        except Exception as e:
            logging.error(f"Error while exporting A1 DataFrame to CSV: {str(e)}")

# Format df with calculations to A1 report
def a1_report(df, df2, config, startlastmonth, endlastmonth):
        logging.info('Creating A1 report DataFrame')
        # Calculate columns
        df = df.apply(pd.to_numeric, errors='coerce').astype('Int64').add_prefix('visitas_')
        df_a1 = df2.merge(right=df, left_on='site_id', right_on='visitas_IDSITE', how='left')
        df_a1['visitas_FB'] = df_a1['visitas_FB'] + df_a1['visitas_LADS']
        df_a1['visitas_EMAIL'] = df_a1['visitas_EMAIL'] + df_a1['visitas_EMC']
        # drop columns
        df_a1.drop(columns=['visitas_IDSITE', 'total','monetizados','enviados','paid','paid_monetizados','paid_enviados','visitas_LANDING','visitas_PAID','visitas_LADS','visitas_EMC','visitas_DSA'], inplace=True)
        # ROLA sites agregated results
        rola_sites = [2,6,7,8,9,10,15,16]
        rola_df = df_a1[df_a1['site_id'].isin(rola_sites)].groupby('yearMonth').sum().reset_index()
        rest_df = df_a1[~df_a1['site_id'].isin(rola_sites)]
        rola_df['sitio'] = 'ROLA_sitios'
        df_a1_final = pd.concat([rest_df, rola_df], ignore_index=True)
        # Define the desired column order
        desired_order = ['yearMonth','sitio','visitas_SEO','seo', 'seo_monetizados', 'seo_enviados','visitas_EMAIL','emc','emc_monetizados','emc_enviados','visitas_FB','fb','fb_monetizados','fb_enviados','visitas_GADS','gads', 'gads_monetizados', 'gads_enviados','muex','muex_monetizados','muex_enviados','visitas_TOTAL','reprocesados']
        df_a1_final = df_a1_final[desired_order]
        
        logging.info('Generating location files path..')
        
        filepath = config['ROOT_PATH'] + '\\' + (config['FILEPATH_A1'] % (startlastmonth, endlastmonth)) 
        
        try:
            logging.info('Exporting DataFrame to CSV into location..')  
            df_a1_final.to_csv(filepath, index=False ) 

        except Exception as e:
            logging.error(f"Error while exporting A1 DataFrame to CSV: {str(e)}")