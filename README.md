# DataFlow_A1

Description:

End-to-end data pipeline project designed for efficient processing and transformation of diverse datasets. 
This comprehensive solution integrates Python scripts for flexible data manipulation, AWS Lambda for ETL processes. With additional support for SQL, API connections, analytics, and Docker, ensures a versatile data pipeline for handling various data sources and scenarios.

Key Features:

Python Scripts: Develop custom and adaptable data transformations using Python.
AWS Lambda: Leverage AWS Glue for scalable and efficient ETL processes. 
SQL Integration: Seamlessly incorporate SQL queries for data manipulation and analysis.. 
Docker: Simplify deployment and management of data pipeline.
API Connections: Connect to external APIs for data retrieval and integration. 

Project Structure:
/scripts: Python scripts for data transformations and manipulations. 
          main.py execute in order the scripts to obtain the final csv output "reporte_a1.."
/config: need to adpat roothpaths for use in AWS Lambda functions for ETL processes.
/docs: example files generated after extract and data transformation.


|-- Dockerfile
|-- main.py
|-- README.md
|-- requirements.txt
|-- scripts
  |-- ga_sitesession.py
  |-- leads_a1.py
  |-- report_a1.py
  |-- modules
    |-- utils.py
  |-- config
    |-- analytics.config.json
    |-- filespath.config.json
    |-- query.json
  |-- credentials
    |-- credentials.json
    |-- credentials_dummy.json
    |-- db_access.ini
    |-- db_access_dummy.ini
  |-- logs
    |-- analytics-site-sessions.log
    |-- report-a1.log
    |-- sql-leads-a1.log
|-- docs
  |-- analytics-sites-sessions .csv
  |-- leads_a1 .csv
  |-- reporte_a1 .csv
|-- tests
  |-- test_utils.py
|-- workflows
  |-- ci.yml
