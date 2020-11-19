# GDS Report Compiler Project
A full-featured custom report building platform that can be easily cloned and tailored to meet the needs of any client.

## Features

#### Easy Project Initialization
After generating a fresh clone of the GRC Platform template, execute the file sys_initialize_workbook.py. This process requires user input:
<ol>
<li><u>Source & Lookup Table Activation:</u> User will be asked to enter 'y' for each data source prompted. The associated lookup and source tables for these data sources will then be activated automatically. If not to include, just hit 'enter' to move onto the next one.</li>
<li><u>Config Sheet Creation:</u>User will be prompted to enter the client's database name and vertical type. Browser will then open automatically to the generated config sheet, the user will copy the key in the url and paste into the prompt.</li>
</ol>

Once this process is completed, two new configured files (app.json & workbook.json) should be present in /conf/stored/ where the project will reference going forward.

#### Dynamic Table Creation
All table schema is pre-defined in conf/stored/workbook.json (generated during project initialization). During script execution, a check is done to ensure the marketing_data, reporting, source and lookup tables exist in the client's database. If not, the tables will be created using the schema available in the json file.
The only exception is the marketing_data table, this table's schema is dynamically generated using the unique columns from each of the active reporting tables.

#### Dynamic Source / Lookup Table Refresh
In an effort to cut down on Google Sheet API calls, source tables (typically account data which doesn't change often) are only refreshed
twice a month (1st & 15th). Lookup table data (location mapping) is still refreshed once during each daily run since this data is more prone to consistent change. 

#### Dynamic Ingest / Backfilter Handling
In order to eliminate the need to manually build and update ingest statements for each data source, the platform generates each ingest statement dynamically during runtime, looping 
through all of the reporting tables to build the statement and keep the order consistent for each data source. The data source reporting 
table is used to determine which columns should be set to NULL and which should not for the script in question.

#### Dynamic Custom Column Insertion
Sometimes columns need to be added to marketing_data which aren't part of a reporting table's schema, and they don't need to be included in ingest statements. In the GDSCompiler platform, when the marketing_data table is being created the platform will check the /conf/stored/custom_marketing_data_columns.json file to check if active and has columns, if so these columns will always be appended to the end of the marketing_data table during creation so not to affect the ingest procedures. If not active, it is skipped and marketing_data table created normally.

#### Custom Post-Processing
In order to give the user more control over the ETL process, custom sql query files can be created and placed in /utils/scripts/post_processing/user. In many cases the order of execution matters, logic is in place to order each data source's sql files based on the file's numeric prefix. Here is the correct format for sql file names:
<ul>
<li>E.g., "1_google_analytics_events_assign_medium.sql"</li>
</ul>

The number at the beginning denotes the order in which the queries are executed, 1 to n essentially. Each data sources group needs to be separately numbered 1 to n.
The name of the file needs to include, after the numeric prefix, the data source name as it appears in the platform (this is how the queries are found for the post-processing stage of each data source). Anything after these two required pieces should just be a basic descriptor of what the query does.

### Control Over Script Workflow
Located in /conf/stored/workflow.json, all available data sources are listed with the ability to toggle (boolean 0 and 1) which steps should be included and which can be skipped during script execution.

The 'args' values are passed as command line arguments during the script run.

 ```{
    "name": "google_analytics_traffic",
    "active": 0,
    "args": {
      "pull": 1,
      "ingest": 1,
      "backfilter": 1,
      "expedited": 1
    }
```
<ol>
<li><u>Name:</u> script name without file extension.</li>
<li><u>Active:</u> set to 0 (False) by default, toggle to 1 to be included in script execution.</li>
<li><u>Pull:</u> set to 1 (True) by default, this indicates data should be pulled for this data source.</li>
<li><u>Ingest:</u> set to 1 (True) by default, this indicates data in reporting table needs to be moved to marketing_data table via dynamic ingest procedure.</li>
<li><u>Backfilter:</u> set to 1 (True) by default, this indicates pulled data present in table should be backfiltered (mapped) based on lookup tables.</li>
<li><u>Expedited:</u> typically only first data source in list is set to 0, rest are set to 1. If 1, the table check and refresh step is skipped, if 0 then the check / creation and refresh is performed. This only should happen once, so the first data source is typically set to 0 to handle first and skip during all of the rest.</li>
</ol>

## Internal Packages
pip install git+https://linkmedia360:{AGENCY_PASSWORD}@github.com/Linkmedia-360/google-ads-py.git
<br><br>
pip install git+https://linkmedia360:{AGENCY_PASSWORD}@github.com/Linkmedia-360/google-my-business-py.git
<br><br>
pip install git+https://linkmedia360:{AGENCY_PASSWORD}@github.com/Linkmedia-360/dialogtech-py.git
<br><br>
pip install git+https://linkmedia360:{AGENCY_PASSWORD}@github.com/Linkmedia-360/moz-py.git
<br><br>
pip install git+https://linkmedia360:{AGENCY_PASSWORD}@github.com/Linkmedia-360/google-analytics-py.git
<br><br>
pip install git+https://linkmedia360:{AGENCY_PASSWORD}@github.com/Linkmedia-360/google-search-console-py.git
