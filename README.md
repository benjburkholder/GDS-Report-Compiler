# GDS Report Compiler Project
A full-featured custom report building platform that can be easily cloned and tailored to meet the needs of any client.

## Features
#### Customizer Class Inheritance
Customization is implemented through children of the parent global Customizer class. All child classes must reside in the custom.py file, however helper files and other utilities may be added as needed.

#### Dynamic Table Creation
All table schema is pre-defined in core.py. During script execution, a check is done to ensure the marketing_data, reporting, source and lookup tables exist in the client's database. If not, the tables will be created using the schema available in core.py.
The only exception is the marketing_data table, this table's schema is dynamically generated using the unique columns from each of the active reporting tables.

#### Dynamic Source / Lookup Table Refresh
In an effort to cut down on Google Sheet API calls, source tables (typically account data which doesn't change often) are only refreshed
twice a month (1st & 15th). Lookup table data (location mapping) is still refreshed once during each daily run since this data is more prone to consistent change. 

#### Dynamic Ingest / Backfilter Handling
In order to eliminate the need to manually build physical ingest statements for each data source, the platform generates each ingest statement dynamically, looping 
through all of the reporting tables to build the statement and keep the order consistent for each data source. The data source reporting 
table is used to determine which columns should be set to NULL and which should not for the script in question.

#### Dynamic Custom Column Insertion
Sometimes columns need to be added to marketing_data which aren't part of a reporting table's schema, and they don't need to be included in ingest statements. In the GDSCompiler platform, when the marketing_data table is being created the platform will check the variable "custom_marketing_data_columns" in core.py and see if it is active with columns, if so these columns will always be appended to the end of the marketing_data table during creation so not to affect the ingest procedures. If variable is not active, it is skipped and marketing_data table created normally.

#### Processing Stages
To allow for maximum customization of data manipulation during the data pull process, each step is broken out into it's own separate stage. These stages are defined in each datasource's "user" module (path: utils/cls/user) and are typically: rename, type, parse, post-processing. The way it works is each stage will update the dataframe then pass to the next step and so on.<br><br>
	1. Rename: The rename step allows for custom renaming rules to be provided,  typically to align the pulled data with the destination table.<br><br>
	2. Type: The type step is for ensuring the pulled data's datatype matches the destination table. This step is done dynamically by pulling in the table schema outlined in core.py and applying the data type fields to the pulled data.<br><br>
	3. Parse: Right now, this step is used for any additional manipulation which wouldn't make sense adding into the other steps.<br><br>
	4. Post-Processing: This step allows the user to write and execute as many custom sql queries as needed on the respective reporting table. Although not explicitly enforced, it's important for organizational purposes to make sure only the current data reporting table is being updated.

#### Recommended Structure


### Usage instructions
This section will provide a walk-through on starting a project with GDSCompiler from start to finish.<br><br>

1. Navigate to https://github.com/linkmedia360/GDS-Report-Compiler. If you'd like to start with the existing stable release, select the "Master" branch. If you'd like to start with the latest and greatest working branch, select the "2020.x.x" branch. Then select "Download ZIP" from the "Clone" button.
2. Unzip the project and rename the project directory to the year+month combination of when launch is anticipated (e.g. 202007).
3. Create the "/secrets" directory in the root of the project and either port over the necessary credential files, or commission them if the client is net new. Depending on the data sources currently in use, this typically involves a service account cred, Oauth cred and YAML file.
4. Create virtual environment and install dependencies. For lm360 packages, you can either leverage the lmpy tool to install, or simply install directly using the "pip install…" command located in each package's README.
5. Open core.py, this is where the bulk of all updates will be made. First create the connection to the client's database in postgres, if client is new, create in postgres first. At the bottom of core.py, enter the client's database name in the "DATABASE" key in the "db" variable.
6. Next enter the client's name in the "client" variable in camel case, this is used to match the secrets cred file names.
7. Next is to activate and alter the table schema for the tables to be created, this can all be found in core.py under the variable "CONFIGURATION_WORKBOOK". Here can be found 3 different types of table: lookup, source, reporting. Lookup tables are used for location mapping, source tables are typically for storing account information for each data source, and reporting tables are the tables actually storing the data which gets moved to "marketing_data" during ingest.
Most cases, you won't need to update the schema much at all since it's already configured for most all project needs. All you will need to do is go down the list of table schemas and toggle the "active" value to True for any table which needs to be active for this client. This boolean is what determines if a table should be created or not.
8. For both lookup and source tables, you'll need to ensure the "sheet" name of each schema section matches correctly with the client's configuration sheet. This is so that the correct tab is selected from the config sheet and data ingested into the correct table mentioned in the schema. The high-level variable "config_sheet_name" will also need to be updated with the name of the client's config sheet so the project knows what to connect to.
9. Once this is done, make sure to add the service creds as a user in the client's config sheet in google sheets.
10. Next open any data source script in the project, and run it. You should see a process which is creating the tables, and refreshing the lookup and source table data.

