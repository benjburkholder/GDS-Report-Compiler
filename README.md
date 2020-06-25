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

#### Processing Stages
To allow for maximum customization of data manipulation during the data pull process, each step is broken out into it's own separate stage. These stages are defined in each datasource's "user" module (path: utils/cls/user) and are typically: rename, type, parse, post-processing. The way it works is each stage will update the dataframe then pass to the next step and so on.
	<br>
	<br>
	1. Rename: The rename step allows for custom renaming rules to be provided,  typically to align the pulled data with the destination table.<br><br>
	2. Type: The type step is for ensuring the pulled data's datatype matches the destination table. This step is done dynamically by pulling in the table schema outlined in core.py and applying the data type fields to the pulled data.<br><br>
	3. Parse: Right now, this step is used for any additional manipulation which wouldn't make sense adding into the other steps.<br><br>
	4. Post-Processing: This step allows the user to write and execute as many custom sql queries as needed on the respective reporting table. Although not explicitly enforced, it's important for organizational purposes to make sure only the current data reporting table is being updated.

#### Custom Logic Handling

#### Recommended Structure


#### Usage instructions