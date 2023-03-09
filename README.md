# OraPost - Data Transfer from Oracle to Postgres 
This python script can read from Oracle source and import table into Postgres target.

## Requirements
Libraries to run the script is provided in __requirements.txt__
Accessing the Oracle databases further require a provider for the library to work.
Provider files can be accessed at: 
https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html

OCI version 19 has backwards compatibility from Oracle 19 to 11.2.

## Installation
* Most reliable way to install and run the script is to create a virtual env with conda using __requirements.txt__.
* OCI dll files are downloaded and put in the project folder.
* __app.spec__ file are modified depending on the dll file locations. OCI v19 is used for the example.
* __dist\config.json__ are modified using the database connection info.
* Then, __compile.bat__ can be run to generate executable.
* Compiled exe and related files are in dist folder.

## Usage
app.exe is run to run the app.
log file is generated in regards to collect logs about the app.
config.json is used to configure the target and sources.

