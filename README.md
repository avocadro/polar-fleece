# Polar Fleece

Personal repository for experimenting and sharing.
The original intention was for Snowflake to handle the ETL and database, and only Viz components requiring app orchestration.
However, due to restrictions on the trial Snowflake account, ETL has been set back and will also require app deployment.

## Using Miniconda
* Install Miniconda
* create an environment: conda create --name api_to_snowflake python=3.9
* activate the environment: conda activate api_to_snowflake
* install the dependencies: conda install requests pandas snowflake-connector-python
* Use it in VS Code: View/'Command Palette'/'Python: Select Interpreter' --> 'api_to_snowflake'
* Export dependencies for Docker: pip list --format=freeze > ETL/requirements.txt 

## Conventional Commits
* 'fix' is for bug patches (patch)
* 'feat' is for new features (minor)
* Appending '!' is for breaking changes (major)
