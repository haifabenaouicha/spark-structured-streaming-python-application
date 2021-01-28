# A sample python spark structured streaming application deployed on Databricks
Below are steps to build the project and run it using Databricks Rest API.

   1)run the  script build-artifact.sh
   
   the script installs everything we need to build the python egg. Nevertheless, the installation of Databricks CLI on the used computer/docker is required. 
   Please follow the steps below to accomplish this :
   https://docs.databricks.com/dev-tools/cli/index.html
   
   The installation of databricks cli is required as both Main.py and the generated egg shall be stored under Databricks file system 
   while using databricks plateform to execute the pyspark Job.  
   
   2)run the script run_on_databricks.sh
   
  