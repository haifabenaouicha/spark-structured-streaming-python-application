s#!/bin/sh


# Install pipenv & move to project folder
python -m pip install -U pip
python -m pip install setuptool
python -m pip install pipenv


pipenv install --python=3.6  #here change to the python version you have already instlled on your computer

# Build the egg.
pipenv run python setup.py bdist_egg


#COPY DATA TO DATA LAKE STORE# Setup a python 3 environment install project dependencies
##pipenv --rm

#this nedds databricks cli to already be intsalled

dbfs cp  main.py  dbfs:/artefacts/   # main script should be copied to a dbfs path

dbfs cp  dist/stremaflex-1.0.0-py3.6.egg  dbfs:/artefacts/   #the generated egg shall be copied  to a dbfs path

dbfs cp conf/test.conf  dbfs:/artefacts/  #push the conf file in  datalake so it is processed independently from source code

#these two paths shall be the same as the onesdefined in the file dbs_api.json





