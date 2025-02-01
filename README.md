# Zillow-airflow-analytics
This project builds an end-to-end Zillow data pipeline using Python, RapidAPI, AWS (S3, Lambda, Redshift), Apache Airflow, and Amazon QuickSight for automation, transformation, and visualization.

## Table of Contents
- [Architecture](#Architecture)
- [Demo](#Demo)
- [DAG diagram](#DAG)
- [Python Code](#Code)
- [Visualization](#Visualization)
- [Learnings](#Learnings)
- [Acknowledgments](#acknowledgments)
- [Miscellaneous Files](#Miscellaneous)

## Architecture
![Data Architecture of the project](assets/Zillow%20Architechture.png)

## DAG
![DAG of the project](assets/airflow_dag.PNG)

## Demo

Click on the thumbnail below. You will be redirected to YouTube. Enjoy! :D

[![Watch Video](assets/demo_screenshot.png)](https://youtu.be/RYipQSIS-MU)


## Code
Python file:
![Zillow_Analytics.py](zillow_analytics.py)

config_api.json file:
```bash
{
	"x-rapidapi-key": "<API-KEY>",
	"x-rapidapi-host": "zillow56.p.rapidapi.com"
}
```

## Visualization
![Data visualization](assets/quicksight.PNG)

## Learnings
- Airflow Orchestration and DAG
- AWS
    - Creating an EC2 instance
    - SSH Connection
    - Lambda functions
    - Redshift connection
    - Quicksight
- Airflow & AWS connections - AWS & Redshift 

## Acknowledgments


## Miscellaneous
Screenshots of individual components of the orchestration. Found in the ```misc``` folder