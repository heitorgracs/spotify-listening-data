import pandas as pd
import prefect

from scripts.transform import dataprep_viz
from scripts.data_enrichment import data_enrichment

@prefect.task(name="Transform Task")
def transform_task():
    dataprep_viz()

@prefect.task(name="Data Enrichment Task")
def data_enrichment_task():
    data_enrichment()

@prefect.flow(name="ETL Pipeline")
def etl_pipeline():
    print("Starting ETL Pipeline")
    transform_task()
    print("ETL Pipeline completed")
    data_enrichment_task()
    print("Data Enrichment Task completed")
    
if __name__ == "__main__":
    etl_pipeline()