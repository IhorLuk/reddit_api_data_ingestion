import os
import yaml
from google.cloud import bigquery

class BigQueryTable():
    def __init__(self, dataset_name: str, table_name: str) -> None:
        """_summary_

        Args:
            dataset_name (str): _description_
            table_name (str): _description_
        """
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ihor-lukianov/personal_projects/reddit_api_data_ingestion/content/key-bucket.json"
        self.client = bigquery.Client()
        self.dataset = dataset_name
        self.table = table_name
        self.table_id = self.client.dataset(self.dataset).table(self.table)
    
    def load_config_schema(self, config_path='schemas.yml'):
        """_summary_

        Args:
            config_path (str): local path to the schemas.yml file locally
        """
        with open(config_path) as schema_file:
            config = yaml.load(schema_file, Loader=yaml.Loader)
            
        for table in config:
            self.tableName = table.get('name')
            self.tableSchema = table.get('schema')
            
    def create_schema_from_yaml(self, table_schema):
        """_summary_

        Args:
            table_schema (list): list of dicts, each dict is a column in a schema for the BigQuery table

        Returns:
            list: pre-defined schema, read from the yml file (usually schemas.yml)
        """
        schema = []
        for column in table_schema:
            schemaField = bigquery.SchemaField(column['name'], column['type'], column['mode'])
            schema.append(schemaField)

            if column['type'] == 'RECORD':
                schemaField._fields = self.create_schema_from_yaml(column['fields'])
        return schema
    
    def create_table_if_not_exists(self, config_path='schemas.yml'):
        """Checks if this table already exists in the BigQuery, creates it from the pre-defined schema in yml file
        """
        try:
            self.client.get_table(self.table_id)
            print(f'Table {self.table_id} exists!')
        except:
            # firstly - load config
            self.load_config_schema(config_path=config_path)
            # define schema
            schema = self.create_schema_from_yaml(self.tableSchema)
            # create table from 
            table = bigquery.Table(self.table_id, schema=schema)
            table = self.client.create_table(table)
            print(f'Table {self.table_id} created')
    
    def load_from_cloud_storage(self, uri):
        """Loads all rows from a parquet file on a Cloud Storage to the BigQuery table

        Args:
            uri (str): link to parquet file in the google cloud storage
        """
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        load_job = self.client.load_table_from_uri(
            uri, self.table_id, job_config=job_config
        )

        # Waits for the job to complete
        load_job.result()
        destination_table = self.client.get_table(self.table_id)
        print(f'Total rows in the table: {destination_table.num_rows}')