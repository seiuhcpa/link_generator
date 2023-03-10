from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy import insert
import yaml


class Alchemist:

    def __init__(self):
        creds = yaml.load(open('api_keychain.yaml', 'r'), yaml.BaseLoader)
        self.bq_creds = creds['bigquery']
        self.bq_engine = create_engine('bigquery://', credentials_info=self.bq_creds)
        self.metadata_obj = MetaData()

    def get_table(self, table_and_schema):
        table_object = Table(table_and_schema, self.metadata_obj, autoload_with=self.bq_engine)
        return table_object

    def create_connection(self):
        conn = self.bq_engine.connect()
        return conn

    def create_engine(self):
        new_engine = create_engine('bigquery://', credentials_info=self.bq_creds, future=True)
        return new_engine

    def get_query(self, query):
        conn = self.bq_engine.connect()
        table_exe = conn.execute(query)
        table_rows = table_exe.fetchall()
        result = [row._asdict() for row in table_rows]
        return result

    def upload_data(self, return_dict, schema_and_table_name):
        requests_table = self.get_table(schema_and_table_name)
        upload_dict = {}
        for c in requests_table.c:
            column_name = c.name
            upload_dict[column_name] = return_dict.get(column_name)
        stmt = insert(requests_table).values(upload_dict)
        engine = self.create_engine()
        with engine.connect() as conn:
            conn.execute(stmt)
