from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy import text, insert, func, select
import yaml

class Alchemist:

    def __init__(self):
        creds = yaml.load(open('api_keychain.yaml', 'r'), yaml.BaseLoader)
        self.bqcreds = creds['bigquery']
        self.bq_engine = create_engine('bigquery://', credentials_info=self.bqcreds)
        self.metadata_obj = MetaData()

    def get_table(self, table_and_schema):
        table_object = Table(table_and_schema, self.metadata_obj, autoload_with=self.bq_engine)
        return table_object

    def create_connection(self):
        conn = self.bq_engine.connect()
        return conn

    def create_engine(self):
        new_engine = create_engine('bigquery://', credentials_info=self.bqcreds, future=True)
        return new_engine

    def get_link_table(self, table_name):
        link_table = Table('dbt_epb.'+table_name, self.metadata_obj, autoload_with=self.bq_engine)
        full_table_select = link_table.select().where(link_table.c.public_private == 'Private')
        conn = self.bq_engine.connect()
        table_exe = conn.execute(full_table_select)
        table_rows = table_exe.fetchall()
        result = [row._asdict() for row in table_rows]
        return result

    def get_query(self, query):
        conn = self.bq_engine.connect()
        table_exe = conn.execute(query)
        table_rows = table_exe.fetchall()
        result = [row._asdict() for row in table_rows]
        return result

    def upload_data(self, return_dict, table_name):
        schema = 'rebrandly.'
        requests_table = self.get_table(schema+table_name)
        stmt = insert(requests_table).values(return_dict)
        engine = self.create_engine()
        print(stmt.compile().params)
        with engine.connect() as conn:
            conn.execute(stmt)