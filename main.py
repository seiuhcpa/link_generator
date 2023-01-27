import sqlalchemy
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import os
from url_creator import LinkGenerator


def get_link_table(table_name):
    creds = 'dbt_service_account_prod-hcpa-762cf2bd.json'
    bq_engine = create_engine('bigquery://', credentials_path=creds)
    metadata_obj = MetaData()
    link_table = Table('dbt_epb.'+table_name, metadata_obj, autoload_with=bq_engine)
    full_table_select = link_table.select().where(link_table.c.public_private == 'Private')
    conn = bq_engine.connect()
    table_exe = conn.execute(full_table_select)
    table_rows = table_exe.fetchall()
    result = [row._asdict() for row in table_rows]
    return result


test = LinkGenerator('cope')
test_dict = [{'employer' : 'Erich Prantl-Bartlett','institution':'test'},
            {'employer' : 'Mack Finkle','institution':'test'}]
for x in test_dict:
    print(test.create_link(x))






# def main():
#     full_link_key = get_link_key()
#     p_link_key = full_link_key['private']
#     print(p_link_key)
#     table = get_link_table('stg_gs_reb__original_link_table')
#     for x in table:
#         employer_field = p_link_key['fields']['employer']['url_tag']
#         new_link = p_link_key['url']+employer_field+quote(x['chapter'])
#         print(x)
#         print(new_link)
#
#
# #main()






