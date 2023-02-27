from sqlalchemy import text, insert, func, select
from url_creator import LinkGenerator
from rebrandly_requester import Rebrandly
from flamel import Alchemist
from datetime import datetime
import requests


def create_payload(record, link_gen):
    link_creator = link_gen.set_chapter_type(record['employer_type'])
    payload = {'title': record['chapter_name'],
               'favorite': False,
               'destination': link_creator.create_link({'employer': record['chapter_name']})}
    return payload


def upload_data(return_dict, table_name):
    schema = 'rebrandly.'
    requests_table = alci.get_table(schema+table_name)
    stmt = insert(requests_table).values(return_dict)
    engine = alci.create_engine()
    print(stmt.compile().params)
    with engine.connect() as conn:
        conn.execute(stmt)


def upload_name_change(return_dict):
    requests_table = alci.get_table('rebrandly.chapter_values')
    stmt = insert(requests_table).values(return_dict)
    engine = alci.create_engine()
    print(stmt.compile().params)
    with engine.connect() as conn:
        status_insert = conn.execute(stmt)
        conn.commit()


def change_employer_name_link(records, link_type):
    link_generator = LinkGenerator(link_type)
    for chap in records:
        pl = create_payload(chap, link_generator)
        time_stamp = datetime.now().isoformat()
        request = rebrandly_connection.change_link(chap['rebrandly_id'],
                                                   pl)
        status_upload_package = {'rebrandly_id': chap['rebrandly_id'],
                                 'request_status': request.status_code,
                                 'request_type': 'Link Update',
                                 'request_timestamp': time_stamp}
        upload_data(status_upload_package, 'requests')
        if request.status_code == requests.codes.ok:
            chapter_value_upload = {
                'chapter_code': chap['chapter_code'],
                'chapter_name': chap['chapter_name'],
                'link_title': chap['chapter_name'],
                'rebrandly_id': chap['rebrandly_id'],
                'link_type': 'main',
                'employer_type': chap['employer_type'],
                'link_date': time_stamp
            }
            upload_data(chapter_value_upload, 'chapter_values')



name_change_query_cope = text('''select rebrandly_id, 
                            chapter_name_uw as chapter_name, 
                            chapter_code,
                            institution_code,
                            employer_type as employer_type,
                            from dbt_epb.cl__chapter_values
                                     left join (select chapter_code, 
                                                       chapter_name as chapter_name_uw 
                                                from analysis_uw.stg_uw__job_chapters
                                                        where employer_type = 'Private'
                                                        and is_active is true
                                                        )
                                               using (chapter_code)
                            where chapter_name != chapter_name_uw 
                              and chapter_name_uw is not null and rebrandly_id is not null
                              and link_type = 'cope' ''')
name_change_query_main = text('''select rebrandly_id, 
                            chapter_name_uw as chapter_name, 
                            chapter_code,
                            institution_code,
                            employer_type as employer_type,
                            from dbt_epb.cl__chapter_values
                                     left join (select chapter_code, 
                                                       chapter_name as chapter_name_uw 
                                                from analysis_uw.stg_uw__job_chapters
                                                        where employer_type = 'Private'
                                                        and is_active is true)
                                               using (chapter_code)
                            where chapter_name != chapter_name_uw 
                              and chapter_name_uw is not null 
                              and rebrandly_id is not null
                              and link_type = 'main' ''')


rebrandly_connection = Rebrandly('api_keychain.yaml')
alci = Alchemist()
name_change_records = alci.get_query(name_change_query_main)
change_employer_name_link(name_change_records, 'main')
cope_name_change_records = alci.get_query(name_change_query_cope)
change_employer_name_link(cope_name_change_records, 'cope')