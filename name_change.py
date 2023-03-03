from sqlalchemy import text, insert, func, select
from url_creator import LinkGenerator
from rebrandly_requester import Rebrandly
from flamel import Alchemist
from datetime import datetime
import requests


def create_name_change_payload(record, link_gen):
    link_creator = link_gen.set_chapter_type(record['employer_type'])
    payload = {'title': record['chapter_name'],
               'favorite': False,
               'destination': link_creator.create_link({'employer': record['chapter_name']})}
    return payload


def change_employer_name_link(records, link_type, flamel):
    link_generator = LinkGenerator(link_type)
    for chap in records:
        pl = create_name_change_payload(chap, link_generator)
        time_stamp = datetime.now().isoformat()
        request = rebrandly_connection.change_link(chap['rebrandly_id'],
                                                   pl)
        status_upload_package = {'rebrandly_id': chap['rebrandly_id'],
                                 'request_status': request.status_code,
                                 'request_type': 'Link Update',
                                 'request_timestamp': time_stamp}
        flamel.upload_data(status_upload_package, 'requests')
        if request.status_code == requests.codes.ok:
            chapter_value_upload = {
                'chapter_code': chap['chapter_code'],
                'chapter_name': chap['chapter_name'],
                'link_title': chap['chapter_name'],
                'rebrandly_id': chap['rebrandly_id'],
                'chapter_id': chap['chapter_id'],
                'institution_id': chap['institution_id'],
                'institution_code': chap['institution_code'],
                'link_type': link_type,
                'employer_type': chap['employer_type'],
                'link_date': time_stamp
            }
            flamel.upload_data(chapter_value_upload, 'chapter_values')



name_change_query_cope = text('''select rebrandly_id,
                                       chapter_name_ac as chapter_name,
                                       cv.chapter_code,
                                       uw_job.chapter_id,
                                       cv.institution_code,
                                       uw_job.institution_id,
                                       employer_type   as employer_type,
                                from dbt_epb.cl__chapter_values cv
                                         left join (select chapter_code,
                                                           chapter_id,
                                                           institution_id,
                                                           institution_code,
                                                           chapter_name as chapter_name_ac,
                                                    from dbt_epb.cl__active_chapters
                                                    where employer_type = 'Private') uw_job
                                                   on cv.chapter_code = uw_job.chapter_code and
                                                      ifnull(cv.institution_code, 'None') =
                                                      ifnull(uw_job.institution_code, 'None')
                                where chapter_name_ac is not null
                                  and chapter_name != chapter_name_ac
                                  and rebrandly_id is not null
                                  and most_recent is true
                                      and link_type = 'cope' ''')
name_change_query_main = text('''select rebrandly_id,
                                       chapter_name_ac as chapter_name,
                                       cv.chapter_code,
                                       uw_job.chapter_id,
                                       cv.institution_code,
                                       uw_job.institution_id,
                                       employer_type   as employer_type,
                                from dbt_epb.cl__chapter_values cv
                                         left join (select chapter_code,
                                                           chapter_id,
                                                           institution_id,
                                                           institution_code,
                                                           chapter_name as chapter_name_ac,
                                                    from dbt_epb.cl__active_chapters
                                                    where employer_type = 'Private') uw_job
                                                   on cv.chapter_code = uw_job.chapter_code and
                                                      ifnull(cv.institution_code, 'None') =
                                                      ifnull(uw_job.institution_code, 'None')
                                where chapter_name_ac is not null
                                  and chapter_name != chapter_name_ac
                                  and rebrandly_id is not null
                                  and most_recent is true
                                  and link_type = 'main' ''')


rebrandly_connection = Rebrandly('api_keychain.yaml')
alci = Alchemist()
# name_change_records = alci.get_query(name_change_query_main)
# change_employer_name_link(name_change_records,
#                           'main', alci)
cope_name_change_records = alci.get_query(name_change_query_cope)
change_employer_name_link(cope_name_change_records, 'cope', alci)