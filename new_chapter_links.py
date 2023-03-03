import json

from sqlalchemy import text, insert, func, select
from url_creator import LinkGenerator
from rebrandly_requester import Rebrandly
from flamel import Alchemist
from datetime import datetime
import requests




def create_new_link_payload(record, link_gen):
    link_creator = link_gen.set_chapter_type(record['employer_type'].lower())
    payload = {'title': record['chapter_name'],
               'slashtag': record['short_code'],
               'favorite': False,
               'destination': link_creator.create_link({'employer': record['chapter_name']})}
    return payload

def create_links(records, link_type, flamel):
    link_generator = LinkGenerator(link_type)
    for chap in records:
        pl = create_new_link_payload(chap, link_generator)
        time_stamp = datetime.now().isoformat()
        request = rebrandly_connection.create_link(pl)
        if request.status_code == requests.codes.ok:
            request_data = json.loads(request.text)
            status_upload_package = {'rebrandly_id': request_data['id'],
                                     'request_status': request.status_code,
                                     'request_type': 'Link Update',
                                     'request_timestamp': time_stamp}
            flamel.upload_data(status_upload_package, 'requests')
            chapter_value_upload = {
                'chapter_code': chap['chapter_code'],
                'chapter_name': chap['chapter_name'],
                'link_title': chap['chapter_name'],
                'rebrandly_id': request_data['id'],
                'chapter_id': chap['chapter_id'],
                'institution_id': chap['institution_id'],
                'institution_code': chap['institution_code'],
                'link_type': link_type,
                'employer_type': chap['employer_type'],
                'link_date': time_stamp
            }
            flamel.upload_data(chapter_value_upload, 'chapter_values')
            short_code_upload = {
                'slashtag': chap['short_code'],
                'rebrandly_id': request_data['id'],
                'link_date': time_stamp
            }
            flamel.upload_data(short_code_upload, 'short_codes')
        else:
            status_upload_package = {'request_status': request.status_code,
                                     'request_type': 'Link Creation',
                                     'request_timestamp': time_stamp}
            flamel.upload_data(status_upload_package, 'requests')

uncovered_chapter_query = text('''select * from dbt_epb.cl__short_codes_to_create''')

alci = Alchemist()

new_chapters = alci.get_query(uncovered_chapter_query)

rebrandly_connection = Rebrandly('api_keychain.yaml')

link_gen = LinkGenerator('main')

create_links(new_chapters, 'main', alci)