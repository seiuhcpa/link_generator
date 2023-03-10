import json

from sqlalchemy import text, insert, func, select
from url_creator import LinkGenerator
from rebrandly_requester import Rebrandly
from flamel import Alchemist
from datetime import datetime
from typing import List, Dict
import requests


def create_new_link_payload(record: Dict, link_gen: LinkGenerator):
    link_creator = link_gen.set_chapter_type(record['employer_type'].lower())
    payload = {'title': record['chapter_name'],
               'slashtag': record['short_code'],
               'favorite': False,
               'destination': link_creator.create_link({'employer': record['chapter_name']})}
    return payload


def request_and_record_link_creation(rec):
    flamel = Alchemist()
    link_generator = LinkGenerator(rec['link_type'])
    rebrandly_connection = Rebrandly('api_keychain.yaml')
    pl = create_new_link_payload(rec, link_generator)
    rec['request_type'] = 'Link Creation'
    time_stamp = datetime.now().isoformat()
    rec['link_date'] = time_stamp
    rec['request_timestamp'] = time_stamp
    rec['slashtag'] = rec['short_code']
    request = rebrandly_connection.create_link(pl)
    rec['request_status'] = request.status_code
    if rec['request_status'] == requests.codes.ok:
        request_data = json.loads(request.text)
        rec['rebrandly_id'] = request_data['id']
        flamel.upload_data(rec, 'rebrandly.requests')
        flamel.upload_data(rec, 'rebrandly.chapter_values')
        flamel.upload_data(rec, 'rebrandly.short_codes')
    else:
        flamel.upload_data(rec, 'requests')

def create_links(records: List[Dict], link_type: str, flamel: Alchemist):
    for rec in records:
        rec['link_type'] = link_type



def main():
    alci = Alchemist()
    short_codes_query = alci.get_table('dbt_epb.cl__short_codes_to_create').select()
    new_chapters = alci.get_query(short_codes_query)
    create_links(new_chapters, 'main', alci)

main()
