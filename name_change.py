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


def chapter_name_change(rec):
    link_generator = LinkGenerator(rec['link_type'])
    flamel = Alchemist()
    rebrandly_connection = Rebrandly('api_keychain.yaml')
    pl = create_name_change_payload(rec, link_generator)
    time_stamp = datetime.now().isoformat()
    rec['request_type'] = 'Link Update'
    rec['link_date'] = time_stamp
    rec['request_timestamp'] = time_stamp
    request = rebrandly_connection.change_link(rec['rebrandly_id'], pl)
    rec['request_status'] = request.status_code
    flamel.upload_data(rec, 'rebrandly.requests')
    if request.status_code == requests.codes.ok:
        flamel.upload_data(rec, 'rebrandly.chapter_values')


def process_name_change_records(records, link_type):
    for chap in records:
        chap['link_type'] = link_type
        chapter_name_change(chap)


def main(link_type):
    name_change_table = Alchemist().get_table('dbt_epb.cl__chapter_name_changes')
    type_records_stmt = name_change_table.select().where(name_change_table.c.link_type == link_type)
    type_records = Alchemist().get_query(type_records_stmt)
    print(type_records)
    process_name_change_records(type_records, link_type)


main('cope')
main('nmo')
main('main')
