import json

from sqlalchemy import text, insert, func, select, true, and_
from url_creator import LinkGenerator
from rebrandly_requester import Rebrandly
from flamel import Alchemist
import yaml
from datetime import datetime
import requests


def main(link_type):
    records = get_type_to_create(link_type)
    alci = Alchemist()
    for rec in records:
        print(rec)
        link_suffix = get_suffix(link_type)
        rec['new_slashtag'] = rec['slashtag'] + link_suffix
        rec['new_link'] = create_link_type(rec, link_type)
        if rec['new_link'] is not None:
            create_and_upload(rec, alci, link_type)
        print(rec['new_link'])


def create_new_link_type_payload(record):
    payload = {'title': record['chapter_name'],
               'slashtag': record['new_slashtag'],
               'favorite': False,
               'destination': record['new_link']}
    return payload

def create_and_upload(rec,flamel,link_type):
    pl = create_new_link_type_payload(rec)
    time_stamp = datetime.now().isoformat()
    rebrandly_connection = Rebrandly('api_keychain.yaml')
    request = rebrandly_connection.create_link(pl)
    if request.status_code == requests.codes.ok:
        request_data = json.loads(request.text)
        status_upload_package = {'rebrandly_id': request_data['id'],
                                 'request_status': request.status_code,
                                 'request_type': 'Link Creation',
                                 'request_timestamp': time_stamp}
        flamel.upload_data(status_upload_package, 'requests')
        chapter_value_upload = {
            'chapter_code': rec['chapter_code'],
            'chapter_name': rec['chapter_name'],
            'link_title': rec['chapter_name'],
            'rebrandly_id': request_data['id'],
            'chapter_id': rec['chapter_id'],
            'institution_id': rec['institution_id'],
            'institution_code': rec['institution_code'],
            'link_type': link_type,
            'employer_type': rec['employer_type'],
            'link_date': time_stamp
        }
        flamel.upload_data(chapter_value_upload, 'chapter_values')
        short_code_upload = {
            'slashtag': rec['new_slashtag'],
            'rebrandly_id': request_data['id'],
            'link_date': time_stamp
        }
        flamel.upload_data(short_code_upload, 'short_codes')
    else:
        status_upload_package = {'request_status': request.status_code,
                                 'request_type': 'Link Creation',
                                 'request_timestamp': time_stamp}
        flamel.upload_data(status_upload_package, 'requests')


def get_suffix(link_type):
    link_key = open('link_roots.yaml', 'r').read()
    y_key = yaml.load(link_key, yaml.BaseLoader)
    suffix = y_key[link_type]['suffix']
    return suffix


def create_link_type(rec, link_type):
    link_creator = LinkGenerator(link_type)
    if rec['employer'] is not None:
        pass
    else:
        rec['employer'] = rec['chapter_name']
    if link_type != 'cope' and rec['employer_type'].lower() == 'private':
        link_instance = link_creator.set_chapter_type(rec['employer_type'].lower())
        new_link = link_instance.create_link(rec)
    elif link_type == 'cope':
        if rec['sector'] == 'Homecare' and rec['institution'] is not None:
            rec['employer'] = rec['institution']
        link_instance = link_creator
        new_link = link_instance.create_link(rec)
    else:
        new_link = None
    return new_link


def get_type_to_create(link_type):
    alci = Alchemist()
    core_table = alci.get_table('dbt_epb.cl__uncovered_types')
    if link_type == 'nmo':
        table_filter = core_table.c.nmo_link.is_not(True)
    if link_type == 'cope':
        table_filter = core_table.c.cope_link.is_not(True)
    stmt = core_table.select().where(table_filter)
    records = alci.get_query(stmt)
    return records


main('nmo')
main('cope')
