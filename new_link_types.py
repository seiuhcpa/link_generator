import json
from url_creator import LinkGenerator
from rebrandly_requester import Rebrandly
from flamel import Alchemist
import yaml
from datetime import datetime
import requests


def main(link_type: str):
    records = get_type_to_create(link_type)
    for rec in records:
        print(rec)
        rec['link_type'] = link_type
        link_suffix = get_suffix(rec['link_type'])
        rec['slashtag'] = rec['slashtag'] + link_suffix
        rec['new_link'] = create_link_type(rec)
        if rec['new_link'] is not None:
            create_and_upload(rec)
        print(rec['new_link'])


def create_new_link_type_payload(record: dict):
    payload = {'title': record['chapter_name'],
               'slashtag': record['slashtag'],
               'favorite': False,
               'destination': record['new_link']}
    return payload


def create_and_upload(rec: dict):
    flamel = Alchemist()
    pl = create_new_link_type_payload(rec)
    time_stamp = datetime.now().isoformat()
    rec['link_date'] = time_stamp
    rec['request_timestamp'] = time_stamp
    rec['request_type'] = 'Link Creation'
    rebrandly_connection = Rebrandly('api_keychain.yaml')
    request = rebrandly_connection.create_link(pl)
    rec['request_status'] = request.status_code
    print(rec)
    if rec['request_status'] == requests.codes.ok:
        request_data = json.loads(request.text)
        rec['rebrandly_id'] = request_data['id']
        flamel.upload_data(rec, 'rebrandly.requests')
        flamel.upload_data(rec, 'rebrandly.chapter_values')
        flamel.upload_data(rec, 'rebrandly.short_codes')
    elif rec['request_status'] is not None:
        rec['rebrandly_id'] = None
        flamel.upload_data(rec, 'rebrandly.requests')
    else:
        print('No Request Made')
        print(rec)


# gets suffix string for non-main link type to append onto main link's slashtag
def get_suffix(link_type: str):
    link_key = open('link_roots.yaml', 'r').read()
    y_key = yaml.load(link_key, yaml.BaseLoader)
    suffix = y_key[link_type]['suffix']
    return suffix


def create_link_type(rec: dict):
    link_creator = LinkGenerator(rec['link_type'])
    if rec['employer'] is not None:
        pass
    else:
        rec['employer'] = rec['chapter_name']
    if rec['link_type'] != 'cope' and rec['employer_type'].lower() == 'private':
        link_instance = link_creator.set_chapter_type(rec['employer_type'].lower())
        new_link = link_instance.create_link(rec)
    elif rec['link_type'] == 'cope':
        if rec['sector'] == 'Homecare' and rec['institution'] is not None:
            rec['employer'] = rec['institution']
        link_instance = link_creator
        new_link = link_instance.create_link(rec)
    else:
        new_link = None
    return new_link


def get_type_to_create(link_type: str):
    alci = Alchemist()
    core_table = alci.get_table('dbt_epb.cl__uncovered_types')
    if link_type == 'nmo':
        table_filter = core_table.c.nmo_link.is_not(True)
    elif link_type == 'cope':
        table_filter = core_table.c.cope_link.is_not(True)
    else:
        table_filter = None
    if table_filter is not None:
        stmt = core_table.select().where(table_filter)
        records = alci.get_query(stmt)
    else:
        records = []
    return records


main('nmo')
main('cope')
