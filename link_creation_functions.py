from datetime import datetime
import requests
import json
from url_creator import LinkGenerator
from rebrandly_requester import Rebrandly
from flamel import Alchemist


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


def create_new_link_payload(record: dict):
    payload = {'title': record['chapter_name'],
               'slashtag': record['slashtag'],
               'favorite': False,
               'destination': record['new_link']}
    return payload


def create_and_upload(rec: dict):
    flamel = Alchemist()
    pl = create_new_link_payload(rec)
    time_stamp = datetime.now().isoformat()
    rec['link_date'] = time_stamp
    rec['request_timestamp'] = time_stamp
    rec['request_type'] = 'Link Creation'
    rebrandly_connection = Rebrandly('api_keychain.yaml')
    request = rebrandly_connection.create_link(pl)
    rec['request_status'] = request.status_code
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