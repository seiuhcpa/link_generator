from link_creation_functions import *
import yaml
from flamel import Alchemist


def get_type_to_create(link_type: str):
    alchemist = Alchemist()
    core_table = alchemist.get_table('dbt_epb.cl__uncovered_types')
    if link_type == 'nmo':
        table_filter = core_table.c.nmo_link.is_not(True)
    elif link_type == 'cope':
        table_filter = core_table.c.cope_link.is_not(True)
    else:
        table_filter = None
    if table_filter is not None:
        stmt = core_table.select().where(table_filter)
        records = alchemist.get_query(stmt)
    else:
        records = []
    return records


def create_links_for_new_chapers():
    alchemist = Alchemist()
    short_codes_query = alchemist.get_table('dbt_epb.cl__short_codes_to_create').select()
    new_chapters = alchemist.get_query(short_codes_query)
    link_type = 'main'
    for rec in new_chapters:
        rec['link_type'] = link_type
        rec['slashtag'] = rec['short_code']
        rec['new_link'] = create_link_type(rec)
        create_and_upload(rec)


def create_link_types(link_type: str):
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


# gets suffix string for non-main link type to append onto main link's slashtag
def get_suffix(link_type: str):
    link_key = open('link_roots.yaml', 'r').read()
    y_key = yaml.load(link_key, yaml.BaseLoader)
    suffix = y_key[link_type]['suffix']
    return suffix


create_links_for_new_chapers()
create_link_types('nmo')
create_link_types('cope')
