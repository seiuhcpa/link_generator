from new_link_creation import *
from flamel import Alchemist
from typing import List, Dict


def create_links(records: List[Dict], link_type: str):
    for rec in records:
        rec['link_type'] = link_type
        rec['slashtag'] = rec['short_code']
        rec['new_link'] = create_link_type(rec)
        create_and_upload(rec)


def main():
    alchemist = Alchemist()
    short_codes_query = alchemist.get_table('dbt_epb.cl__short_codes_to_create').select()
    new_chapters = alchemist.get_query(short_codes_query)
    create_links(new_chapters, 'main')


main()
