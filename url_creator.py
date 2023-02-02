import yaml
from urllib.parse import quote


class LinkGenerator:

    def __init__(self, link_type):
        self.link_type = link_type
        self.link_key = None
        self.url_root = None
        self.url_fields = None
        self.profile_link_type()

    def profile_link_type(self):
        link_key = open('link_roots.yaml', 'r').read()
        y_key = yaml.load(link_key, yaml.BaseLoader)
        self.link_key = y_key[self.link_type]
        self.url_root = self.link_key['url']
        self.url_fields = self.link_key['fields']

    def create_link(self, value_dict):
        link_values = {}
        print(value_dict)
        for key, value in self.url_fields.items():
            if key in value_dict.keys():
                link_values[key] = self.url_fields[key].copy()
                url_value = quote(value_dict[key])
                link_values[key]['url_value'] = url_value
            elif 'url_value' in value:
                link_values[key] = self.url_fields[key].copy()
        link_suffix = []
        for key, value in link_values.items():
            key_combine = value['url_tag']+value['url_value']
            link_suffix.append(key_combine)
        link_suffix = '&'.join(link_suffix)
        pre_filled_link = self.url_root+link_suffix
        return pre_filled_link
