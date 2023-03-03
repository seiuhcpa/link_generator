import requests
import yaml


class Rebrandly:

    def __init__(self, yaml_key):
        self.yaml_key = yaml_key
        self.domain = None
        self.headers = None
        self.url_root = 'https://api.rebrandly.com/v1'
        self.parse_key()

    def parse_key(self):
        key_chain = open(self.yaml_key, 'r').read()
        key_chain = yaml.load(key_chain, yaml.BaseLoader)
        if 'rebrandly' in key_chain:
            rebrandly_key = key_chain['rebrandly']
            self.domain = rebrandly_key['domain']
            self.headers = rebrandly_key['request_header']
        elif 'domain' in key_chain and 'request_header' in key_chain:
            self.domain = key_chain['domain']
            self.headers = key_chain['request_header']

    def get_link_info(self, link_id):
        path = 'links'
        request_parts = [self.url_root, path, link_id]
        request = '/'.join(request_parts)
        response = requests.get(request,
                                headers=self.headers)
        return response

    def change_link(self, link_id, payload):
        path = 'links'
        request_parts = [self.url_root, path, link_id]
        request = '/'.join(request_parts)
        response = requests.post(request,
                                json=payload,
                                headers=self.headers)
        return response

    def create_link(self, payload):
        path = 'links'
        request_parts = [self.url_root, path]
        request = '/'.join(request_parts)
        payload['domain'] = self.domain
        response = requests.post(request,
                                json=payload,
                                headers=self.headers)
        return response





