import requests

def is_valid_url(url):
    '''
    Returns True if URL returns 200 status code
    '''
    return requests.head(url).status_code == 200

