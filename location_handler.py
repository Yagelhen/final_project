import rest_api_test
import db_handler
import pandas as pd

URL = "https://rickandmortyapi.com/api/location/"

"""
fetch from their db 
all LIVE or DEAD characters
"""


def get_all_locations(url):
    page_index = 1
    next_page = True
    locations = []
    while next_page is True:
        res = rest_api_test.get_api(f"{url}?page={page_index}")
        locations.extend(res['results'])
        page_index += 1
        next_page = res['info']['next'] is not None
    return pd.DataFrame.from_dict(locations, orient='columns')


def parse_staging_locations(url):
    locations = get_all_locations(url)
    staging_locations = locations[['id', 'name']]
    return staging_locations


print(parse_staging_locations(URL))

