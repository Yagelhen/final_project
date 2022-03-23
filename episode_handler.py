import rest_api_test
import db_handler
import pandas as pd
import characters_handler


E_URL = "https://rickandmortyapi.com/api/episode/"

"""
fetch from their db 
all LIVE or DEAD characters
"""


def get_all_episodes(url):
    page_index = 1
    next_page = True
    episodes = []
    while next_page is True:
        res = rest_api_test.get_api(f"{url}?page={page_index}")
        episodes.extend(res['results'])
        page_index += 1
        next_page = res['info']['next'] is not None
    return pd.DataFrame.from_dict(episodes, orient='columns')


def parse_staging_episodes(url):
    episodes = get_all_episodes(url)
    staging_episodes = episodes[['id', 'name', 'air_date', 'episode', 'characters']]
    staging_episodes = staging_episodes.explode('characters')
    staging_episodes['characters_id'] = staging_episodes['characters'].apply(lambda x: int(x.split('/').pop()))
    return staging_episodes


def merge_to_fact(url):
    episodes_before_merge = parse_staging_episodes(url)
    characters = characters_handler.parse_staging_characters(characters_handler.C_URL)
    episodes_after_merge = pd.merge(episodes_before_merge, characters, how='left', left_on='characters_id', right_on='id')
    return episodes_after_merge[['id_x', 'name_x', 'air_date', 'episode_x', 'characters_id', 'location']]


print(merge_to_fact(E_URL).head())

