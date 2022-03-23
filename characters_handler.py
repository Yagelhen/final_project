import rest_api_test
import db_handler
import pandas as pd

C_URL = "https://rickandmortyapi.com/api/character/"


"""
fetch from their db 
all LIVE or DEAD characters
"""


def get_all_characters(url):
    page_index = 1
    next_page = True
    characters = []
    while next_page is True:
        res = rest_api_test.get_api(f"{url}?page={page_index}")
        characters.extend(res['results'])
        page_index += 1
        next_page = res['info']['next'] is not None
    return pd.DataFrame.from_dict(characters, orient='columns')


def parse_characters_alive_or_dead(url):
    characters = get_all_characters(url)
    characters_alive_or_dead = characters.loc[(characters['status'] == 'Alive') | (characters['status'] == 'Dead')]
    return characters_alive_or_dead


"""
get api characters
transform them to the new objects we designed
return new_object_list
"""


def parse_staging_characters(url):
    characters_alive_or_dead = parse_characters_alive_or_dead(url)
    staging_characters = characters_alive_or_dead[['id', 'status', 'name', 'episode', 'location']]
    staging_characters = staging_characters.explode('episode')
    return staging_characters


print(parse_staging_characters(C_URL))


# def store_on_db(characters_moduled_list):
#     for character_moduled in characters_moduled_list:
#         print("store new object")
#         db_handler.store(character_moduled, "characters")
#
#
# #print(get_all_characters(URL))
# def run_character_ETL():
#     char_list = get_all_characters(URL)
#     parsed_objcet_list = parse_characters_to_new_objects(char_list)
#     store_on_db(parsed_objcet_list)

