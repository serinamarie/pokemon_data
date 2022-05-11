from typing import List
import requests, json
from prefect import flow, task

@task(name="Retrieve type data from Pokemon API")
def retrieve_data(_pokemon_type: int):
    '''Retrieve data for Pokemon type'''
    if 17 > _pokemon_type > 0:
        url_to_get = "https://pokeapi.co/api/v2/type/" + str(_pokemon_type)
        response = requests.get(url_to_get)
        return response.json()
    else:
        raise ValueError('Integer must be between 1-16')
    
@task(name="Extract Pokemon-specific data from json")
def extract_pokemon_data(_type_data) -> List:
    '''Given json, extracts Pokemon-specific data to list'''
    return [pokemon['pokemon']['name'] for pokemon in _type_data['pokemon']]

@task(name="Save Pokemon Data to txt")
def save_pokemon_data(_pokemon_data: List, _pokemon_type: int) -> None:
    '''Give a list and a Pokemon type (int), writes list to a txt file.'''
    filename = 'pokemon_data/type_' + str(_pokemon_type) + '.txt'
    with open(filename, 'w') as outfile:
        json.dump(_pokemon_data, outfile)

@flow(name='Pokemon Extract and Save')
def pokemon_extract_save(pokemon_type: int) -> None:
    all_type_data = retrieve_data(pokemon_type)
    pokemon_data = extract_pokemon_data(all_type_data)
    save_pokemon_data(pokemon_data, pokemon_type)

if __name__ == "__main__":
    pokemon_extract_save(pokemon_type=2)
    pokemon_extract_save(pokemon_type=5)
    pokemon_extract_save(pokemon_type=8)