from typing import List
import requests, json, httpx, asyncio
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(name="Retrieve type data from Pokemon API", cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def retrieve_type_data(_pokemon_type: int):
    '''Retrieve data for Pokemon type'''
    if 17 > _pokemon_type > 0:
        url_to_get = "https://pokeapi.co/api/v2/type/" + str(_pokemon_type)
        response = requests.get(url_to_get)
        return response.json()
    else:
        raise ValueError('Integer must be between 1-16')

@task(name="Extract Pokemon-specific data from json")
def extract_pokemon_url(_type_data) -> List:
    '''Given json, extracts Pokemon-specific data to list'''
    return [pokemon['pokemon']['url'] for pokemon in _type_data['pokemon']]

@task(name="Retrieve individual Pokemon's data from Pokemon API", retries=1, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
async def retrieve_individal_pokemon_data(url: List):
    async with httpx.AsyncClient() as client:
        response = await client.get(url).json()
        return response['forms']

@task(name="Save Pokemon Data to txt")
def save_pokemon_data(_pokemon_data: List, _pokemon_type: int) -> None:
    '''Give a list and a Pokemon type (int), writes list to a txt file.'''
    filename = 'pokemon_data/type_' + str(_pokemon_type) + '.txt'
    with open(filename, 'w') as outfile:
        json.dump(_pokemon_data, outfile)

@flow(name='Pokemon Extract Url')
def pokemon_extract_urls(pokemon_type: int) -> None:
    all_type_data = retrieve_type_data(pokemon_type).result()
    pokemon_urls = extract_pokemon_url(all_type_data).result()
    return pokemon_urls

@flow(name="Async Flow")
async def pokemon_async_flow(pokemon_type):
    pokemon_urls = pokemon_extract_urls(pokemon_type).result()
    all_pokemon_data = await asyncio.gather(*[retrieve_individal_pokemon_data(pokemon_url) for pokemon_url in pokemon_urls])
    save_pokemon_data(all_pokemon_data, pokemon_type)

if __name__ == "__main__":
    print(asyncio.run(pokemon_async_flow(2)))




