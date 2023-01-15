import requests
import json 
import singer
import datetime
import time
import os 

BASE_URL = "https://mubi.com/services/api/"

logger = singer.get_logger()

def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schema(file_name):
    #path = _get_abs_path("schemas") + "/" + file_name
    path = '/Users/davidwitkowski/Development/projects/tap-mubi/tap_mubi/schemas' + "/" + file_name
    with open(path) as file:
        return json.load(file)

def get_movie_list(list_id):
    res = requests.get(BASE_URL + f"lists/{list_id}")
    return json.loads(res.text)    

def sync_movie_list(list_id):
    _extracted_at = datetime.datetime.now().isoformat()
    _utc_now = round(time.time())
    list_data = get_movie_list(list_id)

    # Flatten nested dictionary for movie list
    i = 0
    movies = []
    for film in list_data["list_films"]: 
        i += 1
        data = {
            "list_id" : list_data["id"],
            "list_user_id" : list_data["user_id"],
            "list_created_at": list_data["created_at"],
            "list_updated_at": list_data["updated_at"],
            "list_title_locale": list_data["title"],
            "list_title_locale": list_data["title_locale"],
            "list_canonical_url": list_data["canonical_url"],
            "movie_id": int(film),
            "movie_rank": i,
            "_extracted_at": _extracted_at,
            "_sdc_id": f"{int(film)}_{_utc_now}"  # ID is concatenation of list ID and unix timestamp (for append-only insertion)
        }
        movies.append(data)

    stream = "top_movies"
    schema = get_schema(stream + ".json")
    singer.write_schema(stream, schema, "_sdc_id")
    singer.write_records(stream, movies)

def get_movie_data(list_id):
    movies = []
    directors = []
    list_data = get_movie_list(list_id)
    i = 0

    for film in list_data["list_films"]: 
        i += 1
        session = requests.Session()

        # Infinite loop to retry querying movie details after exception
        while True: 
            try:
                _extracted_at = datetime.datetime.now().isoformat()
                _utc_now = round(time.time())
                url = BASE_URL + f"films/{film}"
                logger.info(f"Requesting {url}, {i} out of {len(list_data['list_films'])}.")
                res = session.get(url)
                res.raise_for_status()

                movie_data = json.loads(res.text)

                data = {
                    "movie_id": int(film),
                    "movie_title": movie_data["title"],
                    "movie_title_locale": movie_data["title_locale"],
                    "movie_canonical_url": movie_data["canonical_url"],
                    "movie_year": movie_data["year"],
                    "movie_popularity": movie_data["popularity"],
                    "_extracted_at": _extracted_at,
                    "_sdc_id": f"{int(film)}_{_utc_now}"  # ID is concatenation of movie ID and unix timestamp (for append-only insertion)
                }

                movies.append(data)
                
                # Nested dictionary is written in separate stream
                for n in range(len(movie_data["directors"])): 
                    data = {
                        "movie_id": int(film),
                        "director_id": movie_data["directors"][n]["id"],
                        "director_name": movie_data["directors"][n]["name"],
                        "director_canonical_url": movie_data["directors"][n]["canonical_url"],
                        "_extracted_at": _extracted_at,
                        "_sdc_id": f"{int(film)}_{movie_data['directors'][n]['id']}_{_utc_now}",  # ID is concatenation of movie ID, director ID and unix timestamp (for append-only insertion)
                        "_sdc_source_key_id": f"{int(film)}_{_utc_now}"  # refers to the parent object
                    }        
                directors.append(data)

                # Preventive pausing after 300 requests to not get blocked by Mubi. 
                if i % 300 == 0:
                    logger.info(f"Preventive pausing for 5 minutes to not get blocked.")
                    for n in range(1, 6):
                        time.sleep(60)
                        logger.info(f"Paused preventively {n} out of 5 minutes.")

            # Catch 429 error, if the preventive pausing was not enough. Wait additionally.
            except Exception as e:
                logger.warning(f"{e}. Pausing for 5 minutes.")
                for n in range(1, 6):
                    time.sleep(60)
                    logger.info(f"Paused {n} out of 5 minutes.")
                continue
            break

    res = {"movie_details": movies, "directors": directors}
    return res

def sync_movie_data(list_id):
    movie_data = get_movie_data(list_id)
    movie_details = movie_data["movie_details"]
    directors = movie_data["directors"]

    # Movie Details
    stream = "top_movies__details"
    schema = get_schema(stream + ".json")
    singer.write_schema(stream, schema, "_sdc_id")
    singer.write_records(stream, movie_details)
    
    # Movie Details - Directors
    stream = "top_movies__details__directors"
    schema = get_schema(stream + ".json")
    singer.write_schema(stream, schema, "_sdc_id")
    singer.write_records(stream, directors)
