import requests
import json 
import singer
import datetime
import time
import os
import sys

BASE_URL = "https://mubi.com/services/api/"

logger = singer.get_logger()

#### Overriding Singer method format_message

# Because of an encoding issue, we need to overwrite the standard Singer method format_message (which is later used in write_records).
# With the option ensure_ascii=False, we ensure that special characters are formatted correctly, e.g. Céline instead of C\u00e9line.
# Reason is unclear. Cf. this issue: https://github.com/singer-io/singer-python/issues/64.
def _format_message(message):
    return json.dumps(message.asdict(), ensure_ascii=False)

def _write_message(message):
    sys.stdout.write(_format_message(message) + '\n')
    sys.stdout.flush()

def _write_record(stream_name, record, stream_alias=None, time_extracted=None):
    _write_message(singer.RecordMessage(stream=(stream_alias or stream_name),
                                record=record,
                                time_extracted=time_extracted))
def _write_records(stream_name, records):
    for record in records:
        _write_record(stream_name, record)

##### End overriding format_message

def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schema(file_name):
    path = _get_abs_path("schemas") + "/" + file_name
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

                logger.info(f"Got data for {url}: {movie_data}")

                data = {
                    "movie_id": int(film),
                    "movie_title": movie_data["title"],
                    "movie_title_locale": movie_data["title_locale"],
                    "movie_canonical_url": movie_data["canonical_url"],
                    "movie_year": movie_data["year"],
                    "movie_popularity": movie_data["popularity"],
                    "movie_directors": movie_data["directors"],
                    "_extracted_at": _extracted_at,
                    "_sdc_id": f"{int(film)}_{_utc_now}"  # ID is concatenation of movie ID and unix timestamp (for append-only insertion)
                }

                movies.append(data)
              
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


    return movies

def sync_movie_data(list_id):
    movie_data = get_movie_data(list_id)
    stream = "top_movies__details"
    schema = get_schema(stream + ".json")
    singer.write_schema(stream, schema, "_sdc_id")
    _write_records(stream, movie_data)
    