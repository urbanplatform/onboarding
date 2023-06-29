import json
import re
from datetime import datetime
from typing import Optional

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow constants and variables
DAG_NAME = "our_weather_importer"

# Spanish weather data
WEATHER_DATA_PROVIDER = "AEMET"
WEATHER_URL = "https://opendata.aemet.es/opendata/api/observacion/convencional/todas"
WEATHER_API_KEY = "TO_BE_DEFINED"  # :-)

# Airflow DAG arguments
ARGS = {
    "owner": "airflow",
    "email": ["smartcities-dev@ubiwhere.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

# Airflow DAG running configurations
EVERY_15_MINUTES = "*/15 * * * *"
START_DATE = datetime(2023, 1, 1)

# Headers for the Weather API requests
LOCAL_HEADERS = {
    "Content-Type": "application/json",
    "api_key": WEATHER_API_KEY,
}

# Define your city name here
CITY_NAME = "TO_BE_DEFINED"  # :-)

# Date standard formats
DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_TIME_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


# Define your SmartDataModel here
# [`WeatherObserved`](https://github.com/smart-data-models/dataModel.Weather/blob/master/WeatherObserved/doc/spec.md).
SMART_DATA_MODEL = {
    "model_name": "weather.WeatherObserved",
    "data": {
        "name": "",
        "location": None,
        "address": "",
        "areaServed": "",
        "temperature": 0.0,
        "temperatureMinimum": 0.0,
        "temperatureMaximum": 0.0,
        "atmosphericPressure": 0.0,
        "relativeHumidity": None,
        "windSpeed": 0.0,
        "windDirection": 0.0,
        "snowHeight": 0.0,
        "atmosphericPressureSeaLevel": 0.0,
        "rainTotalSum10": 0.0,
        "dateObserved": None,
        "dataProvider": WEATHER_DATA_PROVIDER,
        "source": WEATHER_URL,
    },
}


# Utility function to generate an exception when calling an API
def raise_request_exception(url: str, status_code: int, message: str):
    """
    Raise an exception with the formatted error message.

    Args:
        url (str): The URL which was requested.
        status_code (int): The status code of the response.
        message (str): The error message.
    """
    exception = (
        "Failed to retrieve data from URL: {}, status code: {}, messsage: {}".format(
            url, status_code, message
        )
    )
    raise Exception(exception)


def get_request(
    url: str,
    verify: Optional[bool] = None,
    headers: Optional[dict] = None,
    auth: Optional[dict] = None,
) -> requests.Response:
    """
    Get an HTTP GET request to the specified URL.

    Args:
    url (str): The URL to send the GET request to.
    verify (bool, optional): A flag to enable or disable SSL verification. Defaults to None.
    headers (dict, optional): Additional headers to be included in the request. Defaults to None.

    Returns:
    requests.Response: The response to the GET request.

    Raises:
    Exception: If the status code of the response is not 200. The exception message includes the URL, status code, and message.
    """
    response = requests.get(url=url, verify=verify, headers=headers, auth=auth)
    if response.status_code != 200:
        raise_request_exception(url, response.status_code, response.text)
    return response


def import_weather():
    response = get_request(url=WEATHER_URL, headers=LOCAL_HEADERS)
    # The first endpoint returns a JSON dictionary as follows:
    # {
    #   "descripcion": "Éxito",
    #   "estado": 0,
    #   "datos": "string",
    #   "metadatos": "string"
    # }
    # We retrieve the URL we actually need from the "datos" key
    final_weather_url = json.loads(response.text).get("datos")
    if final_weather_url:
        weather_content = get_request(url=final_weather_url)
        results = json.loads(weather_content.text)
        # AEMET Data Example
        # {
        #  'idema': '5530E', 'lon': -3.789774, 'fint': '2023-06-29T20:00:00', 'prec': 0.0,
        #  'alt': 560.12, 'vmax': 7.7, 'vv': 4.2, 'dv': 270.0, 'lat': 37.190292, 'dmax': 270.0,
        #  'ubi': 'GRANADA/AEROPUERTO', 'pres': 948.9, 'hr': 31.0, 'stdvv': 0.3, 'ts': 31.0,
        #  'pres_nmar': 1010.7, 'tamin': 31.8, 'ta': 31.9, 'tamax': 33.8, 'tpr': 12.6,
        #  'vis': 16.8, 'stddv': 2.0, 'inso': 4.0
        # }
        aemet_translation = {
            "areaServed": "ubi",
            "temperature": "ta",
            "temperatureMinimum": "tamin",
            "temperatureMaximum": "tamax",
            "atmosphericPressure": "pres",
            "windSpeed": "vmax",
            "windDirection": "dv",
            "snowHeight": "nieve",
            "atmosphericPressureSeaLevel": "pres_nmar",
            "rainTotalSum10": "prec",
        }

        for result in results:
            # Locations from AEMET have names like GRANADA/AEROPUERTO so we need to split them
            temp_location = re.split("-|/", result.get("ubi"))
            if temp_location[0] == CITY_NAME:
                location = {
                    "type": "Point",
                    "coordinates": [result.get("lon"), result.get("lat")],
                }
                # TODO geocode the coordinates into a proper address
                # address = geocode_address(result.get("lat"), result.get("lon"))
                converted_item = SMART_DATA_MODEL.copy()
                converted_item["data"]["name"] = "{}-WO-{}".format(
                    CITY_NAME, result.get("idema")
                )
                converted_item["data"]["location"] = location
                converted_item["data"]["dateObserved"] = datetime.strptime(
                    result.get("fint"), DATE_TIME_FORMAT
                ).strftime(DATE_TIME_ISO_FORMAT)
                if result.get("hr"):
                    converted_item["data"]["relativeHumidity"] = result.get("hr") / 100
                # Get the corresponding attributes from the
                for key in aemet_translation:
                    converted_item["data"][key] = result.get(aemet_translation[key])
                # print(converted_item)


# A DAG (Directed Acyclic Graph) represents the overall workflow or pipeline in Airflow.
# It is a collection of tasks and their dependencies, organised in a directed acyclic graph structure.
# Tasks in a DAG are represented as nodes, and the dependencies between tasks are represented as edges in the graph.
# DAGs define the logical order and flow of tasks, allowing us to specify how different tasks should be executed.
# DAGs provide a high-level view of the entire workflow and help manage the scheduling, monitoring, and execution of tasks.
our_dag = DAG(
    DAG_NAME,
    description="Our Weather Importer",
    schedule_interval=EVERY_15_MINUTES,
    start_date=START_DATE,
    default_args=ARGS,
    catchup=False,
)

# An operator represents an atomic unit of work or a single task within a DAG.
# Operators define the actual actions or operations that need to be performed as part of a task.
# There are various types of operators available in Airflow, such as BashOperator, PythonOperator, SQLOperator, etc., each designed for different types of tasks.
# Operators encapsulate the logic and functionality of a task, specifying what needs to be done, such as running a script, executing a SQL query, transferring files, etc.
# Each operator performs a specific action and can be considered as a building block within a DAG, responsible for executing a particular task.
weather_importer = PythonOperator(
    task_id="our_weather_importer",
    python_callable=import_weather,
    provide_context=True,
    dag=our_dag,
)
