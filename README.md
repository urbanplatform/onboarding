# Airflow Weather Importer

This repository contains a sample Airflow DAG file (`our_weather_importer.py`) that retrieves weather data from a specific data provider (AEMET) and converts it into a standardised data model (`WeatherObserved` from SmartDataModels). The DAG is designed to run at regular intervals and import the weather data into the system.

It serves as an onboarding session for some hands-on experience with the Urban Platform data acquisition layer

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Introduction

The Airflow Weather Importer is a data pipeline that leverages Airflow, an open-source platform, to programmatically author, schedule, and monitor workflows. It retrieves weather data from a specific data provider, processes it, and converts it into a standardised data model following the SmartDataModel specifications.

The DAG is scheduled to run at regular intervals (every 15 minutes) and fetches weather observations for a specific city. It uses the provided data provider's API and converts the retrieved data into the `WeatherObserved` SmartDataModel format.

## Prerequisites

Before using this DAG, ensure that you have the following prerequisites installed:

- Python (version 3.6 or higher)
- Airflow (version 2.0 or higher)
- Requests library (install with `pip install requests`)

You also need to obtain an API key from the data provider and replace the placeholder `WEATHER_API_KEY` in the DAG file with the actual API key.

## Getting Started

To get started, follow these steps:

1. Clone this repository to your local machine.
2. Install the required dependencies as mentioned in the prerequisites section.
3. Replace the placeholder `WEATHER_API_KEY` in the DAG file (`our_weather_importer.py`) with your actual API key obtained from the data provider.
4. Customise the DAG settings and variables based on your requirements.

## Usage

To use the Airflow Weather Importer DAG, follow these steps:

1. Copy the `our_weather_importer.py` DAG file to your Airflow DAGs directory.
2. Start the Airflow scheduler and webserver.
3. Access the Airflow web interface and enable the `our_weather_importer` DAG.
4. Adjust the DAG settings and variables if needed, such as the city name, data provider, and scheduling interval.
5. Monitor the DAG execution and check the imported weather data.

Feel free to explore the DAG file and customise it according to your needs. You can modify the data processing logic, add additional tasks, or integrate it with other Airflow components.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request.
Please use `black` for code formatting and `isort`. `ruff` is also welcome as code linter.

## License

This project is licensed under the [MIT License](LICENSE).
