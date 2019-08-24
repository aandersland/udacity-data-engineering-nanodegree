import requests
import configparser
import os
import pandas as pd
import wget
import collections
import multiprocessing


def get_airport_list(_config):
    """
    Method to get a list of airport codes from the airport.csv
    :param _config: Configurations
    :return: Parameter list for the write_file_with_get method
    """
    _air_param_list = []
    _inbound_data_folder = f"{_config['GENERAL']['BASE_DIRECTORY']}/" \
                           f"{_config['GENERAL']['INBOUND_FOLDER']}"

    if _config['GENERAL']['SET_AIRPORTS'] == 'INT':
        _airport_list = _config['WEATHER']['INTERNATIONAL_AIRPORTS'].split(',')
    else:
        _airport_list = _config['WEATHER']['ALL_AIRPORTS'].split(',')

    for _airport in _airport_list:
        _params = {'sid': _airport,
                   'sdate': f'{_config["GENERAL"]["START_YEAR"]}-01-01',
                   'edate': f'{_config["GENERAL"]["END_YEAR"]}-12-31',
                   'elems': 'maxt,mint,avgt,pcpn,snow,snwd', 'meta': 'name'}

        # get parameters for function
        _air_param_list.append((_config['URL']['LINK_WEATHER_DATA'],
                                _params,
                                f'{_airport}.json',
                                f'{_inbound_data_folder}/airports'))

    return _air_param_list


def process_weather(_config):
    """
    Method to gather the weather data
    :param _config: Configurations
    """
    _inbound_data_folder = f"{_config['GENERAL']['BASE_DIRECTORY']}/" \
                           f"{_config['GENERAL']['INBOUND_FOLDER']}/airports"

    # setup multiprocessing
    _pool = multiprocessing.Pool()

    # get list of params for method call
    _param_list = get_airport_list(_config)

    if not os.path.exists(_inbound_data_folder):
        os.mkdir(_inbound_data_folder)

    _pool.starmap(write_file_with_get, _param_list)


def process_flights(_config):
    """
    Method to gather the flight data
    :param _config: Configurations
    """

    _inbound_data_folder = f"{_config['GENERAL']['BASE_DIRECTORY']}/" \
                           f"{_config['GENERAL']['INBOUND_FOLDER']}/flights"
    _param_list = []

    for yr in range(int(_config['GENERAL']['START_YEAR']),
                    int(_config['GENERAL']['END_YEAR'])+1):
        _file_name = f"{yr}.{_config['URL']['FLIGHT_DATA_EXTENSION']}"
        _url = f"{_config['URL']['LINK_FLIGHT_DATA']}/{_file_name}"
        _param_list.append((_url, _file_name, _inbound_data_folder))

    if not os.path.exists(_inbound_data_folder):
        os.mkdir(_inbound_data_folder)
    _cpu = int(_config['GENERAL']['CPU_CORES_PARALLEL_PROCESSING'])
    _pool = multiprocessing.Pool(_cpu)
    _pool.starmap(get_url_content_curl, _param_list)


def process_airports(_config):
    """
    Method to gather the airport data
    :param _config: Configurations
    """
    _inbound_data_folder = f"{_config['GENERAL']['BASE_DIRECTORY']}/" \
                           f"{_config['GENERAL']['INBOUND_FOLDER']}"

    _r = get_url_content(_config['URL']['LINK_AIRPORTS_DATA'], None)

    write_file(_r, 'airports.csv', _inbound_data_folder)


def get_url_content(_url, _params):
    """
    Method to get the file content from requests
    :param _url: URL to file
    :param _params: Additional parameters for the request
    :return: A Request response object
    """

    if _params is None:
        r = requests.get(_url, allow_redirects=True, stream=True)
    else:
        r = requests.get(_url, allow_redirects=True, stream=True,
                         params=_params)
    return r


def get_url_content_curl(_url, _file_name, _inbound_data_folder):
    """
    Method to get bz2 flight files
    :param _url: URL for file to download
    :param _file_name: File name to create
    :param _inbound_data_folder: Directory to write files too
    """
    os.system(f"curl {_url} -o {_inbound_data_folder}/{_file_name}")


def write_file(_response, _file_name, _directory):
    """
    Method to write files to a target directory from a request response
    :param _response: Response from a get request
    :param _file_name: Name of new file
    :param _directory: Location file is to be written to
    """
    if not os.path.exists(_directory):
        os.mkdir(_directory)

    if 'no data available' not in _response.text:
        with open(f'{_directory}/{_file_name}', 'wb') as fd:
            for chunk in _response.iter_content(chunk_size=1024):
                fd.write(chunk)
            fd.close()


def write_file_with_get(_url, _params, _file_name, _directory):
    """
    Combined method to get response and write files for multiprocessing
    :param _url: URL to get content from
    :param _params: Parameters to include in request
    :param _file_name: Name of new file
    :param _directory: Directory that files will be written too
    """
    _response = get_url_content(_url, _params)

    if 'no data available' not in _response.text:
        with open(f'{_directory}/{_file_name}', 'wb') as fd:
            for chunk in _response.iter_content(chunk_size=1024):
                fd.write(chunk)
            fd.close()


def main():
    config = configparser.ConfigParser()
    config.read('configs.cfg')

    process_airports(config)
    process_flights(config)
    process_weather(config)


if __name__ == "__main__":
    main()
