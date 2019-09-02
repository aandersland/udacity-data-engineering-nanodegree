import subprocess
import configparser

config = configparser.ConfigParser()
config.read('configs.cfg')

venv_path = f"{config['GENERAL']['BASE_DIRECTORY']}/" \
            f"{config['GENERAL']['VENV_FOLDER']}"
airflow_path = f"{config['GENERAL']['BASE_DIRECTORY']}/" \
               f"{config['GENERAL']['AIRFLOW_FOLDER']}"

subprocess.run(
    f'export AIRFLOW_HOME={airflow_path} && airflow scheduler',
    env={'PATH': venv_path}, shell=True)