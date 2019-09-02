#!/user/bin/python3.6
import os
import shutil
import subprocess
import configparser

config = configparser.ConfigParser()
config.read('configs.cfg')

venv_path = f"{config['GENERAL']['BASE_DIRECTORY']}/" \
            f"{config['GENERAL']['VENV_FOLDER']}"
airflow_path = f"{config['GENERAL']['BASE_DIRECTORY']}/" \
               f"{config['GENERAL']['AIRFLOW_FOLDER']}"
dags_path = f"{config['GENERAL']['BASE_DIRECTORY']}/" \
            f"{config['GENERAL']['DAGS_FOLDER']}"
plugins_path = f"{config['GENERAL']['BASE_DIRECTORY']}/" \
               f"{config['GENERAL']['PLUGINS_FOLDER']}"

print('Starting environment setup script. . .\n')

# clean up existing environment
if os.path.exists('venv'):
    print('Removing venv. . . ')
    shutil.rmtree('venv')

if os.path.exists('airflow'):
    print('Removing airflow. . . ')
    shutil.rmtree('airflow')
    os.mkdir(airflow_path)

# create virtual env
if not os.path.exists('venv'):
    os.system('virtualenv -p python3 venv')
    subprocess.run('', env={'PATH': venv_path}, shell=True)

print('\nInstalling pip. . .')
os.system('sudo apt-get -y install python3-pip')

if os.path.exists('requirements.txt'):
    print('\nInstalling python requirements')
    subprocess.run('pip3 install -r requirements.txt', env={'PATH': venv_path},
                   shell=True)
else:
    print('\nNo python requirements to install')

print('\n')

# initialize airflow db
# subprocess.run(f'export AIRFLOW_HOME={airflow_path} && airflow initdb',
#                env={'PATH': venv_path}, shell=True)

# udpate airflow config folder locations
airflow_config = configparser.ConfigParser()
airflow_config.read(f'{airflow_path}/airflow.cfg')
airflow_config.set('core', 'dags_folder', dags_path)
airflow_config.set('core', 'plugins_folder', plugins_path)
airflow_config.set('core', 'load_examples', 'False')

with open(f'{airflow_path}/airflow.cfg', 'w') as configfile:
    airflow_config.write(configfile)

# verify airflow config folders
airflow_config = configparser.ConfigParser()
airflow_config.read(f'{airflow_path}/airflow.cfg')
subprocess.run('pip3 list', env={'PATH': venv_path}, shell=True)
subprocess.run('python --version', env={'PATH': venv_path}, shell=True)
print('airflow dags location: ', airflow_config['core']['dags_folder'])
print('airflow plugins location: ', airflow_config['core']['plugins_folder'])

# reinitialize airflow db with new configs
# subprocess.run(f'export AIRFLOW_HOME={airflow_path} && airflow initdb',
#                env={'PATH': venv_path}, shell=True)

print('\nScript Complete.')
