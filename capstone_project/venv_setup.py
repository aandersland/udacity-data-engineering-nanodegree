#!/user/bin/python3.6
import os
import shutil
import subprocess

venv_path = 'venv/bin'

print('Starting environment setup script. . .\n')

# clean up existing environment
if os.path.exists('venv'):
    print('Removing venv. . . ')
    shutil.rmtree('venv')

# if os.path.exists('airflow'):
#     print('Removing airflow. . . ')
#     shutil.rmtree('airflow')

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

subprocess.run('python --version', env={'PATH': venv_path}, shell=True)
subprocess.run('pip3 list', env={'PATH': venv_path}, shell=True)

print('\nScript Complete.')
