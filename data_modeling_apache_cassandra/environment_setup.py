#!/user/bin/python3.6
import os
import shutil
import subprocess


venv_path = 'venv/bin'

print('Starting environment setup script. . .\n')

# # clean up existing environment
if os.path.exists('venv'):
    print('Removing venv. . . ')
    shutil.rmtree('venv')

# create virtual env
if not os.path.exists('venv'):
    os.system('virtualenv -p python3 venv')
    subprocess.run('', env={'PATH': venv_path}, shell=True)

subprocess.run('', env={'PATH': venv_path}, shell=True)

print('\nInstalling curl. . .')
os.system('sudo apt -y install curl')

print('\nPrep for Cassandra install. . .')
os.system(
    'echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | \
    sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list')
os.system(
    'curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -')

print('\nUpdating packages. . .')
os.system('sudo apt-get -y update')

print('\nUpgrading packages. . .')
os.system('sudo apt-get -y upgrade')

print('\nUpdating dist. . .')
os.system('sudo apt-get -y update && sudo apt-get -y dist-upgrade')

print('\nInstalling pip, cassandra packages. . .')
os.system('sudo apt-get -y install python3-pip cassandra cassandra-driver')

if os.path.exists('requirements.txt'):
    print('\nInstalling python requirements')
    subprocess.run('pip3 install -r requirements.txt', env={'PATH': venv_path},
                   shell=True)
else:
    print('\nNo python requirements to install')

print('\n')

subprocess.run('python --version', env={'PATH': venv_path}, shell=True)
print('Cassandra version:')
os.system('cassandra -v')
subprocess.run('pip3 list', env={'PATH': venv_path}, shell=True)

print('\nScript Complete.')
