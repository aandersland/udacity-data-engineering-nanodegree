#!/user/bin/python3.6
import os
import shutil

python_venv_bin = './venv/bin/python3.6'
venv_setup_script = './venv_setup.py'

print('Starting environment setup script. . .')

# # clean up existing environment
if os.path.exists('venv'):
    print('Removing venv. . . ')
    shutil.rmtree('venv')

# create virtual env
if not os.path.exists('venv'):
    os.system('virtualenv -p python3 venv')

# update venv and install libraries
os.system('python3 venv_setup.py')
