import os

# activate virtual environment
activate_this = 'venv/bin/activate_this.py'
exec(open(activate_this).read(), {'__file__': activate_this})

print('Updating packages. . .')
os.system('sudo apt-get -y update')

print('Upgrading packages. . .')
os.system('sudo apt-get -y upgrade')

print('Updating dist. . .')
os.system('sudo apt-get -y update && sudo apt-get -y dist-upgrade')

print('Installing pip, postgres packages. . .')
os.system('sudo apt-get -y install python3-pip postgresql postgresql-contrib '
          'libpq-dev')

if os.path.exists('requirements.txt'):
    print('Installing python requirements')
    os.system('pip3 install -r requirements.txt')
else:
    print('No python requirements to install')

os.system('python --version')
os.system('pip3 --version')
