import configparser
import os
config = configparser.RawConfigParser()
cwd=os.getcwd()
configFilePath = f'{cwd}/config.ini'
config.read(configFilePath)

print(config.defaults()['list_url'])