import configparser
import os

def config(filename):
    if not os.path.isfile(filename):
        raise FileNotFoundError('{} not found'.format(filename))
    config = configparser.ConfigParser()
    config.read(filename)
    return config

if __name__ == '__main__':
    config = config('example.ini')
    for section in config.sections():
        print(section)
        for key in config[section]:
            print(key,config[section][key],type(config[section][key]))
