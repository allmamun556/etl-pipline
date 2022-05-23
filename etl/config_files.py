import configparser



config=configparser.ConfigParser()
#build the structure of config files
config.add_section('DB')
config.set('DB','host', 'localhost')
config.set('DB','user', 'postgres')
config.set('DB','password', 'Htmpzkvm7867')


config.add_section('DataBase')
config.set('DataBase','dbname', 'datalake_final')

with open(r"etl/configfile.ini",'w') as configfile:
    config.write(configfile)