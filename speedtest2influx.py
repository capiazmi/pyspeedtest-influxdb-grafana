import time
import json
import subprocess
import os

from influxdb_client_3 import InfluxDBClient3, Point
from datetime import datetime

# this is programmed 
# Speedtest Settings
TEST_INTERVAL = int(600)  # Time between tests (in seconds).
TEST_FAIL_INTERVAL = int(60)  # Time before retrying a failed Speedtest (in seconds).
DB_RETRY_INTERVAL = int(60) # Time before retrying a failed data upload.
DB_DATABASE = 'influxdbbucketname'
PRINT_DATA = "False" # Do you want to see the results in your logs? Type must be str. Will be converted to bool.
token = "changetoinfluxdbToken"
org = "influxdborganisation"
host = "hostnameofinfluxdb"

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")

def logger(level, message):
    print(level, ":", datetime.now().strftime("%d/%m/%Y %H:%M:%S"), ":", message)
    
def format_influx(cliout):
    data = json.loads(cliout)
    influx_data = [
        {
            'measurement': 'ping',
            'time': data['timestamp'],
            'fields': {
                'jitter': float(data['ping']['jitter']),
                'latency': float(data['ping']['latency']),
                'low': float(data['ping']['low']),
                'high': float(data['ping']['high'])
            }
        },
        {
            'measurement': 'download',
            'time': data['timestamp'],
            'fields': {
                # Byte to Megabit
                'bandwidth': data['download']['bandwidth'] / 125000,
                'bytes': data['download']['bytes'],
                'elapsed': data['download']['elapsed']
            }
        },
        {
            'measurement': 'upload',
            'time': data['timestamp'],
            'fields': {
                # Byte to Megabit
                'bandwidth': data['upload']['bandwidth'] / 125000,
                'bytes': data['upload']['bytes'],
                'elapsed': data['upload']['elapsed']
            }
        },
        {
            'measurement': 'packetLoss',
            'time': data['timestamp'],
            'fields': {
                'packetLoss': float(data.get('packetLoss', 0.0))
            }
        },
        {
            'measurement': 'isp',
            'time': data['timestamp'],
            'fields': {
                'isp': data.get('isp')
            }
        },
        {
            'measurement': 'interface',
            'time': data['timestamp'],
            'fields': {
                'internalIp': data['interface']['internalIp'],
                'name': data['interface']['name'],
                'isVpn': data['interface']['isVpn'],
                'externalIp': data['interface']['externalIp'],
            }
        },
        {
            'measurement': 'server',
            'time': data['timestamp'],
            'fields': {
                'id': data['server']['id'],
                'host': data['server']['host'],
                'port': data['server']['port'],
                'name': data['server']['name'],
                'location': data['server']['location'],
                'country': data['server']['country'],
                'ip': data['server']['ip']
            }
        },
        {
            'measurement': 'result',
            'time': data['timestamp'],
            'fields': {
                'url': data['result']['url']
            }
        }
    ]
    return influx_data

def main():
    db_initialized = False
    
    while(db_initialized == False):
        try:
            influxdb_client = InfluxDBClient3(host=host, token=token, org=org, database=DB_DATABASE)
        except:
            logger("Error","DB initialization  error")
            time.sleep(int(DB_RETRY_INTERVAL))
        else:
            logger("Info", "DB initialization complete")
            db_initialized = True
            
    while(1):
        speedtest = subprocess.run(["speedtest", "--accept-license", "--accept-gdpr", "-f", "json", "-s", "19302"], capture_output=True)
        
        print(speedtest.stdout)
        
        
        if speedtest.returncode == 0:  # Speedtest was successful.
            data = format_influx(speedtest.stdout)
            logger("Info", "Speedtest successful")
            try:
                influxdb_client.write(database=DB_DATABASE, record=data)
                logger("Info", "Data written to DB successfully")
                if str2bool(PRINT_DATA) == True:
                    logger("Info", data)
                time.sleep(TEST_INTERVAL)
            except:
                logger("Error", "Data write to DB failed")
                time.sleep(TEST_FAIL_INTERVAL)
        else:  # Speedtest failed.
            logger("Error", "Speedtest failed")
            logger("Error", speedtest.stderr)
            logger("Info", speedtest.stdout)
            time.sleep(TEST_FAIL_INTERVAL)
            
if __name__ == '__main__':
    logger('Info', 'Speedtest CLI Data Logger to InfluxDB started')
    main()
