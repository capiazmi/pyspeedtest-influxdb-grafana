import os
import re
import json
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from influxdb_client_3 import InfluxDBClient3, Point
from datetime import datetime

os.environ["INFLUX_TOKEN"] = "kAA4tQKJXaHbMC_BDtKFPW12M_3cFZb1x0PoNRcJmlYJCHd_rULJ1afbj0q0neqb1CEkPE2NDbix-XBUAbKVHw=="
os.environ["INFLUX_ORG"] = "Personal Project for Home Lab"
os.environ["INFLUX_BUCKET"] = "pingLatency"
os.environ["INFLUX_HOST"] = "https://us-east-1-1.aws.cloud2.influxdata.com"

influxToken = os.getenv("INFLUX_TOKEN", "default_token")
influxOrg = os.getenv("INFLUX_ORG", "Personal Project for Home Lab")
influxBucket = os.getenv("INFLUX_BUCKET", "pingLatency")
influxHost = os.getenv("INFLUX_HOST", "https://us-east-1-1.aws.cloud2.influxdata.com")

TEST_INTERVAL = int(60)
TEST_FAIL_INTERVAL = int(30)
DB_RETRY_INTERVAL = int(30)
PRINT_DATA = False
MAX_WORKERS = 5

def logger(level, message):
    print(level, ":", datetime.now().replace(microsecond=0).isoformat(), ":", message)

def format_influx(cliout):
    try:
        data = json.loads(cliout)
        influx_data = [
            {
                'measurement': 'ping',
                'time': data['timestamp'],
                'fields': {
                    "host": data["host"],
                    "packet_loss": str(data["packet_loss"]),
                    "latency_avg": float(data["latency_avg"]),
                    "latency_min": float(data["latency_min"]),
                    "latency_max": float(data["latency_max"]),
                    "IPv4_address": data["IPv4_address"],
                    "IPv6_address": data["IPv6_address"] if data["IPv6_address"] else "",
                    "hostnameTarget": data["hostnameTarget"],
                    "hostname": data["hostname"]
                }
            },
        ]
        return influx_data
    except Exception as e:
        logger("Error", f"Error formatting influx data: {e}")
        return None

def check_ping(hostpingaddr):
    try:
        hostname = hostpingaddr.strip()
        response = os.popen(f"ping -c 8 {hostname}").read()

        if "100% packet loss" in response:
            return {"status": "Host unreachable"}

        packet_loss = re.search(r"(\d+)% packet loss", response)
        if packet_loss:
            packet_loss = packet_loss.group(1)
        else:
            packet_loss = "100"

        latency_stats = re.findall(r"(\d+\.\d+)/(\d+\.\d+)/(\d+\.\d+)/(\d+\.\d+)", response)
        ipv4adr, ipv6adr = None, None

        try:
            ipv4adr = socket.gethostbyname(hostname)
            ipv6adr = socket.getaddrinfo(hostname, None, socket.AF_INET6)[0][4][0]
        except socket.error:
            ipv6adr = None

        timestamp = int(time.time() * 1e9)

        if latency_stats:
            latency_avg, latency_min, latency_max, latency_stddev = latency_stats[0]
            return {
                "timestamp": timestamp,
                "host": hostname,
                "packet_loss": packet_loss,
                "latency_avg": latency_avg,
                "latency_min": latency_min,
                "latency_max": latency_max,
                "IPv4_address": ipv4adr,
                "IPv6_address": ipv6adr,
                "hostnameTarget": hostname,
                "hostname": hostname
            }
        return {"status": "Ping failed"}
    except Exception as e:
        logger("Error", f"Error during ping: {e}")
        return {"status": "Error"}

def ping_and_write_to_db(line, influxdb_client):
    try:
        ping_status = check_ping(line)
        if "status" not in ping_status:
            pingResult = json.dumps(ping_status)
            data = format_influx(pingResult)
            if data:
                retries = 3
                while retries > 0:
                    try:
                        influxdb_client.write(database=influxBucket, record=data)
                        logger("Info", f"Data written to DB for {line.strip()} successfully")
                        if PRINT_DATA:
                            logger("Info", data)
                        break
                    except Exception as e:
                        logger("Error", f"DB write failed for {line.strip()}: {e}")
                        retries -= 1
                        time.sleep(DB_RETRY_INTERVAL)
            else:
                logger("Error", "Failed to format ping data for InfluxDB")
        else:
            logger("Error", f"Ping failed for {line.strip()}: {ping_status.get('status', 'Unknown Error')}")
    except Exception as e:
        logger("Error", f"Error during ping or write to DB for {line.strip()}: {e}")

def main():
    db_initialized = False

    while not db_initialized:
        try:
            influxdb_client = InfluxDBClient3(host=influxHost, token=influxToken, org=influxOrg, database=influxBucket)
            db_initialized = True
            logger("Info", "DB initialization complete")
        except Exception as e:
            logger("Error", f"DB initialization error: {e}")
            time.sleep(DB_RETRY_INTERVAL)

    with open("pinglist.txt", "r") as file:
        lines = file.readlines()

    while True:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(ping_and_write_to_db, line, influxdb_client) for line in lines]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger("Error", f"Error in parallel execution: {e}")

        time.sleep(TEST_INTERVAL)

if __name__ == '__main__':
    logger('Info', 'Pinging to host for InfluxDB started')
    while True:
        try:
            main()
        except Exception as e:
            logger("Critical", f"Main loop crashed: {e}")
            time.sleep(30)
