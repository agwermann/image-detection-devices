import logging
import time
import subprocess
import sys

# This code generates multiple publishers
# python3 publish-workload.py mytopic 10 1 10 0.1

n_args = 5

if len(sys.argv) < n_args+1:
    print("Missing paremeters: TOPIC_NAME, NUM_MESSAGES, MESSAGE_PERIOD, NUMBER_OF_PUBLISHERS, TIME_BETWEEN_PUBLISHERS")
    exit()

broker = 'localhost'
port = 1883
topic = sys.argv[1]
n_messages = int(sys.argv[2])
message_period = float(sys.argv[3])
n_publishers = int(sys.argv[4])
time_between_publishers = float(sys.argv[5])

processes = []

if __name__ == '__main__':
    for i in range(1, n_publishers):
        process = subprocess.Popen(['python3', 'publisher.py', topic, str(n_messages), str(message_period), '>', 'log.txt'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        processes.append(process)
        time.sleep(time_between_publishers)

    for process in processes:
        process.wait()