# Use p5-base as the base image
FROM p5-base

# Start the Spark worker and connect to the Spark boss
CMD ./spark-3.5.1-bin-hadoop3/sbin/start-worker.sh spark://boss:7077 -c 1 -m 512M && sleep infinity

