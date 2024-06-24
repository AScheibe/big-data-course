# Use p5-base as the base image
FROM p5-base

# Expose the Spark boss port
EXPOSE 7077

# Start the Spark boss
CMD ./spark-3.5.1-bin-hadoop3/sbin/start-master.sh && sleep infinity


