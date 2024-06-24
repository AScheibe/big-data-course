FROM python:3.11

RUN apt-get update && apt-get install -y net-tools
RUN pip3 install grpcio==1.60.1 grpcio-tools==1.60.1 --break-system-packages

COPY . .

CMD ["python3", "server.py"]
