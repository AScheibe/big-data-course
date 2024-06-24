import grpc
import sys
import threading
import mathdb_pb2
import mathdb_pb2_grpc

class Client:
    def __init__(self, address):
        self.channel = grpc.insecure_channel(address)
        self.stub = mathdb_pb2_grpc.MathDbStub(self.channel)

    def set(self, key, value):
        request = mathdb_pb2.SetRequest(key=key, value=value)
        return self.stub.Set(request)

    def get(self, key):
        request = mathdb_pb2.GetRequest(key=key)
        return self.stub.Get(request)

    def add(self, key_a, key_b):
        request = mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b)
        return self.stub.Add(request)

    def sub(self, key_a, key_b):
        request = mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b)
        return self.stub.Sub(request)

    def mult(self, key_a, key_b):
        request = mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b)
        return self.stub.Mult(request)

    def div(self, key_a, key_b):
        request = mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b)
        return self.stub.Div(request)

def worker(client, f, info):
    with open(f, 'r') as file:
        next(file)  
        for line in file:
            parts = line.strip().split(',')
            operation = parts[0].lower()
            key_a = parts[1]
            key_b = float(parts[2]) if operation == 'set' else parts[2]

            try:
                if operation == 'set':
                    client.set(key_a, key_b)
                else:
                    # check if this can be used
                    response = getattr(client, operation)(key_a, key_b)
                
                    with info['lock']:
                        info['total'] += 1
                        if response.cache_hit:
                            info['hits'] += 1

            except Exception as e:
                print(f"Error in worker: {e}")

def main():
    port = sys.argv[1]
    csv_files = sys.argv[2:]
    
    address = f'localhost:{port}'

    client = Client(address)

    info = {'hits': 0, 'total': 0, 'lock': threading.Lock()}
    threads = []

    for f in csv_files:
        thread = threading.Thread(target=worker, args=(client, f, info))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    hit_rate = info['hits'] / info['total'] if info['total'] > 0 else 0
    print(hit_rate)

if __name__ == '__main__':
    main()
