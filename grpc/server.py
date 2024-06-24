import threading
import grpc
from concurrent import futures
import mathdb_pb2
import mathdb_pb2_grpc
import traceback

class MathCache:
    def __init__(self) -> None:
        self.pairs = {}
        self.cache = {}
        self.cache_order = []

        self.lock = threading.Lock()

    def Set(self, key, value):
        with self.lock:
            self.pairs[key] = value
            self.cache.clear()
            self.cache_order.clear()

    def Get(self, key):
        with self.lock:
            if key not in self.pairs:
                raise KeyError
            return self.pairs[key]

    def Add(self, key_a, key_b):
        return self.perform('Add', key_a, key_b)

    def Sub(self, key_a, key_b):
        return self.perform('Sub', key_a, key_b)

    def Mult(self, key_a, key_b):
        return self.perform('Mult', key_a, key_b)

    def Div(self, key_a, key_b):
        return self.perform('Div', key_a, key_b)

    def add(self, key_a, key_b):
        return self.Get(key_a) + self.Get(key_b)

    def sub(self, key_a, key_b):
        return self.Get(key_a) - self.Get(key_b)

    def mult(self, key_a, key_b):
        return self.Get(key_a) * self.Get(key_b)

    def div(self, key_a, key_b):
        return self.Get(key_a) / self.Get(key_b)

    def perform(self, operation, key_a, key_b):
        with self.lock:
            cache_key = (operation, key_a, key_b)
            if cache_key in self.cache:
                self.cache_order.remove(cache_key)
                self.cache_order.append(cache_key)
                return self.cache[cache_key], True

        if operation == 'Add':
            result = self.add(key_a, key_b)
        elif operation == 'Sub':
            result = self.sub(key_a, key_b)
        elif operation == 'Mult':
            result = self.mult(key_a, key_b)
        elif operation == 'Div':
            result = self.div(key_a, key_b)
        
        with self.lock:
            self.cache[cache_key] = result
            self.cache_order.append(cache_key)

            if len(self.cache_order) > 10:
                oldest_key = self.cache_order.pop(0)
                del self.cache[oldest_key]

            return result, False
        
class MathDb(mathdb_pb2_grpc.MathDbServicer):
    def __init__(self):
        self.cache = MathCache()

    def Set(self, request, context):
        try:
            self.cache.Set(request.key, request.value)
            return mathdb_pb2.SetResponse(error="")
        except Exception as e:
            err = traceback.format_exc()
            return mathdb_pb2.SetResponse(error=err)

    def Get(self, request, context):
        try:
            value = self.cache.Get(request.key)
            return mathdb_pb2.GetResponse(value=value, error="")
        except Exception as e:
            err = traceback.format_exc()
            return mathdb_pb2.GetResponse(error=err)

    def Add(self, request, context):
        try:
            value, cache_hit = self.cache.Add(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=value, cache_hit=cache_hit, error="")
        except Exception as e:
            err = traceback.format_exc()
            return mathdb_pb2.BinaryOpResponse(error=err)

    def Sub(self, request, context):
        try:
            value, cache_hit = self.cache.Sub(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=value, cache_hit=cache_hit, error="")
        except Exception as e:
            err = traceback.format_exc()
            return mathdb_pb2.BinaryOpResponse(error=err)

    def Mult(self, request, context):
        try:
            value, cache_hit = self.cache.Mult(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=value, cache_hit=cache_hit, error="")
        except Exception as e:
            err = traceback.format_exc()
            return mathdb_pb2.BinaryOpResponse(error=err)

    def Div(self, request, context):
        try:
            value, cache_hit = self.cache.Div(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=value, cache_hit=cache_hit, error="")
        except Exception as e:
            err = traceback.format_exc()
            return mathdb_pb2.BinaryOpResponse(error=err)

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
    mathdb_pb2_grpc.add_MathDbServicer_to_server(MathDb(), server)
    server.add_insecure_port("[::]:5440")
    server.start()
    server.wait_for_termination()
