import grpc
import station_pb2
import station_pb2_grpc
from concurrent import futures
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import Unavailable
from cassandra import ConsistencyLevel

cluster = Cluster(["p6-db-1", "p6-db-2", "p6-db-3"])
cass = cluster.connect('weather')

class Station_Record:
    def __init__(self, tmin, tmax):
        self.tmin = tmin 
        self.tmax = tmax


cluster.register_user_type('weather', 'station_record', Station_Record)
class StationServicer(station_pb2_grpc.StationServicer):

    def RecordTemps(self, request, context):
        record = Station_Record(request.tmin,request.tmax)
        insert_statement = cass.prepare("""INSERT INTO weather.stations (id, date, record)
                                    VALUES (?, ?, ?)""")
        insert_statement.consistency_level = ConsistencyLevel.ONE
        try:
            cass.execute(insert_statement, 
                (request.station, request.date, record))
            return station_pb2.RecordTempsReply(error="")

        except Unavailable as e:
           error = f"need {e.required_replicas} replicas, but only have {e.alive_replicas}"
        except NoHostAvailable as e:
            for i in e.errors.values():
                if type(i)== Unavailable:
                    error = f"need {i.required_replicas} replicas, but only have {i.alive_replicas}"
        except Exception as e:
            error = traceback.format_exc()

        return station_pb2.RecordTempsReply(error = error)

    def StationMax(self, request, context):
        max_temp = -1000
        max_statement = cass.prepare("""SELECT MAX(record.tmax) FROM weather.stations WHERE id = ?""")
        max_statement.consistency_level = ConsistencyLevel.THREE
        try:
            result = cass.execute(max_statement, (request.station, ))
            max_temp = result.one()[0]
            return station_pb2.StationMaxReply(error="", tmax=max_temp)
        except Unavailable as e:
           error = f"need {e.required_replicas} replicas, but only have {e.alive_replicas}"
        except NoHostAvailable as e:
            for i in e.errors.values():
                if type(i)== Unavailable:
                    error = f"need {i.required_replicas} replicas, but only have {i.alive_replicas}"
        except Exception as e:
            error = traceback.format_exc()

        return station_pb2.StationMaxReply(error = error) 

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    station_pb2_grpc.add_StationServicer_to_server(StationServicer(), server)
    server.add_insecure_port('[::]:5440')
    server.start()
    server.wait_for_termination()
