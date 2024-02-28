import pickle
import time
from datetime import datetime
from threading import Lock, Thread

import pymongo
from pymongo.errors import PyMongoError

import cep_library.configs as configs
from cep_library.cep.model.cep_task import MongoQuery
from cep_library.data.base_database_management import BaseDatabaseManagement
from cep_library.data.model.database_info_model import DataCarryModel, DatabaseInfoModel
from cep_library.mqtt.mqttconsumer import MQTTConsumer
from cep_library.mqtt.mqttproducer import MQTTProducer


# https://pymongo.readthedocs.io/en/stable/examples/index.html
# https://pymongo.readthedocs.io/en/stable/examples/bulk.html
# https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html?highlight=TTL#pymongo.collection.Collection.create_index
class DatabaseManagementService(BaseDatabaseManagement):
    def __init__(self, host_name, host, port, groupid="", db_inits=None) -> None:
        if configs.DATABASE_TYPE == 1:
            raise Exception("INVALID DATABASE TYPE")
        
        self.connections = {}
        self._host_name = host_name
        self._database_publisher = MQTTProducer(client_id=f"producer_{configs.kafka_database_topics[0]}_{self._host_name}_{groupid}")

        self._database_consumer = MQTTConsumer(
            client_id=f"{configs.kafka_database_topics[0]}_{self._host_name}_{groupid}",
            topic_name=configs.kafka_database_topics,
            target=self.process_database_info,
            target_args=None,
        )

        self.db_conn_lock = Lock()

        dim = DatabaseInfoModel(
            name=self._host_name,
            host=host,
            port=port,
        )
        
        self._db_info_mdoel = pickle.dumps(dim)

        # connect to db
        # https://stackoverflow.com/questions/31030307/why-is-pymongo-3-giving-serverselectiontimeouterror
        db_client = pymongo.MongoClient(
            "mongodb://%s:%s@%s:%s/?authMechanism=DEFAULT"
            % ("root", "password", configs.env_mongo_host, configs.env_mongo_port),
            connect=True,
            directConnection=True,
            maxConnecting=configs.data_max_connection
        )
        db = db_client.cep

        # add self to the dict of connections
        self.connections[self._host_name] = db_client

        # Create other initial connections if given
        if db_inits:
            for db_in in db_inits:
                crr_dim = DatabaseInfoModel(
                    name=db_in,
                    host=db_in,
                    port=configs.env_mongo_port,
                )
                crr_m = pickle.dumps(crr_dim)
                self.process_database_info(crr_m, None)

        if configs.DEBUG_MODE:
            print("[*] connected database name: ", db.name)

        # db management initialized
        self.killed = False

    #region Collections
    
    def create_collection(self, host_name, collection_name, ttl_duration):
        # wait to make sure the connection exists
        self.hold_till_connection_exists(host_name)
        
        conn:pymongo.MongoClient = self.connections[host_name]
        db = conn.get_database('cep')
        # print("[DATABASE] Fetching database...")
        try:
            if collection_name not in db.list_collection_names():
                coll = db.create_collection(collection_name)
                coll.create_index("createdAt", expireAfterSeconds=ttl_duration, name="createdAt_expiration", sparse=True)
                # print("Collection: ", collection_name,  " created at host: ", host_name)
            else:
                # print(f"[DATABASE] Collection {collection_name} already exists.")
                pass
        except:
            pass
        return True
    
    #endregion Collections

    # region manages the database connections

    def hold_till_connection_exists(self, host_name):
        while True:
            if host_name in self.connections:
                # print(f"[DATABASE] connection {host_name} exists.")
                break
            if configs.DEBUG_MODE:
                print("Target database not available yet: ", host_name)
                print("Current connections: ", self.connections)
            self.publish_database_info()
            time.sleep(1.0)

    def process_database_info(self, msg: bytes, args):
        self.db_conn_lock.acquire()
        db_info: DatabaseInfoModel = pickle.loads(msg)

        # connect if not already connected
        # print("Received db connection: ", db_info.name, " ", db_info.host, " ", db_info.port)
        if db_info.name not in self.connections:
            # connect to db
            db_client = pymongo.MongoClient(
                "mongodb://%s:%s@%s:%s" % ("root", "password", db_info.host, db_info.port)
            )
                        
            self.connections[db_info.name] = db_client

            # if a new host comes, it indicates that we should publish back our own
            # connection again for stability.
            self._database_publisher.produce(
                configs.kafka_database_topics[0], self._db_info_mdoel, on_delivery=self.ack
            )

            if configs.DEBUG_MODE:
                print(
                    self._host_name,
                    " connected to another database with connection info: ",
                    db_info.__dict__,
                )
        self.db_conn_lock.release()

    def run(self):
        # Start consuming in the background
        Thread(daemon=True, target=self._database_publisher.run).start()
        Thread(daemon=True, target=self._database_consumer.run).start()

        # Periodically sends hearbeat for database information
        # in case others could not receive it
        Thread(daemon=True, target=self.heartbeat).start()

        # We should not need anything new regards to management after this point
        if configs.DEBUG_MODE:
            print("[*] client database management initialized")

    def heartbeat(self):
        while not self.killed:
            self.publish_database_info()
            time.sleep(1.0)

    def publish_database_info(self):
        # send own database connection info to topic for others to listen
        self._database_publisher.produce(
            configs.kafka_database_topics[0], self._db_info_mdoel, on_delivery=self.ack
        )           

    def close(self):
        self._database_publisher.close()
        self._database_consumer.close()

        try:        
            db_conn:pymongo.MongoClient
            for db_conn in self.connections:
                self.connections[db_conn].close()
        except Exception as e:
            print(f"[DB Close Exception]: {e}")
        
        self.killed = True

    def ack(self, err, msg):
        pass

    # endregion manages the database connections

    # region database operations

    def insert_one(self, data:dict, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):
                # print("inserting to db...: ", data)
                db: pymongo.MongoClient = self.connections[preferred]
                id = db.cep[collection].insert_one(data).inserted_id
                return id
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                print(f"failed with non-timeout error: {exc!r}")

    def insert_many(self, data, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):
                # print("inserting to db...: ", data)
                db: pymongo.MongoClient = self.connections[preferred]
                ids = db.cep[collection].insert_many(data).inserted_ids
                return ids
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                print(f"failed with non-timeout error: {exc!r}")

    def find_one(self, id, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):
                db: pymongo.MongoClient = self.connections[preferred]
                data = db.cep[collection].find_one({"_id": id})
                return data
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                print(f"failed with non-timeout error: {exc!r}")

    def delete_one(self, id, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):
                db: pymongo.MongoClient = self.connections[preferred]
                data = db.cep[collection].delete_one({"_id": id})
                return data
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                print(f"failed with non-timeout error: {exc!r}")

    def delete_many(self, data, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):
                db: pymongo.MongoClient = self.connections[preferred]
                data = db.cep[collection].delete_many(data)
                return data
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                print(f"failed with non-timeout error: {exc!r}")

    def update_one(self, id, collection: str, preferred: str):
        try:
            with pymongo.timeout(configs.data_operation_timeout):
                db: pymongo.MongoClient = self.connections[preferred]
                data = db.cep[collection].update_one({"_id": id})
                return data
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                print(f"failed with non-timeout error: {exc!r}")

    # sample_data for column inclusion-exclusion mycol.find({},{ "_id": 0, "name": 1, "address": 1 }):
    # https://pymongo.readthedocs.io/en/stable/tutorial.html#range-queries
    # https://stackoverflow.com/questions/35145333/how-to-select-a-single-field-in-mongodb-using-pymongo
    # https://stackoverflow.com/questions/8109122/how-to-sort-mongodb-with-pymongo
    # https://stackoverflow.com/questions/28968660/how-to-convert-a-pymongo-cursor-cursor-into-a-dict
    def query(self, data, query: MongoQuery, collection: str, preferred: str):
        # print("running a query")
        try:
            with pymongo.timeout(configs.data_operation_timeout):
                if query.within_timedelta is not None:
                    # print("Updating query with timedelta...")
                    query.query["createdAt"] = {"$gt": datetime.utcnow() - query.within_timedelta}
                # print(query.query)
                # print ("db1")
                # print (self.connections.keys())
                db: pymongo.MongoClient = self.connections[preferred]
                # print ("db2")
                data = db.cep[collection].find(filter=query.query, projection=query.columns, limit=query.limit)
                # print ("db3")
                return list(data)
        except PyMongoError as exc:
            if exc.timeout:
                print(f"block timed out: {exc!r}")
            else:
                print(f"failed with non-timeout error: {exc!r}")

    def aggregate(self):
        pass

    # carries data from one mongo db to another one
    def carry_data(self, data_carry_model:DataCarryModel):    
        # wait until target database can be connected
        self.hold_till_connection_exists(data_carry_model.target_host)
        
        fin_deleted_count = 0
        fin_migrated_count = 0
                    
        # get the data to carry
        source_data = self.query(None, data_carry_model.query, data_carry_model.collection, data_carry_model.source_host)
        
        # save it to the target
        if source_data:
            print("data exists for migration...")
            
        if source_data:
            fin_migrated_count = len(source_data)
            self.insert_many(source_data, data_carry_model.collection, data_carry_model.target_host)
        
        # remove the data from source if required
        if source_data and data_carry_model.remove_source:
            # get the ids and delete them            
            delete_query = {}
            
            res = self.delete_many(delete_query, data_carry_model.collection, data_carry_model.source_host)
            if res:
                fin_deleted_count = res.deleted_count
            if configs.DEBUG_MODE:
                print(f"{res.deleted_count} records deleted from topic collection: {data_carry_model.collection}")

        if not source_data:
            if configs.DEBUG_MODE:
                print("No data to carry over.")

        return fin_deleted_count, fin_migrated_count
    # endregion database operations
