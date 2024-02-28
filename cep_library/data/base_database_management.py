from cep_library import configs
from cep_library.cep.model.cep_task import MongoQuery
from cep_library.data.model.database_info_model import DataCarryModel


class BaseDatabaseManagement:
    def __init__(self, host_name, host, port, groupid="", db_inits=None) -> None:
        pass
        
    def create_collection(self, host_name, collection_name, ttl_duration):
        if configs.DEBUG_MODE:
            print("hmmmmm")
        raise NotImplementedError()

    def hold_till_connection_exists(self, host_name):
        raise NotImplementedError()

    def process_database_info(self, msg: bytes, args):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()

    def heartbeat(self):
        raise NotImplementedError()

    def publish_database_info(self):
        raise NotImplementedError()
    
    def close(self):
        raise NotImplementedError()

    def ack(self, err, msg):
        raise NotImplementedError()

    def insert_one(self, data:dict, collection: str, preferred: str):
        raise NotImplementedError()

    def insert_many(self, data, collection: str, preferred: str):
        raise NotImplementedError()

    def find_one(self, id, collection: str, preferred: str):
        raise NotImplementedError()

    def delete_one(self, id, collection: str, preferred: str):
        raise NotImplementedError()

    def delete_many(self, data, collection: str, preferred: str):
        raise NotImplementedError()

    def update_one(self, id, collection: str, preferred: str):
        raise NotImplementedError()

    def query(self, data, query: MongoQuery, collection: str, preferred: str):
        raise NotImplementedError()

    def aggregate(self):
        raise NotImplementedError()

    # carries data from one mongo db to another one
    def carry_data(self, data_carry_model:DataCarryModel):    
        raise NotImplementedError()