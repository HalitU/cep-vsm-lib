from cep_library import configs
from cep_library.data.base_database_management import BaseDatabaseManagement
from cep_library.data.database_management_service import DatabaseManagementService


class DatabaseFactory:
    def __init__(self) -> None:
        pass
    
    def get_database_service(self, groupid="") -> BaseDatabaseManagement:
        return DatabaseManagementService(configs.env_host_name, configs.env_mongo_host, configs.env_mongo_port, groupid=groupid)