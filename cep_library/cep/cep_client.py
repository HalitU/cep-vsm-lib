from fastapi import FastAPI
from cep_library.cep.cep_management_service import CEPManagementService
from cep_library.data.database_management_service import BaseDatabaseManagement
import cep_library.configs as configs

class RegisterCEPClient:
    def __init__(self, app: FastAPI, 
                 db: BaseDatabaseManagement, 
                 threads_for_at_least_once_trigger) -> None:
        self.client_management = CEPManagementService(configs.env_host_name, app, db=db, threads_for_at_least_once_trigger=threads_for_at_least_once_trigger)
