from typing import List

from cep_library.cep.model.cep_task import CEPTask


class DataMigrationModel:
    def __init__(self):
        self.topic: str
        self.target_host: str
        self.current_host: str

class TaskAlterationModel:
    def __init__(self):
        self.host: str
        self.job_name: str
        self.activate: bool
        self.cep_task: CEPTask
        self.migration_requests: List[DataMigrationModel]

    def __init__(self, **entries):
        self.__dict__.update(entries)
        