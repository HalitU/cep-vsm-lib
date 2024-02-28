from typing import List

from cep_library.cep.model.cep_task import RequiredOutputTopics


class RawUpdateEvent:
    def __init__(self):
        self.db_host: str
        self.db_port: int
        self.producer_name: str
        self.output_topics: List[RequiredOutputTopics]
        self.raw_name: str
     