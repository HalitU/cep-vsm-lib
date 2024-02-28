import threading
from datetime import datetime, timedelta
from typing import List, Any


class EventModel:
    def __init__(self):
        self.data_id: Any
        self.data: Any
        self.event_date: datetime

class RequiredInputTopics:
    def __init__(self):
        self.order: int
        self.input_topic: str
        self.from_database: bool
        self.type: type
        self.stored_database: str
        self.is_image_read: bool

class InputTopicSingle:
    def __init__(self) -> None:
        self.last_id = None
        self.last_timeout = None
        self.name = None
        self.data = None
        
class InputTopicCounter:
    def __init__(self) -> None:
        self.topics:dict = {}
        self.lock = threading.Lock()
        
    def register_topic(self, topic_name):
        self.topics[topic_name] = InputTopicSingle()
        self.topics[topic_name].name = topic_name
        
    def update_topic_counter(self, topic_name, _id, _ttl, data):
        self.lock.acquire()
        
        current_utc = datetime.utcnow()
        
        data_lifetime_status_type = 5
        if self.topics[topic_name].last_timeout is None:
            data_lifetime_status_type = 2
        else:
            if self.topics[topic_name].last_timeout >= current_utc and self.topics[topic_name].last_timeout < _ttl:
                data_lifetime_status_type = 1
            elif _ttl >= current_utc and self.topics[topic_name].last_timeout < current_utc and self.topics[topic_name].last_timeout < _ttl:
                data_lifetime_status_type = 2
            elif _ttl < current_utc and self.topics[topic_name].last_timeout < current_utc and self.topics[topic_name].last_timeout < _ttl:
                data_lifetime_status_type = 3
            elif self.topics[topic_name].last_timeout > _ttl:
                data_lifetime_status_type = 4
            else:
                data_lifetime_status_type = 5

        if data_lifetime_status_type == 5:
            print(f"Life time status: {data_lifetime_status_type}, incoming ttl: {_ttl}, last timeout: {self.topics[topic_name].last_timeout}, current date: {current_utc}")
        
        # Update the data only if a new one comes, so latest one is the most important
        if self.topics[topic_name].last_timeout is None or self.topics[topic_name].last_timeout < _ttl:
            self.topics[topic_name].last_id = _id
            self.topics[topic_name].last_timeout = _ttl
            self.topics[topic_name].data = data
        
        response = {}
        response_data = {}
        response_valid = True
        v:InputTopicSingle
        val_ctr = 0
        for _, v in self.topics.items():
            if v.last_id != None and v.last_timeout != None and v.last_timeout >= current_utc:
                response[v.name] = v.last_id
                response_data[v.name] = v.data
                val_ctr += 1
            else:
                response_valid = False
                break
        if response_valid == True and val_ctr != len(self.topics.keys()):
            for i in range(30):
                print("INVALID TOPIC LIFE VALIDATION DETECTED!!!!!!!!!!!!!!!!!!!!!!!")
            raise Exception("INVALID TOPIC LIFE VALIDATION DETECTED!!!!!!!!!!!!!!!!!!!!!!!")
        self.lock.release()
        return response, response_data, response_valid, data_lifetime_status_type

class RequiredOutputTopics:
    def __init__(self):
        self.order: int
        self.output_topic: str
        self.to_database: bool = True
        self.type: type
        self.target_database: str

class MongoQuery:
    def __init__(self):
        self.query: dict = {}
        self.columns: dict = {}
        self.sort: dict = []
        self.within_timedelta: timedelta = None
        self.limit: int = 0

class CEPTaskSetting:
    def __init__(self):
        self.action_name: str
        self.arguments: List[object]
        self.created_date: datetime
        self.updated_date: datetime
        self.output_topics: List[RequiredOutputTopics]
        self.required_sub_tasks: List[RequiredInputTopics]
        self.output_enabled: bool
        self.delete_after_consume: bool = True
        self.query: MongoQuery = None
        self.action_path: str
        self.host_name: str
        self.expected_task_cost: int
        self.last_event: bool
        self.additional_files: List[str]

    def __init__(
        self,
        action_name="",
        arguments=None,
        created_date=datetime.utcnow(),
        updated_date=None,
        output_topics=[],
        required_sub_tasks=[],
        output_enabled=True,
        delete_after_consume=False,
        query=None,
        action_path="",
        expected_task_cost=0,
        last_event=False,
        additional_files=[]
    ) -> None:
        self.action_name=action_name
        self.output_topics=output_topics
        self.arguments=arguments
        self.created_date=created_date
        self.updated_date=updated_date
        self.required_sub_tasks=required_sub_tasks
        self.output_enabled=output_enabled
        self.delete_after_consume=delete_after_consume
        self.query=query
        self.action_path = action_path
        self.host_name = ""
        self.expected_task_cost = expected_task_cost
        self.last_event = last_event
        self.additional_files = additional_files

class CEPTask:
    def __init__(self, cep_task_setting: CEPTaskSetting) -> None:
        self.task = None
        self._settings: CEPTaskSetting = cep_task_setting
        self.cpu_eval_started = False
        self.cpu_start_date:datetime = None
        self.cpu_start_hertz:float = None
        self.cpu_start_usage_hertz_total:float = None