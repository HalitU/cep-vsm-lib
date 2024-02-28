from datetime import datetime, timedelta
import pymongo
from cep_library.cep.model.cep_task import (
    CEPTask,
    CEPTaskSetting,
    MongoQuery,
    RequiredInputTopics,
    RequiredOutputTopics,
)
from cep_library.management.server.management_server import ManagementServer
import server_configs

class CarCaseFlow:
    def __init__(self) -> None:
        pass

    def get_mongo_query(self, read_timedelta = server_configs.query_with_timedelta, limit:int = 0):
        mongo_query = MongoQuery()
        mongo_query.columns = {"_id": 0, "data": 1, "initDate": 1}
        mongo_query.sort = [("createdAt", pymongo.DESCENDING)]
        mongo_query.query = {
            "createdAt": {
                "$gt": datetime.utcnow()
                + timedelta(milliseconds=read_timedelta)
            }
        }
        mongo_query.within_timedelta = timedelta(
            milliseconds=read_timedelta
        )
        mongo_query.limit = limit
        return mongo_query

    def register_tasks(self, manager: ManagementServer):
        manager.register_task(self.distance_task())
        manager.register_task(self.heartbeat_task())
        manager.register_task(self.image_processor_task())
        manager.register_task(self.nearby_road_task())
        manager.register_task(self.object_detection_task())
        manager.register_task(self.maintenance_guesser_task())
        manager.register_task(self.destination_calculator_task())
        manager.register_task(self.slowdown_warning_task())
        manager.register_task(self.gas_alarm_predictor_task())
        manager.register_task(self.sudden_stop_trigger_task())

    def distance_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.dist_1.input"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.dist_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="distance_task",
            action_path="action_storage/car_case/scripts/distance_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query(),
            expected_task_cost=1
        )
        return CEPTask(cep_task_setting=cep_action_settings)

    def heartbeat_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.heartbeat_1.input"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.heartbeat_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="heartbeat_task",
            action_path="action_storage/car_case/scripts/heartbeat_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query(),
            expected_task_cost=1
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    def image_processor_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.image_1.input"
        rit_one.from_database = True
        rit_one.is_image_read = True

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.image_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="image_processor_task",
            action_path="action_storage/car_case/scripts/image_processor_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query(limit=server_configs.IMAGE_FETCH_LIMIT),
            expected_task_cost=100
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    def nearby_road_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.road_1.input"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.road_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="nearby_road_task",
            action_path="action_storage/car_case/scripts/nearby_road_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query(),
            expected_task_cost=1
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    #
    def object_detection_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.dist_1.output"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        rit_two = RequiredInputTopics()
        rit_two.input_topic = "events.image_1.output"
        rit_two.from_database = True
        rit_two.is_image_read = True
        
        rit_three = RequiredInputTopics()
        rit_three.input_topic = "events.heartbeat_1.output"
        rit_three.from_database = True     
        rit_three.is_image_read = False           

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.object_1.output"
        out_one.to_database = True

        additional_files = ["sample_data/haarcascade_smile.xml"]

        cep_action_settings = CEPTaskSetting(
            action_name="object_detection_task",
            action_path="action_storage/car_case/scripts/object_detection_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one,rit_two,rit_three],
            query=self.get_mongo_query(limit=server_configs.IMAGE_FETCH_LIMIT),
            expected_task_cost=100,
            additional_files=additional_files
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    def maintenance_guesser_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.road_1.output"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.maintenance_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="maintenance_guesser_task",
            action_path="action_storage/car_case/scripts/maintenance_guesser_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query(),
            expected_task_cost=1
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    def destination_calculator_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.road_1.output"
        rit_one.from_database = True
        rit_one.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.destination_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="destination_calculator_task",
            action_path="action_storage/car_case/scripts/destination_calculator_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one],
            query=self.get_mongo_query(),
            expected_task_cost=1
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    def slowdown_warning_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.destination_1.output"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        rit_two = RequiredInputTopics()
        rit_two.input_topic = "events.maintenance_1.output"
        rit_two.from_database = True     
        rit_two.is_image_read = False   

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.slowdown_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="slowdown_warning_task",
            action_path="action_storage/car_case/scripts/slowdown_warning_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one,rit_two],
            query=self.get_mongo_query(),
            expected_task_cost=1
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    def gas_alarm_predictor_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.destination_1.output"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        rit_two = RequiredInputTopics()
        rit_two.input_topic = "events.maintenance_1.output"
        rit_two.from_database = True    
        rit_two.is_image_read = False

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.gas_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="gas_alarm_predictor_task",
            action_path="action_storage/car_case/scripts/gas_alarm_predictor_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one,rit_two],
            query=self.get_mongo_query(),
            expected_task_cost=1,
            last_event=True
        )
        return CEPTask(cep_task_setting=cep_action_settings)
    
    def sudden_stop_trigger_task(self):
        rit_one = RequiredInputTopics()
        rit_one.input_topic = "events.slowdown_1.output"
        rit_one.from_database = True
        rit_one.is_image_read = False
        
        rit_two = RequiredInputTopics()
        rit_two.input_topic = "events.object_1.output"
        rit_two.from_database = True   
        rit_two.is_image_read = True
        
        rit_three = RequiredInputTopics()
        rit_three.input_topic = "events.dist_1.output"
        rit_three.from_database = True          
        rit_three.is_image_read = False 

        out_one = RequiredOutputTopics()
        out_one.output_topic = "events.stop_1.output"
        out_one.to_database = True

        cep_action_settings = CEPTaskSetting(
            action_name="sudden_stop_trigger_task",
            action_path="action_storage/car_case/scripts/sudden_stop_trigger_task",
            created_date=datetime.utcnow(),
            output_enabled=True,
            output_topics=[out_one],
            required_sub_tasks=[rit_one,rit_two,rit_three],
            query=self.get_mongo_query(),
            expected_task_cost=1,
            last_event=True
        )
        return CEPTask(cep_task_setting=cep_action_settings)    