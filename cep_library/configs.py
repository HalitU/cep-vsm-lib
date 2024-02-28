import os

env_mongo_host = os.getenv('MONGO_ONE_HOST') if os.getenv('MONGO_ONE_HOST') is not None else "127.0.0.1"
env_mongo_port = os.getenv('MONGO_ONE_PORT') if os.getenv('MONGO_ONE_PORT') is not None else "27017"
data_carry_enabled = bool(os.getenv('DATA_CARRY_ENABLED')) if os.getenv('DATA_CARRY_ENABLED') is not None else False
data_operation_timeout = int(os.getenv('DATA_OPERATION_TIMEOUT')) if os.getenv('DATA_OPERATION_TIMEOUT') is not None else 30
data_max_connection = int(os.getenv('DATA_MAX_CONNECTION')) if os.getenv('DATA_MAX_CONNECTION') is not None else 30
collection_lifetime = int(os.getenv('COLLECTION_LIFETIME')) if os.getenv('COLLECTION_LIFETIME') is not None else 60
data_carry_window = int(os.getenv('DATA_CARRY_WINDOW')) if os.getenv('DATA_CARRY_WINDOW') is not None else 30

print(f"Is data migration enabled: {data_carry_enabled} , data operation timeout(seconds): {data_operation_timeout} \
      , max allowed db connection count: {data_max_connection}, collection lifetime(seconds): {collection_lifetime} \
          , data carry window: {data_carry_window}")

CP_CPU_LIMIT = float(os.getenv('CP_CPU_LIMIT')) if os.getenv('CP_CPU_LIMIT') is not None else 0.5

print(f"Optimizer CPU percentage limit: {CP_CPU_LIMIT}")

env_server_name = os.getenv('SERVER_NAME') if os.getenv('SERVER_NAME') is not None else "127.0.0.1"
env_server_port = int(os.getenv('SERVER_PORT')) if os.getenv('SERVER_PORT') is not None else 8000
# 0 default 1 use the requested hosts 2 other etc...
env_distribution_type = int(os.getenv('DISTRIBUTION_TYPE')) if os.getenv('DISTRIBUTION_TYPE') is not None else 0
print("Env server name is: ", os.getenv('SERVER_NAME'))

env_host_name = os.getenv('HOST_NAME') if os.getenv('HOST_NAME') is not None else "client_one"
env_host_ip = os.getenv('HOST_IP') if os.getenv('HOST_IP') is not None else "127.0.0.1"
env_host_port = int(os.getenv('HOST_PORT')) if os.getenv('HOST_PORT') is not None else 8080

evaluation_period = int(os.getenv('EVAL_PERIOD_SECS')) if os.getenv('EVAL_PERIOD_SECS') is not None else 30

env_kafka_alteration_topics = os.getenv('KAFKA_ALTERATION_TOPICS')
env_kafka_database_topics = os.getenv('KAFKA_DATABASE_TOPICS')
env_kafka_resource_topics = os.getenv('KAFKA_RESOURCE_TOPICS')
env_kafka_producer_topics = os.getenv('KAFKA_PRODUCER_TOPICS')

kafka_alteration_topics = env_kafka_alteration_topics if env_kafka_alteration_topics is not None else ["events.management.alteration"]
kafka_database_topics = env_kafka_database_topics if env_kafka_database_topics is not None else ["events.management.database"]
kafka_resource_topics = env_kafka_resource_topics if env_kafka_resource_topics is not None else ["events.management.resource"]
kafka_producer_topics = env_kafka_producer_topics if env_kafka_producer_topics is not None else ["events.management.producer"]

task_load_type = int(os.getenv('TASK_LOAD_TYPE')) if int(os.getenv('TASK_LOAD_TYPE')) is not None else 0

cep_multiprocessor_available = bool(os.getenv('CEP_MULTIPROCESSOR_AVAILABLE')) if os.getenv('CEP_MULTIPROCESSOR_AVAILABLE') is not None else False

print(f"Is multiprocessing available: {cep_multiprocessor_available}")

DEBUG_MODE = bool(os.getenv('DEBUG_MODE')) if os.getenv('DEBUG_MODE') is not None else False

# 0 linux on windows, 1 rpi4
OS_TYPE = int(os.getenv('OS_TYPE')) if os.getenv('OS_TYPE') is not None else 0

USE_HISTORIC_DIST_AFTER_N = int(os.getenv('USE_HISTORIC_DIST_AFTER_N')) if os.getenv('USE_HISTORIC_DIST_AFTER_N') is not None else 0

CP_RUNTIME_SECONDS = int(os.getenv('CP_RUNTIME_SECONDS')) if os.getenv('CP_RUNTIME_SECONDS') is not None else 2

CONSTRAINED_TTL = int(os.getenv('CONSTRAINED_TTL')) if os.getenv('CONSTRAINED_TTL') is not None else 1000

AGGREGATE_ENABLED = int(os.getenv('AGGREGATE_ENABLED')) if os.getenv('AGGREGATE_ENABLED') is not None else 0

MQTT_HOST = os.getenv('MQTT_HOST') if os.getenv('MQTT_HOST') is not None else 'mqttserver'
MQTT_PORT = int(os.getenv('MQTT_PORT')) if os.getenv('MQTT_PORT') is not None else 1883

EVAL_ACTIVE = int(os.getenv('EVAL_ACTIVE')) if os.getenv('EVAL_ACTIVE') is not None else 0

# VSM is by default active
DISABLE_VSM = int(os.getenv('DISABLE_VSM')) if os.getenv('DISABLE_VSM') is not None else 0

print(f"Is VSM activated: {DISABLE_VSM == 0}")

MIN_COST_AFTER_N_DIST = int(os.getenv('MIN_COST_AFTER_N_DIST')) if os.getenv('MIN_COST_AFTER_N_DIST') is not None else 2
ENFORCE_PRODUCTION_LOCATION = int(os.getenv('ENFORCE_PRODUCTION_LOCATION')) if os.getenv('ENFORCE_PRODUCTION_LOCATION') is not None else 0

DIFFERENT_DEVICE_READ_PENATLY = float(os.getenv('DIFFERENT_DEVICE_READ_PENATLY')) if os.getenv('DIFFERENT_DEVICE_READ_PENATLY') is not None else 1.5
DEVICE_CHANGE_PENALTY = int(os.getenv('DEVICE_CHANGE_PENALTY')) if os.getenv('DEVICE_CHANGE_PENALTY') is not None else 1
DEVICE_NON_CHANGE_MULTIPLIER = int(os.getenv('DEVICE_NON_CHANGE_MULTIPLIER')) if os.getenv('DEVICE_NON_CHANGE_MULTIPLIER') is not None else 1

MANIPULATE_CP_PARAMS =  int(os.getenv('MANIPULATE_CP_PARAMS')) if os.getenv('MANIPULATE_CP_PARAMS') is not None else 1

DIFFERENCE_BALANCE_WEIGHT = int(os.getenv('DIFFERENCE_BALANCE_WEIGHT')) if os.getenv('DIFFERENCE_BALANCE_WEIGHT') is not None else 3

DIFFERENCE_EQUALIZER_WEIGHT = int(os.getenv('DIFFERENCE_EQUALIZER_WEIGHT')) if os.getenv('DIFFERENCE_EQUALIZER_WEIGHT') is not None else 3

OPT_USE_CRR_VALUES_ONLY = int(os.getenv('OPT_USE_CRR_VALUES_ONLY')) if os.getenv('OPT_USE_CRR_VALUES_ONLY') is not None else 0

MIN_REQUIRED_ACTIVE_VAR = int(os.getenv('MIN_REQUIRED_ACTIVE_VAR')) if os.getenv('MIN_REQUIRED_ACTIVE_VAR') is not None else 1

DEVICE_ACTION_LIMIT = int(os.getenv('DEVICE_ACTION_LIMIT')) if os.getenv('DEVICE_ACTION_LIMIT') is not None else 0

DEVICE_ACTION_MIN_LIMIT = int(os.getenv('DEVICE_ACTION_MIN_LIMIT')) if os.getenv('DEVICE_ACTION_MIN_LIMIT') is not None else 0
DEVICE_INTAKE_MIN_LIMIT = int(os.getenv('DEVICE_INTAKE_MIN_LIMIT')) if os.getenv('DEVICE_INTAKE_MIN_LIMIT') is not None else 0

UTILIZE_PREFERRED_ACTION_COST = int(os.getenv('UTILIZE_PREFERRED_ACTION_COST')) if os.getenv('UTILIZE_PREFERRED_ACTION_COST') is not None else 0
UTILIZE_PREFERRED_ACTION_LIMIT = int(os.getenv('UTILIZE_PREFERRED_ACTION_LIMIT')) if os.getenv('UTILIZE_PREFERRED_ACTION_LIMIT') is not None else 0


print(f"Is action cost limit calculated from preferred numbers: {UTILIZE_PREFERRED_ACTION_COST}, limit: {UTILIZE_PREFERRED_ACTION_LIMIT}")

DATABASE_TYPE = int(os.getenv('DATABASE_TYPE')) if os.getenv('DATABASE_TYPE') is not None else 0

USE_CUSTOM_COSTS = int(os.getenv('USE_CUSTOM_COSTS')) if os.getenv('USE_CUSTOM_COSTS') is not None else 0

REPEAT_ON_NON_STAT = int(os.getenv('REPEAT_ON_NON_STAT')) if os.getenv('REPEAT_ON_NON_STAT') is not None else 0

DATA_LOCALITY_WORKER_ACTION_LIMIT = int(os.getenv('DATA_LOCALITY_WORKER_ACTION_LIMIT')) if os.getenv('DATA_LOCALITY_WORKER_ACTION_LIMIT') is not None else 0

MQTT_QOS_LEVEL = int(os.getenv('MQTT_QOS_LEVEL')) if os.getenv('MQTT_QOS_LEVEL') is not None else 0

if MQTT_QOS_LEVEL > 2: raise Exception("Qos level cannot be higher than 2!!")

print(f"Chosen QoS level is :{MQTT_QOS_LEVEL}")
