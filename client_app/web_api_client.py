from pathlib import Path

import uvicorn
from fastapi import FastAPI
from cep_library.data.database_factory import DatabaseFactory
from raw.producer_factory import RawProducerFactory

import cep_library.configs as configs
from cep_library.cep.cep_client import RegisterCEPClient
from cep_library.raw.raw_data_producer import RawDataProducer
from raw.required_manipulator import RequiredRawManipulator
import client_configs

Path("stats").mkdir(parents=True, exist_ok=True)

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    print ("starting up the web server...")


@app.on_event("shutdown")
def shutdown_event():
    print("Shutting down")


if __name__ == "__main__":
    print("Running the client with parameters: ")
    print("env_host_name: ", configs.env_host_name)
    print("env_host_name: ", configs.env_host_ip)
    print("env_host_name: ", configs.env_host_port)
    print("env_host_name: ", configs.env_mongo_host)
    print("env_host_name: ", configs.env_mongo_port)

    df = DatabaseFactory()
    db = df.get_database_service()
    
    mn = RequiredRawManipulator(length=client_configs.simulation_duration,
                                window_length=client_configs.RAW_REQUIRED_AMOUNT_WINDOW,
                                low_threshold=client_configs.RAW_REQUIRED_AMOUNT_L_THRESHOLD,
                                high_threshold=client_configs.RAW_REQUIRED_AMOUNT_H_THRESHOLD,
                                window_dist_type=client_configs.PREFERRED_WINDOW_DIST_TYPE)

    tasks = RegisterCEPClient(app, db, [mn.run])

    # Register raw data producing sensors
    rpf = RawProducerFactory(db, app)
    sensors = []
    for c_ix, config_type in enumerate(client_configs.raw_producer_types):
        if client_configs.RAW_PRODUCER_ACTIVATED[c_ix] == 1:
            print(f"Preparing a sensor at index {c_ix}")
            raw_output_topic_name = client_configs.raw_output_topic_names[c_ix]
            raw_output_data_name = client_configs.raw_output_data_names[c_ix]
            raw_sleep_duration = client_configs.raw_output_sleep_durations[c_ix]
            raw_length = client_configs.raw_output_lengths[c_ix]
            raw_per_moment = client_configs.raw_output_per_moment_counts[c_ix]
            raw_per_moment_dist_type = client_configs.raw_per_moment_dist_types[c_ix]
            
            raw_output_window_min_threshold = client_configs.RAW_OUTPUT_WINDOW_MIN_THRESHOLDS[c_ix]
            raw_output_window_max_threshold = client_configs.RAW_OUTPUT_WINDOW_MAX_THRESHOLDS[c_ix]
            raw_output_window_random_seed = client_configs.RAW_OUTPUT_WINDOW_RANDOM_SEEDS[c_ix]
            raw_output_window_duration = client_configs.RAW_OUTPUT_WINDOW_DURATION[c_ix]
            raw_expected_sensor_cost = client_configs.RAW_EXPECTED_SENSOR_COST[c_ix]
            
            raw_output_size_min = client_configs.RAW_OUTPUT_SIZE_MIN[c_ix]
            raw_output_size_max = client_configs.RAW_OUTPUT_SIZE_MAX[c_ix]
            
            print(f"[Client] Creating a sensor with params: {raw_length}, {raw_per_moment}, {raw_per_moment_dist_type}, {raw_sleep_duration}")
            
            print(f"[Client] Additional sensors params for threshold production: {raw_output_window_min_threshold}, {raw_output_window_max_threshold}, {raw_output_window_random_seed}, {raw_output_window_duration}, {raw_output_size_min}, {raw_output_size_max}")
            
            cep_sensor_manager:RawDataProducer = rpf.get_sensor(raw_output_topic_name, raw_output_data_name, raw_expected_sensor_cost)
            cep_raw_settings = cep_sensor_manager.get_settings()
            
            sensor_producer = rpf.get_sensor_producer(config_type, raw_length, raw_per_moment, raw_per_moment_dist_type,
                                                    raw_sleep_duration, cep_sensor_manager, app, cep_raw_settings,
                                                    raw_output_window_min_threshold,
                                                    raw_output_window_max_threshold,
                                                    raw_output_window_random_seed,
                                                    raw_output_window_duration, 
                                                    raw_output_size_min, 
                                                    raw_output_size_max)
            sensors.append(sensor_producer)
        
    # Starting the server
    uvicorn.run(app, host=configs.env_host_ip, port=configs.env_host_port, log_level="warning")
