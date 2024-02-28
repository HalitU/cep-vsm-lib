### What is this repo about?

This is a working prototype of the library given in the paper TODO::PAPERNAMEHERE.

Utilizing it is easy however there are a few caveats as the experiments in the paper are done using Raspberry Pi4 devices, and therefore the required library versions, as well as your operating system may not be suitable to run the provided samples directly.

Additionally, if you want to restrict the resource usage of your devices for more realistic scenarios, sometimes it is not enough to only change docker compose settings, but also some system configuration files as well. These can be found easily after searching on web for your corresponding operating system and hardware.

### Table of Contents

- [Utilized Sources](#utilized-sources)
- [Folder Structure](#folder-structure)
    - [Library Files](#cep_library)
    - [Sample Client App](#client_app)
    - [Sample Server (Management Unit) App](#server_app)
    - [Other Operation Files](#ops)
- [How to use it?](#how-to-use-it)
    - [Preparing the Event Codes](#preparing-the-event-codes)
    - [Registering the Codes](#registering-the-codes)
    - [Configurations](#configurations)
    - [Running the System](#running-the-system)
    - [Collecting Statistics](#collecting-statistics)

### Utilized Sources

Eclipse Mosquitto MQTT broker for communication: https://mosquitto.org/

MongoDB: https://hub.docker.com/_/mongo

FastAPI for enabling easy http service calls: https://github.com/tiangolo/fastapi

OpenCV haarcascade smile file used in example case: https://github.com/opencv/opencv/tree/4.x/data/haarcascades Please download it from the source and place it to the following structure: server_app/sample_data/haarcascade_smile.xml. So that the client applications will download it from the path "sample_data/haarcascade_smile.xml" when trying to migrate object detection event. Path can be changed by manipulating server_app/action_storage/car_case/scripts/object_detection_task.py file and the function in server_app/action_storage/car_case/car_case_flow.py called object_detection_task (both files need to change because migration loads the cascade filter once!).

List of libraries within requirements.txt files:

- psutil
- fastapi
- networkx
- numpy
- opencv_python
- pydantic
- pymongo
- Pympler
- requests
- pandas
- uvicorn
- ortools
- paho

### Folder Structure

#### Library Files
cep_library includes the necessary library codes.

management directory has several directories that enable the entire system to run. It has capability of collecting statistics that are used in the distribution methods, which the statistics can be downloaded by an exposed endpoint using FastAPI. It has distribution algorithms that include the four distribution methods discussed within the paper.

data directory enables the virtual shared memory structure. Basically it creates a virtual layer for devices to be able to establish communication with each others MongoDB databases, and do the required read/write operations.

cep directory makes it possible for devices to listen the incoming event migration/update requests, and act accordingly. Simply a migration request involves several steps to download necessary files to local device, import the module and subscribe to the required topics so that the imported method can be triggered. It also collects a decent chunk of statistics that is being sent to the management unit at each evaluation period.

mqtt has the minimal files to establish event communication between IoT devices.

raw directory includes code that can be used by the raw data producing applications such as temperature readings, frame feeds etc. The performance of these sensors are used in the constrained programming optimization.

#### Sample Client App
client_app is a sample client application that includes sample raw data generating codes. Under the raw folder three different data producing sources, audio, video, and simple temperature class files can be seen. These are used during the experiments. There is also a file designed to manipulate the minimum event data requirement threshold called "required_manipulator.py". It simply changes the minimum number of data that needs to be alive when an event gets triggered, setting its parameters within "clien_configs.py" too high can results in too many misses so beware. As this repo is initialized, I set them to 1 which is the minimum value.

#### Sample Server (Management Unit) App
server_app is a sample management unit that includes the example CEP flow case that is discussed within the paper. In order to test how the library works, I have included the file called "trigger_experiment.py" which gets triggered by a fastapi endpoint in the web_api_server.py file. It starts the raw data production and enables the CEP evaluation, so that within the time period setting given in the environment variable, CEP library will start evaluating the performance and adjust distributions according to the chosen distribution model.

Four different distributions are provided, as they are also mentioned in the paper. This distribution choice can be provided within the experiment triggering endpoint mentioned just above.

- 8 => Greedy locality approach
- 21 => Complete round-robin
- 25 => Constrained Programming optimization introduced in the paper.
- 26 => Random distribution, set the history parameter to 1, so that the random distribtion will happen once and keep that until the end of the experiment. Otherwise doing random every period will introduce possible unnecessary migration penalty.

#### Other Operation Files
ops directory includes a base yaml file that shows the different types of configurations.

Please check base.yml and docker-compose.yml for observing many different environment variables that can affect the system in many different ways. Some configurations are discussed under the [Configurations](#configurations) subsection.

### How to use it?

This section gives the necessary information about how to use the library for your own complex event flows.

The provided docker files includes one management device application, and two clients that can run on a device with docker installed. Once the structure is understood, it is easy to realize the setup within multiple devices after configuring the IP settings, provided that your devices can connect each other for the requried ports.

For all sakes and purposes, this is not a production ready code, use it at your own risk.

To see how it all runs on your local device, simple run the following commands (requries docker installed), you can simply run the docker compose itself without targeting specific service, it is up to you.

docker compose -f docker-compose.yml up cep_api_server --build

docker compose -f docker-compose.yml up client_one --build

docker compose -f docker-compose.yml up client_two --build

A warning that since the sample scenario does object detection, it requires a few libraries that might take some time to install for slow machines. Please do check the requirements.txt files under client_app and server_app directories.

For testing the example scenario you do need to find a sound and an video footage, place them where the client_app can reach, and assign their path to the following parameters under docker-compose.yml so that the raw data producing code can access and process them.

      - VIDEO_PATH=some_directories/some_flename.some_extension_like_mp4
      - AUDIO_PATH=some_directories/some_flename.some_extension_like_wav

#### Preparing the Event Codes

Check thet car_case_flow.py file under the server_app/action_storage/car_case directory. There it can be seen that each event can be registered to ManagementServer object, where each registraion requries a CEPTask object with a properly defined CEPTaskSetting object. These are all necessary for system to understand which topics to listen to, which ones to publish to, as well as where the code script for the event is located at. Action_names need to be unique.

#### Registering the Codes

Then web_api_server.py file can be checked to see how this registration actually starts, which is a very simple process. The ManagementServer is initialized, then passed to the code written by developers to register the events defined earlier.

#### Configurations

All important configurations are read from environment variables which is stored in cep_library/configs.py file. These can be manipulated by passing the required environment variables to docker compose files. It is important that the sample files provided are designed to be run locally in the same network, so they have docker names as host names. But within the distributed environment, you would want to place the local IoT device IP that needs to be accessed so that the VSM and other related communications can be established.

There are multiple configurations that are separated around client_configs, server_configs and core configs file. client and server are there just for sample experiment purposes. But to shortly summarize important configurations:

- EVAL_PERIOD_SECS : At what periods the distrubiton evaluation will happen
- COLLECTION_LIFETIME : Lifetime of stored data in VSM
- DATA_CARRY_ENABLED : Enable migration when data target changes
- DATA_CARRY_WINDOW : Maximum age of data to be migrated
- QUERY_WITHIN_TIMEDELTA : Maximum age of data to be queried from VSM
- MQTT_QOS_LEVEL : QoS level 0,1,2
- CP_RUNTIME_SECONDS : Maximum constrained programming runtime before it stops
- SIM_DURATION : Simulation duration
- IMAGE_FETCH_LIMIT : Limits the number of image frames that the events pull, for performance purposes
- USE_HISTORIC_DIST_AFTER_N : Stop evaluating and keep the last distribution after N evaluations.
- CONSTRAINED_TTL : Lifetime of stored data in VSM
- AGGREGATE_ENABLED : Whether to get event data from the event message or from VSM
- MIN_COST_AFTER_N_DIST : Only apply the distribution for CPO if the evaluation provides better cost.
- ENFORCE_PRODUCTION_LOCATION : Forces the raw data to be written to where the raw data producing sensors are located at.
- DIFFERENT_DEVICE_READ_PENATLY : Penalty CPO introduces when the data for an event is read from remote device.
- DEVICE_CHANGE_PENALTY : Penalty CPO introduces when event target device changes.
- DEVICE_NON_CHANGE_MULTIPLIER : Penalty CPO introduces when event target does not change. This is used in combination with DEVICE_CHANGE_PENALTY to enable non-integer cost calculation, as the or-tools support integers only for constrained programming.
- MANIPULATE_CP_PARAMS : CPO can manipulate the optimization settings at each evaluation.
- OPT_USE_CRR_VALUES_ONLY : Whethet to utilize only current period statistics or statistics since the beginning of the device runtime.
- MIN_REQUIRED_ACTIVE_VAR : Minimum number of data that is required to be alive to count an event trigger as hit.
- RAW_IMAGE_SCALE_PERCENT : Scales the sample raw data producing code vide footage. Do not keep high for performance purposes.
- DEVICE_ACTION_LIMIT : Maximum number of code scripts that can be assigned to a device at each distribution.
- DEVICE_ACTION_MIN_LIMIT : Minimum number of code scripts that can be assigned to a device at each distribution.
- DEVICE_INTAKE_MIN_LIMIT : Minimum number of event data targets that can be assigned to a device at each distribution.
- UTILIZE_PREFERRED_ACTION_COST : Each action has a cost assigned to it that can be changed in the server sample app code. This limits the sum of these costs for a device.
- UTILIZE_PREFERRED_ACTION_LIMIT : Whether to enforce the UTILIZE_PREFERRED_ACTION_COST limit or not.
- REPEAT_ON_NON_STAT : Enforces the CPO to repeate if no optimal or feasible solution is found.
- DATA_LOCALITY_WORKER_ACTION_LIMIT : Limits the maximum number of code scripts assigned when greedy distribution is utilized.
- DISTRIBUTION_TYPE : Type of the distribution algorithm, 8,21,25,26
- SERVER_NAME : IP of the management unit
- SERVER_PORT : Port of the management unit
- MQTT_HOST : MQTT IP
- MQTT_PORT : MQTT port
- DATA_OPERATION_TIMEOUT : Timeout for VSM operations
- DATA_MAX_CONNECTION : Number of simulataneous connections from a single app allowed to the VSM
- EVAL_ACTIVE : Starts the evaluation when this value is true
- NOISE_SIZE : Introduces noise to sample raw producing data
- MONGO_ONE_HOST : Mongo IP
- MONGO_ONE_PORT : Mongo port
- HOST_NAME : Unique client identifier
- HOST_IP : Client IP
- HOST_PORT : Client port
- VIDEO_PATH : For the sample experiment the video path
- AUDIO_PATH : For the sample experiment the audio path
- RAW_PRODUCER_ACTIVATED : Enables producing the example raw data
- RAW_PRODUCER_TYPES : Different types of producers exists
- RAW_OUTPUT_TOPIC_NAME : Topics that the raw data producers publish to, check the sample codes.
- RAW_OUTPUT_DATA_NAME : Identifier of the output data, check the sample codes.
- RAW_OUTPUT_LENGTHS : How much data will be produced at each interval, check the sample codes.
- RAW_OUTPUT_SLEEP_DURATIONS : The interval of producing data, check the sample codes.
- RAW_OUTPUT_PER_MOMENT_COUNTS : How much data will be produced at each interval, check the sample codes.
- RAW_PER_MOMENT_DIST_TYPES : Data can be produced according to different distributions, check the sample codes.
- RAW_OUTPUT_WINDOW_MIN_THRESHOLDS : Minimum number of data to produce at each window, check the sample codes.
- RAW_OUTPUT_WINDOW_MAX_THRESHOLDS : Maximum number of data to produce at each window, check the sample codes.
- RAW_OUTPUT_WINDOW_RANDOM_SEEDS : Random seeds for observing similar results, check the sample codes.
- RAW_OUTPUT_WINDOW_DURATION : Duration of a window, check the sample codes.
- RAW_REQUIRED_AMOUNT_DIST_ACTIVE : Whether to use required data count manipulation or not
- RAW_REQUIRED_AMOUNT_L_THRESHOLD : Minimum number of data that needs to be alive to trigger an event
- RAW_REQUIRED_AMOUNT_H_THRESHOLD : Maximum number of data that needs to be alive to trigger an event
- RAW_REQUIRED_AMOUNT_WINDOW : Window size for each manipulation
- PREFERRED_WINDOW_DIST_TYPE : Distribution of the manipulation between L and H limits.
- RAW_OUTPUT_SIZE_MIN : Minimum multipler for temperature data so that data size can be manipulated
- RAW_OUTPUT_SIZE_MAX : Maximum multipler for temperature data so that data size can be manipulated

#### Running the System

Run the following codes, which basically gets a python base image, installs the necessary libraries, then just triggers the web_api_client.py or web_api_server.py codes. You will see a log on the server side when the client manages to connects to it successfully.

docker compose -f docker-compose.yml up cep_api_server --build

docker compose -f docker-compose.yml up client_one --build

docker compose -f docker-compose.yml up client_two --build

#### Collecting Statistics

For research purposes I have tried to calculate a decent chunk of statistics from the IoT devices. These can be downloaded using the FastAPI endpoint from the management (server) application.
