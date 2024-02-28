import os

raw_producer_type = os.getenv('RAW_PRODUCER_TYPES') if os.getenv('RAW_PRODUCER_TYPES') is not None else ''
if raw_producer_type == '':
    raw_producer_types = None
else:
    raw_producer_types = raw_producer_type.split(',')
    print("raw data producing tasks: ", raw_producer_types)
    raw_producer_types = [int(idd) for idd in raw_producer_types]

raw_output_topic_name = os.getenv('RAW_OUTPUT_TOPIC_NAME') if os.getenv('RAW_OUTPUT_TOPIC_NAME') is not None else ''
if raw_output_topic_name == '':
    raw_output_topic_names = None
else:
    raw_output_topic_names = raw_output_topic_name.split(',')

raw_output_data_name = os.getenv('RAW_OUTPUT_DATA_NAME') if os.getenv('RAW_OUTPUT_DATA_NAME') is not None else ''
if raw_output_data_name == '':
    raw_output_data_names = None
else:
    raw_output_data_names = raw_output_data_name.split(',')

raw_output_lengths = os.getenv('RAW_OUTPUT_LENGTHS') if os.getenv('RAW_OUTPUT_LENGTHS') is not None else ''
if raw_output_lengths == '':
    raw_output_lengths = None
else:
    raw_output_lengths = [int(r) for r in raw_output_lengths.split(',')]

raw_output_sleep_durations = os.getenv('RAW_OUTPUT_SLEEP_DURATIONS') if os.getenv(
    'RAW_OUTPUT_SLEEP_DURATIONS') is not None else ''
if raw_output_sleep_durations == '':
    raw_output_sleep_durations = None
else:
    raw_output_sleep_durations = [int(r) for r in raw_output_sleep_durations.split(',')]

raw_output_per_moment_counts = os.getenv('RAW_OUTPUT_PER_MOMENT_COUNTS') if os.getenv(
    'RAW_OUTPUT_PER_MOMENT_COUNTS') is not None else ''
if raw_output_per_moment_counts == '':
    raw_output_per_moment_counts = None
else:
    raw_output_per_moment_counts = [int(r) for r in raw_output_per_moment_counts.split(',')]

RAW_OUTPUT_WINDOW_MIN_THRESHOLDS = os.getenv('RAW_OUTPUT_WINDOW_MIN_THRESHOLDS') if os.getenv(
    'RAW_OUTPUT_WINDOW_MIN_THRESHOLDS') is not None else ''
if RAW_OUTPUT_WINDOW_MIN_THRESHOLDS == '':
    RAW_OUTPUT_WINDOW_MIN_THRESHOLDS = None
else:
    RAW_OUTPUT_WINDOW_MIN_THRESHOLDS = [int(r) for r in RAW_OUTPUT_WINDOW_MIN_THRESHOLDS.split(',')]

RAW_OUTPUT_WINDOW_MAX_THRESHOLDS = os.getenv('RAW_OUTPUT_WINDOW_MAX_THRESHOLDS') if os.getenv(
    'RAW_OUTPUT_WINDOW_MAX_THRESHOLDS') is not None else ''
if RAW_OUTPUT_WINDOW_MAX_THRESHOLDS == '':
    RAW_OUTPUT_WINDOW_MAX_THRESHOLDS = None
else:
    RAW_OUTPUT_WINDOW_MAX_THRESHOLDS = [int(r) for r in RAW_OUTPUT_WINDOW_MAX_THRESHOLDS.split(',')]

RAW_OUTPUT_WINDOW_RANDOM_SEEDS = os.getenv('RAW_OUTPUT_WINDOW_RANDOM_SEEDS') if os.getenv(
    'RAW_OUTPUT_WINDOW_RANDOM_SEEDS') is not None else ''
if RAW_OUTPUT_WINDOW_RANDOM_SEEDS == '':
    RAW_OUTPUT_WINDOW_RANDOM_SEEDS = None
else:
    RAW_OUTPUT_WINDOW_RANDOM_SEEDS = [int(r) for r in RAW_OUTPUT_WINDOW_RANDOM_SEEDS.split(',')]

RAW_OUTPUT_WINDOW_DURATION = os.getenv('RAW_OUTPUT_WINDOW_DURATION') if os.getenv(
    'RAW_OUTPUT_WINDOW_DURATION') is not None else ''
if RAW_OUTPUT_WINDOW_DURATION == '':
    RAW_OUTPUT_WINDOW_DURATION = None
else:
    RAW_OUTPUT_WINDOW_DURATION = [int(r) for r in RAW_OUTPUT_WINDOW_DURATION.split(',')]

RAW_EXPECTED_SENSOR_COST = os.getenv('RAW_EXPECTED_SENSOR_COST') if os.getenv(
    'RAW_EXPECTED_SENSOR_COST') is not None else ''
if RAW_EXPECTED_SENSOR_COST == '':
    RAW_EXPECTED_SENSOR_COST = None
else:
    RAW_EXPECTED_SENSOR_COST = [int(r) for r in RAW_EXPECTED_SENSOR_COST.split(',')]

RAW_OUTPUT_SIZE_MIN = os.getenv('RAW_OUTPUT_SIZE_MIN') if os.getenv('RAW_OUTPUT_SIZE_MIN') is not None else ''
if RAW_OUTPUT_SIZE_MIN == '':
    RAW_OUTPUT_SIZE_MIN = None
else:
    RAW_OUTPUT_SIZE_MIN = [int(r) for r in RAW_OUTPUT_SIZE_MIN.split(',')]

RAW_OUTPUT_SIZE_MAX = os.getenv('RAW_OUTPUT_SIZE_MAX') if os.getenv('RAW_OUTPUT_SIZE_MAX') is not None else ''
if RAW_OUTPUT_SIZE_MAX == '':
    RAW_OUTPUT_SIZE_MAX = None
else:
    RAW_OUTPUT_SIZE_MAX = [int(r) for r in RAW_OUTPUT_SIZE_MAX.split(',')]

raw_per_moment_dist_types = os.getenv('RAW_PER_MOMENT_DIST_TYPES') if os.getenv(
    'RAW_PER_MOMENT_DIST_TYPES') is not None else ''
if raw_per_moment_dist_types == '':
    raw_per_moment_dist_types = None
else:
    raw_per_moment_dist_types = [int(r) for r in raw_per_moment_dist_types.split(',')]

RAW_PRODUCER_ACTIVATED = os.getenv('RAW_PRODUCER_ACTIVATED') if os.getenv('RAW_PRODUCER_ACTIVATED') is not None else ''
if RAW_PRODUCER_ACTIVATED == '':
    RAW_PRODUCER_ACTIVATED = None
else:
    RAW_PRODUCER_ACTIVATED = [int(r) for r in RAW_PRODUCER_ACTIVATED.split(',')]

RAW_IMAGE_SCALE_PERCENT = int(os.getenv('RAW_IMAGE_SCALE_PERCENT')) if os.getenv('RAW_IMAGE_SCALE_PERCENT') is not None else 3

RAW_REQUIRED_AMOUNT_DIST_ACTIVE = int(os.getenv('RAW_REQUIRED_AMOUNT_DIST_ACTIVE')) if os.getenv('RAW_REQUIRED_AMOUNT_DIST_ACTIVE') is not None else 0
RAW_REQUIRED_AMOUNT_L_THRESHOLD= int(os.getenv('RAW_REQUIRED_AMOUNT_L_THRESHOLD')) if os.getenv('RAW_REQUIRED_AMOUNT_L_THRESHOLD') is not None else 0
RAW_REQUIRED_AMOUNT_H_THRESHOLD= int(os.getenv('RAW_REQUIRED_AMOUNT_H_THRESHOLD')) if os.getenv('RAW_REQUIRED_AMOUNT_H_THRESHOLD') is not None else 0
RAW_REQUIRED_AMOUNT_WINDOW = int(os.getenv('RAW_REQUIRED_AMOUNT_WINDOW')) if os.getenv('RAW_REQUIRED_AMOUNT_WINDOW') is not None else 0

PREFERRED_WINDOW_DIST_TYPE = int(os.getenv('PREFERRED_WINDOW_DIST_TYPE')) if os.getenv('PREFERRED_WINDOW_DIST_TYPE') is not None else 0

simulation_duration = int(os.getenv('SIM_DURATION')) if os.getenv('SIM_DURATION') is not None else 300
