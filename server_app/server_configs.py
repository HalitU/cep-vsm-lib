import os

IMAGE_FETCH_LIMIT = int(os.getenv('IMAGE_FETCH_LIMIT')) if os.getenv('IMAGE_FETCH_LIMIT') is not None else 1

simulation_duration = int(os.getenv('SIM_DURATION')) if os.getenv('SIM_DURATION') is not None else 300
query_with_timedelta = int(os.getenv('QUERY_WITHIN_TIMEDELTA')) if os.getenv('QUERY_WITHIN_TIMEDELTA') is not None else 1000

print(f"Simulation raw producer settings; sim duration: {simulation_duration}, queries are within timedelta: {query_with_timedelta}, server query type: {4}")

