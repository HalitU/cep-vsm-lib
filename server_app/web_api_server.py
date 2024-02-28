from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from typing import List

import cep_library
from action_storage.car_case.car_case_flow import CarCaseFlow
from trigger_experiment import TriggerExperiments, SimVals

Path("stats").mkdir(parents=True, exist_ok=True)

app = FastAPI()

server_management = cep_library.ManagementServer(app)


@app.post("/start_simulation/")
def statistics(algorithm_number: List[int] = [1], hist_data_after: int = 0):
    global server_management

    print(f"[SERVER] Setting history config as: {hist_data_after}")
    cep_library.configs.USE_HISTORIC_DIST_AFTER_N = hist_data_after
    sim_vals = SimVals()
    tae = TriggerExperiments()
    tae.run(algorithm_number,
            file_reset_func=server_management.reset_stat_files,
            periodic_evaluation=server_management.periodic_evaluation_detail,
            producers=server_management.producers,
            sim_vals=sim_vals)

    print("Experiment is over,  downloading results...")

    return FileResponse(path='current_run_results.zip', filename="current_run_results.zip")

if __name__ == "__main__":
    print(f"Registering [CarCaseFlow] tasks")
    tasks = CarCaseFlow()
    tasks.register_tasks(server_management)

    uvicorn.run(app,
                host=cep_library.configs.env_server_name,
                port=cep_library.configs.env_server_port,
                log_level="warning")
