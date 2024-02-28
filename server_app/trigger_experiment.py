from datetime import datetime
import sys
import requests
import os
from pathlib import Path
from typing import List
import cep_library.configs as configs
from threading import Thread
import shutil
import time

from cep_library.raw.model.raw_settings import RawSettings
import server_configs

class SimVals:
    def __init__(self) -> None:
        self.sim_trigger_time:str = "NAAAH"


class TriggerExperiments:
    def __init__(self) -> None:
        pass
    
    def run(self, algorithm_number:List[int]=[1], task_weight:List[int]=[0], file_reset_func=None, periodic_evaluation=None, sim_vals: SimVals = None, producers:dict = None):
        Path("/current_run_results").mkdir(parents=True, exist_ok=True)
        
        for algorimth_no in algorithm_number:
            
            os.environ['DISTRIBUTION_TYPE']=str(algorimth_no)
            configs.env_distribution_type = algorimth_no
            configs.EVAL_ACTIVE = 1
            
            for task_type in task_weight:
                print("Unlocking devices for simulations...")
                                
                time.sleep(configs.evaluation_period * 2)
                
                crr_experiment_folder = "/current_run_results/"+str(algorimth_no)+"_"+str(task_type)
                Path(crr_experiment_folder).mkdir(parents=True, exist_ok=True)
                
                sim_vals.sim_trigger_time = datetime.utcnow().strftime("%m.%d.%Y %H.%M.%S.%f")
                
                # Call each client async and wait until all of them finishes
                thread_list = []
                for p in producers:
                    settings:RawSettings = producers[p]
                    thread_list.append(Thread(daemon=True, target=self.run_device, 
                                              kwargs={
                                                  'host': settings.producer_name, 
                                                  'port': settings.producer_port, 
                                                  'data_name': settings.raw_data_name}))
                
                for t in thread_list:
                    t.start()
                    
                print("Sensors triggered...")
                    
                # for t in thread_list:
                #     t.join(timeout=300.0)
                    
                def timed_join_all(threads, timeout):
                    start = cur_time = time.time()
                    while cur_time <= (start + timeout):
                        for thread in threads:
                            if not thread.is_alive():
                                thread.join()
                        time.sleep(1)
                        cur_time = time.time()        
                        
                timed_join_all(thread_list, server_configs.simulation_duration)
                    
                # Trigger statistic collection one last time to make sure current results are collected
                periodic_evaluation()
                    
                # Carry the current statistic files to another directory     
                print("Experiment is over, accumulating the results...")
                
                shutil.copytree("stats", crr_experiment_folder, dirs_exist_ok=True)                      
                    
                print("Triggering stop for all producers...")
                
                for p in producers:
                    settings:RawSettings = producers[p]
                    thread_list.append(Thread(daemon=True, target=self.stop_device, 
                                              kwargs={
                                                  'host': settings.producer_name, 
                                                  'port': settings.producer_port, 
                                                  'data_name': settings.raw_data_name}))
                    
                print("Waiting 210 seconds until the next evaluation cycle to get latest results...")
                
                if len(algorithm_number) == 1:
                    break
                
                time.sleep(210)
                                                                
                # Rest the stat files
                file_reset_func()

        # Zip the results file
        print("Zipping results...")
        
        shutil.make_archive('current_run_results', 'zip', "/current_run_results")
        configs.EVAL_ACTIVE = 0
                
    def stop_device(self, host, port, data_name):
        url = f'http://{host}:{port}/stop_sampling/{data_name}'
        response = requests.get(url=url)  
        if response.status_code != 200:
            raise Exception("Status is NOT 200!")              
                
    def run_device(self, host, port, data_name):
        url = f'http://{host}:{port}/start_sampling/{data_name}'
        response = requests.get(url=url)
        if response.status_code != 200:
            print("Status is NOT 200!")
            sys.exit(0)
            raise Exception("Status is NOT 200!")
