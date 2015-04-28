
import controller

job_id = 6
job_type = 'scenario_risk'
database = {'HOST': 'priseDB.fe.up.pt', 'NAME': 'riscodb_dev', 'USER': 'postgres', 'PASSWORD': 'prisefeup'}

controller.start(job_id, job_type, database) 
