#! /usr/bin/python

import jinja2
import os
import json
import subprocess

from scenario_risk import create_vulnerability_model
from scenario_damage import create_exposure_model
from psha_risk import run, save

PATH = os.path.dirname(os.path.realpath(__file__)) + '/files/'

templateLoader = jinja2.FileSystemLoader(PATH + 'templates/')
templateEnv = jinja2.Environment(loader=templateLoader)


def create_ini_file(params, vulnerability_models, folder):
    print "-------"
    print "Creating .ini file"

    conf_template = templateEnv.get_template('configuration_psha_risk.jinja')
    conf_output = conf_template.render({'params':params, 'vulnerability_models': vulnerability_models})

    with open(folder+"/configuration.ini", "wb") as file:
        file.write(conf_output)
        file.close()

def start(id, connection):
    print "-------"
    print "Starting calculating Event Based PSHA risk: "+str(id)
    
    cur = connection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchone()[0]

    FOLDER = PATH + db_name + "/event_based_risk/"+str(id)

    try:
        os.makedirs(FOLDER)
    except:
        pass


    cur = connection.cursor()
    cur.execute('SELECT jobs_classical_psha_risk.name, st_astext(jobs_classical_psha_risk.region), \
                jobs_classical_psha_risk.random_seed, jobs_classical_psha_risk.exposure_model_id, \
                jobs_classical_psha_risk.hazard_event_based_id, \
                jobs_classical_psha_risk.asset_hazard_distance, jobs_classical_psha_risk.lrem_steps_per_interval, \
                jobs_classical_psha_risk.asset_correlation, jobs_classical_psha_risk.poes, \
                jobs_classical_psha_risk.quantile_loss_curves \
                jobs_event_based_risk.loss_curve_resolution \
                FROM jobs_classical_psha_risk, jobs_event_based_risk \
                WHERE jobs_classical_psha_risk.id = %s \
                AND jobs_classical_psha_risk.id = jobs_event_based_risk.classical_psha_risk_ptr_id', (id,))
    data = cur.fetchone()

    region_wkt = data[1]

    params = dict(type= 'event_based',
                name = data[0],
                region = data[1].split('(')[2].split(')')[0],
                random_seed = data[2],
                exposure_model_id = data[3],
                hazard_id = data[4],
                asset_hazard_distance = data[5],
                lrem_steps_per_interval = data[6],
                asset_correlation = data[7],
                poes = data[8],
                quantile_loss_curves = data[9],
                loss_curve_resolution = data[10]
                )


    cur.execute('SELECT jobs_classical_psha_risk_vulnerability_models.vulnerability_model_id, eng_models_vulnerability_model.type \
                FROM jobs_classical_psha_risk_vulnerability_models, eng_models_vulnerability_model \
                WHERE jobs_classical_psha_risk_vulnerability_models.classical_psha_risk_id = %s \
                AND jobs_classical_psha_risk_vulnerability_models.vulnerability_model_id = eng_models_vulnerability_model.id', [id])

    vulnerability_models = [{'id':e[0], 'type':e[1],} for e in cur.fetchall()]

    for model in vulnerability_models:
        create_vulnerability_model(model['id'], connection, FOLDER)

    create_exposure_model(params['exposure_model_id'], connection, FOLDER, region_wkt)

    create_ini_file(params, vulnerability_models, FOLDER)
    oq_curves_ids, oq_map_ids = run(params['hazard_id'], connection, FOLDER)
    save(id, oq_curves_ids, oq_map_ids, connection)


    cur.execute("update jobs_classical_psha_risk set status = 'FINISHED' where id = %s", (id,))
    connection.commit()
