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


def create_ini_file(params, vulnerability_models, assets, folder):
    print "-------"
    print "Creating .ini file"

    conf_template = templateEnv.get_template('configuration_psha_risk.jinja')
    conf_output = conf_template.render({'params':params, 'vulnerability_models': vulnerability_models, 'assets': assets})

    with open(folder+"/configuration.ini", "wb") as file:
        file.write(conf_output)
        file.close()

def save_event_loss_table(oq_job_id, vulnerability_models, exposure_model_id, hazard_job_id, connection):
    print "-------"
    print "Storing Event Loss Tables"

    cur = connection.cursor()

    for model in vulnerability_models:
        if model['type'] == 'occupants'+'_vulnerability':
            loss_type = 'fatalities'
        else:
            loss_type =  model['type'].split('_vulnerability')[0]
        
        print '* '+loss_type

        cur.execute("INSERT INTO jobs_event_loss_table (rupture_id, job_vulnerability_id, asset_id, loss) \
                    SELECT jobs_event_based_hazard_ses_rupture.id, %s, eng_models_asset.id, foreign_event_loss_asset.loss \
                    FROM foreign_output, foreign_event_loss, foreign_event_loss_asset, foreign_exposure_data, \
                    eng_models_asset, jobs_event_based_hazard_ses_rupture  \
                    WHERE foreign_output.oq_job_id = %s \
                    AND foreign_output.output_type = 'event_loss_asset' \
                    AND foreign_event_loss.loss_type = %s \
                    AND foreign_event_loss.output_id = foreign_output.id \
                    AND foreign_event_loss_asset.event_loss_id = foreign_event_loss.id \
                    AND foreign_exposure_data.id = foreign_event_loss_asset.asset_id \
                    AND foreign_exposure_data.asset_ref = eng_models_asset.name \
                    AND eng_models_asset.model_id = %s \
                    AND foreign_event_loss_asset.rupture_id = jobs_event_based_hazard_ses_rupture.rupture_id \
                    AND jobs_event_based_hazard_ses_rupture.job_id = %s", (model['id'], oq_job_id, loss_type, exposure_model_id, hazard_job_id))
        connection.commit()



def start(id, connection):
    print "-------"
    print "Starting calculating Event Based PSHA risk: "+str(id)
    
    cur = connection.cursor()
    cur.execute('SELECT current_database()')
    db_name = cur.fetchone()[0]

    FOLDER = PATH + db_name + "/event_based_risk/"+str(id)

    try:
        os.makedirs(FOLDER)
    except:
        pass


    cur = connection.cursor()
    cur.execute('SELECT jobs_classical_psha_risk.name, st_astext(jobs_classical_psha_risk.region), \
                jobs_classical_psha_risk.random_seed, jobs_classical_psha_risk.exposure_model_id, \
                jobs_event_based_risk.hazard_event_based_id, \
                jobs_classical_psha_risk.asset_hazard_distance, jobs_classical_psha_risk.lrem_steps_per_interval, \
                jobs_classical_psha_risk.asset_correlation, jobs_classical_psha_risk.poes, \
                jobs_classical_psha_risk.quantile_loss_curves, \
                jobs_event_based_risk.loss_curve_resolution \
                FROM jobs_classical_psha_risk, jobs_event_based_risk \
                WHERE jobs_classical_psha_risk.id = %s \
                AND jobs_classical_psha_risk.id = jobs_event_based_risk.classical_psha_risk_ptr_id', (id,))
    data = cur.fetchone()

    region_wkt = data[1]

    params = dict(type= 'event_based_risk',
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


    cur.execute('SELECT jobs_classical_psha_risk_vulnerability.vulnerability_model_id, eng_models_vulnerability_model.type \
                FROM jobs_classical_psha_risk_vulnerability, eng_models_vulnerability_model \
                WHERE jobs_classical_psha_risk_vulnerability.job_id = %s \
                AND jobs_classical_psha_risk_vulnerability.vulnerability_model_id = eng_models_vulnerability_model.id', [id])

    vulnerability_models = [{'id':e[0], 'type':e[1],} for e in cur.fetchall()]

    for model in vulnerability_models:
        create_vulnerability_model(model['id'], connection, FOLDER)

    create_exposure_model(params['exposure_model_id'], connection, FOLDER, region_wkt)

    cur.execute('SELECT name FROM eng_models_asset WHERE model_id = %s', (params['exposure_model_id'], ))
    assets = list(asset[0] for asset in cur.fetchall() )
    create_ini_file(params, vulnerability_models, assets, FOLDER)

    cur.execute('SELECT oq_id FROM jobs_classical_psha_hazard WHERE id = %s', (params['hazard_id'],))
    hazard_calculation_id = cur.fetchone()[0]

    oq_curves_ids, oq_map_ids, risk_output_id = run(id, hazard_calculation_id, connection, FOLDER)
    save(id, oq_curves_ids, oq_map_ids, connection)

    save_event_loss_table(risk_output_id, vulnerability_models, params['exposure_model_id'], params['hazard_id'], connection)

    cur.execute("UPDATE jobs_classical_psha_risk SET status = 'FINISHED' WHERE id = %s", (id,))
    connection.commit()
