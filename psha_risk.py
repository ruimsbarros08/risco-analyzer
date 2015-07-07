#! /usr/bin/python

import jinja2
import os
import json
import subprocess

from scenario_risk import create_vulnerability_model
from scenario_damage import create_exposure_model

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



def run(hazard_id, con, folder):
    print "-------"
    print "Running Classical PSHA risk..."
    
    cur = con.cursor()
    proc = subprocess.Popen(["oq-engine", "--log-file", folder+"/log.txt", "--rr", folder+"/configuration.ini", "--hazard-calculation-id", str(hazard_id)], stdout=subprocess.PIPE)
    proc.wait()
    
    output_risk = proc.stdout.read().split("\n")

    for e in output_risk:
        try:
            a = e.split(' | ')
            curve_id = int(a[0])
        except:
            pass

    cur.execute("SELECT oq_job_id FROM foreign_output WHERE id = %s", (curve_id,))
    risk_output_id = cur.fetchone()[0]


    cur.execute("SELECT foreign_loss_curve.id, foreign_loss_curve.statistics, \
                foreign_loss_curve.quantile, foreign_loss_curve.loss_type, foreign_loss_curve.insured \
                FROM foreign_loss_curve, foreign_output \
                WHERE foreign_output.oq_job_id = %s \
                AND foreign_loss_curve.output_id = foreign_output.id \
                AND foreign_output.output_type = 'loss_curve'", (risk_output_id,))

    oq_curves_ids = [ { 'id':e[0],
                        'statistics': e[1],
                        'quantile': e[2],
                        'loss_type': e[3],
                        'insured': e[4] } for e in cur.fetchall() ]

    cur.execute("SELECT foreign_loss_map.id, foreign_loss_map.statistics, \
                foreign_loss_map.quantile, foreign_loss_map.loss_type, foreign_loss_map.insured \
                FROM foreign_loss_map, foreign_output \
                WHERE foreign_output.oq_job_id = %s \
                AND foreign_loss_map.output_id = foreign_output.id \
                AND foreign_output.output_type = 'loss_map'", (risk_output_id,))

    oq_map_ids = [ { 'id':e[0],
                        'statistics': e[1],
                        'quantile': e[2],
                        'loss_type': e[3],
                        'insured': e[4] } for e in cur.fetchall() ]


    cur.execute('update jobs_classical_psha_risk set oq_id = %s where id = %s', (risk_output_id, job_id))
    con.commit()

    return oq_curves_ids, oq_map_ids


def save(job_id, oq_curves_ids, oq_map_ids, con): 
    print "-------"
    print "Storing curves"

    for e in oq_curves_ids:

        print " * OQ id: "+str(e['id'])

        if e['loss_type'] == 'fatalities':
            loss_type = 'occupants'+'_vulnerability'
        else:
            loss_type =  e['loss_type']+'_vulnerability'
        
        cur = con.cursor()

        cur.execute('SELECT jobs_classical_psha_risk_vulnerability.id, jobs_classical_psha_risk.exposure_model_id \
                    FROM eng_models_vulnerability_model, jobs_classical_psha_risk_vulnerability, jobs_classical_psha_risk \
                    WHERE eng_models_vulnerability_model.type = %s \
                    AND eng_models_vulnerability_model.id = jobs_classical_psha_risk_vulnerability.vulnerability_model_id \
                    AND jobs_classical_psha_risk_vulnerability.job_id = %s', (loss_type, job_id))
        data = cur.fetchone()
        job_vul_id = data[0]
        exposure_model_id = data[1]

        cur.execute("INSERT INTO jobs_classical_psha_risk_loss_curves (vulnerability_model_id, asset_id, hazard_output_id, \
                    statistics, quantile, loss_ratios, poes, average_loss_ratio, stddev_loss_ratio, asset_value, insured) \
                    SELECT %s, eng_models_asset.id, foreign_loss_curve.hazard_output_id, \
                    foreign_loss_curve.statistics, foreign_loss_curve.quantile, \
                    foreign_loss_curve_data.loss_ratios, foreign_loss_curve_data.poes, \
                    foreign_loss_curve_data.average_loss_ratio, foreign_loss_curve_data.stddev_loss_ratio, \
                    foreign_loss_curve_data.asset_value, foreign_loss_curve.insured \
                    FROM eng_models_asset, foreign_loss_curve, foreign_loss_curve_data \
                    WHERE foreign_loss_curve.id = %s \
                    AND foreign_loss_curve_data.loss_curve_id = foreign_loss_curve.id \
                    AND foreign_loss_curve_data.asset_ref = eng_models_asset.name \
                    AND eng_models_asset.model_id = %s", (job_vul_id, e['id'], exposure_model_id))
        con.commit()


    print "-------"
    print "Storing maps"

    for e in oq_map_ids:
    
        print " * OQ id: "+str(e['id'])

        if e['loss_type'] == 'fatalities':
            loss_type = 'occupants'+'_vulnerability'
        else:
            loss_type =  e['loss_type']+'_vulnerability'
        
        cur = con.cursor()

        cur.execute('SELECT jobs_classical_psha_risk_vulnerability.id, jobs_classical_psha_risk.exposure_model_id \
                    FROM eng_models_vulnerability_model, jobs_classical_psha_risk_vulnerability, jobs_classical_psha_risk \
                    WHERE eng_models_vulnerability_model.type = %s \
                    AND eng_models_vulnerability_model.id = jobs_classical_psha_risk_vulnerability.vulnerability_model_id \
                    AND jobs_classical_psha_risk_vulnerability.job_id = %s', (loss_type, job_id))
        data = cur.fetchone()
        job_vul_id = data[0]
        exposure_model_id = data[1]

        if e['statistics'] != None:

            cur.execute("INSERT INTO jobs_classical_psha_risk_loss_maps (vulnerability_model_id, asset_id, hazard_output_id, \
                        statistics, quantile, poe, mean, stddev, insured) \
                        SELECT %s, eng_models_asset.id, foreign_loss_map.hazard_output_id, \
                        foreign_loss_map.statistics, foreign_loss_map.quantile, foreign_loss_map.poe, \
                        foreign_loss_map_data.value, foreign_loss_map_data.std_dev, foreign_loss_map.insured  \
                        FROM eng_models_asset, foreign_loss_map, foreign_loss_map_data \
                        WHERE foreign_loss_map.id = %s \
                        AND foreign_loss_map_data.loss_map_id = foreign_loss_map.id \
                        AND foreign_loss_map_data.asset_ref = eng_models_asset.name \
                        AND eng_models_asset.model_id = %s", (job_vul_id, e['id'], exposure_model_id))

            con.commit()




def start(id, connection):
    print "-------"
    print "Starting calculating Classical PSHA risk: "+str(id)
    
    cur = connection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchone()[0]

    FOLDER = PATH + db_name + "/psha_risk/"+str(id)

    try:
        os.makedirs(FOLDER)
    except:
        pass


    cur = connection.cursor()
    cur.execute('select name, st_astext(region), random_seed, exposure_model_id, hazard_id, \
                asset_hazard_distance, lrem_steps_per_interval, asset_correlation, poes, quantile_loss_curves \
                from jobs_classical_psha_risk \
                where id = %s', (id,))
    data = cur.fetchone()

    region_wkt = data[1]

    params = dict(type= 'classical_risk',
                name = data[0],
                region = data[1].split('(')[2].split(')')[0],
                random_seed = data[2],
                exposure_model_id = data[3],
                hazard_id = data[4],
                asset_hazard_distance = data[5],
                lrem_steps_per_interval = data[6],
                asset_correlation = data[7],
                poes = data[8],
                quantile_loss_curves = data[9]
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
