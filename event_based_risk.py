#! /usr/bin/python

import jinja2
import os
import json
import subprocess
import numpy

from scenario_risk import create_vulnerability_model
from scenario_damage import create_exposure_model

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



def run(job_id, hazard_id, con, folder):
    print "-------"
    print "Running risk..."
    
    # celeryd_node = subprocess.Popen('./celeryd_start_node.sh')
    # celeryd_node.wait()
    
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
                AND foreign_output.output_type = 'event_loss_curve'", (risk_output_id,))

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


    cur.execute('UPDATE jobs_classical_psha_risk SET oq_id = %s WHERE id = %s', (risk_output_id, job_id))
    con.commit()

    return oq_curves_ids, oq_map_ids, risk_output_id

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
                    AND jobs_classical_psha_risk_vulnerability.job_id = %s \
                    AND jobs_classical_psha_risk.id = jobs_classical_psha_risk_vulnerability.job_id', (loss_type, job_id))
        data = cur.fetchone()
        job_vul_id = data[0]
        exposure_model_id = data[1]

        cur.execute("INSERT INTO jobs_classical_psha_risk_loss_curves (vulnerability_model_id, asset_id, hazard_output_id, \
                    statistics, quantile, loss_ratios, poes, average_loss_ratio, stddev_loss_ratio, asset_value, insured) \
                    SELECT %s, eng_models_asset.id, foreign_loss_curve.hazard_output_id, \
                    'mean', foreign_loss_curve.quantile, \
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
                    AND jobs_classical_psha_risk_vulnerability.job_id = %s \
                    AND jobs_classical_psha_risk.id = jobs_classical_psha_risk_vulnerability.job_id', (loss_type, job_id))
        data = cur.fetchone()
        job_vul_id = data[0]
        exposure_model_id = data[1]

        cur.execute("INSERT INTO jobs_classical_psha_risk_loss_maps (vulnerability_model_id, asset_id, hazard_output_id, \
                    statistics, quantile, poe, mean, stddev, insured) \
                    SELECT %s, eng_models_asset.id, foreign_loss_map.hazard_output_id, \
                    'mean', foreign_loss_map.quantile, foreign_loss_map.poe, \
                    foreign_loss_map_data.value, foreign_loss_map_data.std_dev, foreign_loss_map.insured  \
                    FROM eng_models_asset, foreign_loss_map, foreign_loss_map_data \
                    WHERE foreign_loss_map.id = %s \
                    AND foreign_loss_map_data.loss_map_id = foreign_loss_map.id \
                    AND foreign_loss_map_data.asset_ref = eng_models_asset.name \
                    AND eng_models_asset.model_id = %s", (job_vul_id, e['id'], exposure_model_id))

        con.commit()




def save_event_loss_table(oq_job_id, vulnerability_models, hazard_job_id, investigation_time, nr_ses, connection):
    print "-------"
    print "Storing Event Loss Tables"

    cur = connection.cursor()

    for model in vulnerability_models:
        if model['type'] == 'occupants'+'_vulnerability':
            loss_type = 'fatalities'
        else:
            loss_type =  model['type'].split('_vulnerability')[0]
        
        print '* '+loss_type

        # cur.execute("INSERT INTO jobs_event_loss_table (rupture_id, job_vulnerability_id, asset_id, loss) \
        #             SELECT jobs_event_based_hazard_ses_rupture.id, %s, eng_models_asset.id, foreign_event_loss_asset.loss \
        #             FROM foreign_output, foreign_event_loss, foreign_event_loss_asset, foreign_exposure_data, \
        #             eng_models_asset, jobs_event_based_hazard_ses_rupture  \
        #             WHERE foreign_output.oq_job_id = %s \
        #             AND foreign_output.output_type = 'event_loss_asset' \
        #             AND foreign_event_loss.loss_type = %s \
        #             AND foreign_event_loss.output_id = foreign_output.id \
        #             AND foreign_event_loss_asset.event_loss_id = foreign_event_loss.id \
        #             AND foreign_exposure_data.id = foreign_event_loss_asset.asset_id \
        #             AND foreign_exposure_data.asset_ref = eng_models_asset.name \
        #             AND eng_models_asset.model_id = %s \
        #             AND foreign_event_loss_asset.rupture_id = jobs_event_based_hazard_ses_rupture.rupture_id \
        #             AND jobs_event_based_hazard_ses_rupture.job_id = %s", (model['job_vul'], oq_job_id, loss_type, exposure_model_id, hazard_job_id))
        # connection.commit()

        cur.execute("SELECT jobs_event_based_hazard_ses_rupture.ses_id,  sum(foreign_event_loss_asset.loss * jobs_event_based_hazard_ses_rupture.weight) as l \
                    FROM foreign_output, foreign_event_loss, foreign_event_loss_asset, jobs_event_based_hazard_ses_rupture  \
                    WHERE jobs_event_based_hazard_ses_rupture.job_id = %s \
                    AND foreign_event_loss_asset.rupture_id = jobs_event_based_hazard_ses_rupture.rupture_id \
                    AND foreign_output.oq_job_id = %s \
                    AND foreign_output.output_type = 'event_loss_asset' \
                    AND foreign_event_loss.loss_type = %s \
                    AND foreign_event_loss.output_id = foreign_output.id \
                    AND foreign_event_loss_asset.event_loss_id = foreign_event_loss.id \
                    GROUP BY jobs_event_based_hazard_ses_rupture.ses_id \
                    ORDER BY l DESC", (hazard_job_id, oq_job_id, loss_type ))

        investigation_time_loss_values = cursor.fetchall()

        annual_time_loss_rates=(numpy.arange(1,nr_ses+1)/float(nr_ses))/float(investigation_time)
        period = 1/annual_time_loss_rates

        cur.execute("UPDATE jobs_classical_psha_risk_vulnerability \
                    SET it_loss_values = %s, \
                    at_loss_rates = %s, \
                    periods = %s \
                    WHERE id = %s", (investigation_time_loss_values, annual_time_loss_rates, period, model['id']) )
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
                jobs_event_based_risk.loss_curve_resolution, \
                jobs_classical_psha_hazard.investigation_time, jobs_event_based_hazard.ses_per_logic_tree_path   \
                FROM jobs_classical_psha_risk, jobs_event_based_risk, jobs_classical_psha_hazard, jobs_event_based_hazard \
                WHERE jobs_classical_psha_risk.id = %s \
                AND jobs_classical_psha_risk.id = jobs_event_based_risk.classical_psha_risk_ptr_id \
                AND jobs_event_based_risk.hazard_event_based_id = jobs_event_based_hazard.classical_psha_hazard_ptr_id \
                AND jobs_event_based_hazard.classical_psha_hazard_ptr_id = jobs_classical_psha_hazard.id', (id,))
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
                loss_curve_resolution = data[10],
                investigation_time = data[11],
                nr_ses = data[12]
                )


    cur.execute('SELECT jobs_classical_psha_risk_vulnerability.id, jobs_classical_psha_risk_vulnerability.vulnerability_model_id, eng_models_vulnerability_model.type \
                FROM jobs_classical_psha_risk_vulnerability, eng_models_vulnerability_model \
                WHERE jobs_classical_psha_risk_vulnerability.job_id = %s \
                AND jobs_classical_psha_risk_vulnerability.vulnerability_model_id = eng_models_vulnerability_model.id', [id])

    vulnerability_models = [{'job_vul':e[0], 'id':e[1], 'type':e[2],} for e in cur.fetchall()]

    for model in vulnerability_models:
        create_vulnerability_model(model['id'], connection, FOLDER)


    cur.execute('SELECT st_astext(region), max_distance FROM jobs_classical_psha_hazard WHERE id =%s', (params['hazard_id'], ))
    hazard_data = cur.fetchone()

    # cur.execute('SELECT st_astext( ST_Intersection( ST_Buffer( ST_GeomFromText(%s, 4326), %s ), ST_GeomFromText(%s, 4326) ) )', (hazard_data[0], "radius_of_buffer_in_meters="+str(hazard_data[1]*1000), region_wkt))
    # assets_region = cur.fetchone()[0]

    cur.execute('SELECT st_astext( ST_Intersection( ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326) ) )', (hazard_data[0], region_wkt))
    assets_region = cur.fetchone()[0]

    assets = create_exposure_model(params['exposure_model_id'], connection, FOLDER, assets_region)

    create_ini_file(params, vulnerability_models, assets, FOLDER)

    cur.execute('SELECT oq_id FROM jobs_classical_psha_hazard WHERE id = %s', (params['hazard_id'],))
    hazard_calculation_id = cur.fetchone()[0]

    oq_curves_ids, oq_map_ids, oq_job_id = run(id, hazard_calculation_id, connection, FOLDER)
    save(id, oq_curves_ids, oq_map_ids, connection)

    save_event_loss_table(risk_output_id, vulnerability_models, params['exposure_model_id'], params['hazard_id'], connection)
    save_event_loss_table(oq_job_id, vulnerability_models, params['hazard_id'], params['investigation_time'], params['nr_ses'], connection)



    cur.execute("UPDATE jobs_classical_psha_risk SET status = 'FINISHED' WHERE id = %s", (id,))
    connection.commit()



