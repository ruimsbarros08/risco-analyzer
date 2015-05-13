#! /usr/bin/python

import jinja2
import os
import json
import subprocess

from scenario_damage import create_exposure_model

PATH = os.path.dirname(os.path.realpath(__file__)) + '/files/'

templateLoader = jinja2.FileSystemLoader(PATH + 'templates/')
templateEnv = jinja2.Environment(loader=templateLoader)


def create_vulnerability_model(id, con, folder):
    print "-------"
    print "Creating Vulnerability Model"

    cur = con.cursor()
    cur.execute('select * \
                from eng_models_vulnerability_model \
                where id = %s', (id,))

    model_data = cur.fetchone()

    model = {
            'name': model_data[2],
            'asset_category': model_data[4],
            'loss_category': model_data[5],
            #'imt': imt,
            'iml': model_data[6],
            'type': model_data[11]
                }

    cur.execute('select eng_models_vulnerability_function.probabilistic_distribution, eng_models_vulnerability_function.loss_ratio, \
                eng_models_vulnerability_function.coefficients_variation, eng_models_vulnerability_function.imt, \
                eng_models_vulnerability_function.sa_period , eng_models_building_taxonomy.name \
                from eng_models_vulnerability_function, eng_models_building_taxonomy \
                where eng_models_vulnerability_function.model_id = %s \
                and eng_models_vulnerability_function.taxonomy_id = eng_models_building_taxonomy.id', (id,))

    imts = []
    functions = []

    for function in cur.fetchall():
        if function[3] == 'SA':
            imt = 'SA('+str(function[4])+')'
        else:
            imt = function[3]

        if imt in imts:
            pass
        else:
            imts.append(imt)

        functions.append(dict(probabilistic_distribution = function[0],
                        loss_ratio = function[1],
                        coefficients_variation = function[2],
                        imt = imt,
                        taxonomy = function[5]))


    vul_template = templateEnv.get_template('vulnerability_model.jinja')
    vul_output = vul_template.render(dict(model= model, taxonomies= functions, imts = imts))

    new_output = []

    output_list = vul_output.split('\n')
    for e in output_list:
        if e.isspace():
            pass
        else:
            new_output.append(e)

    vul_output = "\n".join(new_output)

    with open(folder + "/" + model['type'] + "_model.xml", "wb+") as file:
        file.write(vul_output)
        file.close()



def create_ini_file(job_id, con, folder):
    print "-------"
    print "Creating .ini file"

    cur = con.cursor()
    cur.execute("select name, max_hazard_dist, st_astext(region), master_seed, vul_correlation_coefficient, \
                insured_losses, time_of_the_day \
                from jobs_scenario_risk \
                where id = %s", (job_id,))
    data = cur.fetchone()

    cur.execute("select eng_models_vulnerability_model.type \
                from jobs_scenario_risk_vulnerability_model, eng_models_vulnerability_model \
                where jobs_scenario_risk_vulnerability_model.job_id = %s \
                and jobs_scenario_risk_vulnerability_model.vulnerability_model_id = eng_models_vulnerability_model.id", (job_id,))
    vulnerability = cur.fetchall()

    params = dict(name= data[0],
                max_hazard_dist = data[1],
                region = data[2].split('(')[2].split(')')[0],
                master_seed = data[3],
                vul_correlation_coefficient = data[4],
                insured_losses = data[5],
                time_of_the_day = data[6]
                    )

    conf_template = templateEnv.get_template('configuration_scenario_risk.jinja')
    conf_output = conf_template.render({'model': params, 'vulnerability': vulnerability})

    with open(folder+"/configuration.ini", "wb") as file:
        file.write(conf_output)
        file.close()



def run(job_id, con, folder):
    print "-------"
    print "Running scenario risk..."

    cur = con.cursor()
    cur.execute('select jobs_scenario_hazard.oq_id from jobs_scenario_risk, jobs_scenario_hazard \
                where jobs_scenario_risk.hazard_job_id = jobs_scenario_hazard.id \
                and jobs_scenario_risk.id = %s', (job_id,))
    hazard_id = cur.fetchone()[0]

    proc_damage = subprocess.Popen(["/usr/local/openquake/oq-engine/bin/openquake", "--log-file", "/dev/null", "--run-risk", folder+"/configuration.ini", "--hazard-output-id", str(hazard_id)], stdout=subprocess.PIPE)
    proc_damage.wait()
    output_proc_risk = proc_damage.stdout.read().split("\n")
    
    oq_ids = []
    for e in output_proc_risk:
        try:
            a = e.split(' | ')
            if a[1] == 'Loss Map':
                oq_ids.append(a[0])
        except:
            pass

    return oq_ids


def save(job_id, type, con):
    print "-------"
    print "Storing the results. OQ_id: "+str(type['oq_id'])
    
    cur = con.cursor()

    print '* '+type['type']

    if type['type'] == 'fatalities':
        loss_type = 'occupants'+'_vulnerability'
    else:
        loss_type = type['type']+'_vulnerability'

    cur.execute('SELECT jobs_scenario_risk_vulnerability_model.id, jobs_scenario_risk.exposure_model_id \
                FROM eng_models_vulnerability_model, jobs_scenario_risk_vulnerability_model, jobs_scenario_risk \
                WHERE eng_models_vulnerability_model.type = %s \
                AND eng_models_vulnerability_model.id = jobs_scenario_risk_vulnerability_model.vulnerability_model_id \
                AND jobs_scenario_risk_vulnerability_model.job_id = %s', (loss_type, job_id))
    data = cur.fetchone()
    job_vul_id = data[0]
    exposure_model_id = data[1]

    if 'insured_oq_id' in type:
        cur.execute("INSERT INTO jobs_scenario_risk_results (job_vul_id, asset_id, mean, stddev, insured_mean, insured_stddev) \
                    SELECT %s, eng_models_asset.id, total.value, total.std_dev, insured.value, insured.std_dev \
                    FROM (SELECT asset_ref, value, std_dev \
                        FROM foreign_loss_map, foreign_loss_map_data \
                        WHERE foreign_loss_map.output_id = %s \
                        AND foreign_loss_map_data.loss_map_id = foreign_loss_map.id) AS total, \
                        (SELECT asset_ref, value, std_dev \
                        FROM foreign_loss_map, foreign_loss_map_data \
                        WHERE foreign_loss_map.output_id = %s \
                        AND foreign_loss_map_data.loss_map_id = foreign_loss_map.id) AS insured, \
                        eng_models_asset \
                    WHERE eng_models_asset.name = total.asset_ref \
                    AND eng_models_asset.name = insured.asset_ref \
                    AND eng_models_asset.model_id = %s", (job_vul_id, type['oq_id'], type['insured_oq_id'], exposure_model_id))
    
    else:
        cur.execute("INSERT INTO jobs_scenario_risk_results (job_vul_id, asset_id, mean, stddev) \
                    SELECT %s, eng_models_asset.id, \
                    foreign_loss_map_data.value, foreign_loss_map_data.std_dev \
                    FROM foreign_loss_map, foreign_loss_map_data, eng_models_asset \
                    WHERE foreign_loss_map_data.loss_map_id = foreign_loss_map.id\
                    AND eng_models_asset.name = foreign_loss_map_data.asset_ref \
                    AND eng_models_asset.model_id = %s \
                    AND foreign_loss_map.output_id = %s", (job_vul_id, exposure_model_id, type['oq_id']))
    con.commit()


def start(id, connection):

    print "-------"
    print "Starting calculating scenario risk: "+str(id)

    cur = connection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchone()[0]

    FOLDER = PATH + db_name + "/scenario_risk/"+str(id)

    try:
        os.makedirs(FOLDER)
    except:
        pass

    cur.execute('select vulnerability_model_id from jobs_scenario_risk_vulnerability_model \
                where job_id = %s', [id])
    for model in cur.fetchall():
        create_vulnerability_model(model[0], connection, FOLDER)
    
    cur.execute('select exposure_model_id, region from jobs_scenario_risk where id = %s', (id,))
    data = cur.fetchone()
    exposure_model_id = data[0]
    region = data[1]
    create_exposure_model(exposure_model_id, connection, FOLDER, region)
    
    create_ini_file(id, connection, FOLDER)
    oq_ids = run(id, connection, FOLDER)

    loss_types = []
    for oq_id in oq_ids:
        cur.execute('SELECT loss_type, insured FROM foreign_loss_map WHERE output_id = %s', (oq_id,))
        data = cur.fetchone()
        loss_type = data[0]
        insured = data[1]
        for e in loss_types:
            if loss_type == e['type']:
                if insured:
                    e['insured_oq_id'] = oq_id
                else:
                    e['oq_id'] = oq_id
                exist = True
                break
            else:
                exist = False
            
        if not exist:
            if insured:
                loss_types.append({'type': loss_type, 'insured_oq_id': oq_id})
            else:
                loss_types.append({'type': loss_type, 'oq_id': oq_id})

    for type in loss_types:
        save(id, type, connection)

    cur.execute("update jobs_scenario_risk set status = 'FINISHED' where id = %s", (id,))
    connection.commit()
    
