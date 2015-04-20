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

    if model_data[6] == 'SA':
        imt = 'SA('+str(model_data[7])+')'
    else:
        imt = model_data[6]

    model = {
            'name': model_data[2],
            'asset_category': model_data[4],
            'loss_category': model_data[5],
            'imt': imt,
            'iml': model_data[8],
            'type': model_data[13]
                }

    cur.execute('select eng_models_vulnerability_function.probabilistic_distribution, eng_models_vulnerability_function.loss_ratio, \
                eng_models_vulnerability_function.coefficients_variation, eng_models_building_taxonomy.name \
                from eng_models_vulnerability_function, eng_models_building_taxonomy \
                where eng_models_vulnerability_function.model_id = %s \
                and eng_models_vulnerability_function.taxonomy_id = eng_models_building_taxonomy.id', (id,))
    taxonomies = [dict(probabilistic_distribution = f[0],
                        loss_ratio = f[1],
                        coefficients_variation = f[2],
                        taxonomy = f[3]) for f in cur.fetchall()]


    vul_template = templateEnv.get_template('vulnerability_model.jinja')
    vul_output = vul_template.render(dict(model= model, taxonomies= taxonomies))

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
                from jobs_scenario_risk_vulnerability_models, eng_models_vulnerability_model \
                where jobs_scenario_risk_vulnerability_models.job_id = %s \
                and jobs_scenario_risk_vulnerability_models.model_id = eng_models_vulnerability_model.id", (job_id,))
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
    output_proc_damage = proc_damage.stdout.read().split("\n")

    oq_id = output_proc_damage[0].split(' ')[1]

    return oq_id


def save(job_id, oq_id, con):
    print "-------"
    print "Storing the results. OQ_id: "+str(oq_id)
    
    cur = con.cursor()
    cur.execute("", (job_id, oq_id))
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

    cur.execute('select vulnerability_model_id from jobs_scenario_risk_vulnerability_models \
                where scenario_risk_id = %s', [id])
    for model in cur.fetchall():
        create_vulnerability_model(model[0], connection, FOLDER)
    
    cur.execute('select exposure_model_id from jobs_scenario_risk where id = %s', (id,))
    exposure_model_id = cur.fetchone()[0]
    create_exposure_model(exposure_model_id, connection, FOLDER)
    
    create_ini_file(id, connection, FOLDER)
    oq_id = run(id, connection, FOLDER)
    save(id, oq_id, connection)

    cur.execute('update jobs_scenario_risk set status = "FINISHED" where id = %s', (id,))
    connection.commit()
    
