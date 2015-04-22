#! /usr/bin/python

import jinja2
import os
import json
import subprocess

PATH = os.path.dirname(os.path.realpath(__file__)) + '/files/'

templateLoader = jinja2.FileSystemLoader(PATH + 'templates/')
templateEnv = jinja2.Environment(loader=templateLoader)


def create_fragility_model(job_id, con, folder):
    print "-------"
    print "Creating Fragility Model"

    cur = con.cursor()
    cur.execute('select eng_models_fragility_model.name, eng_models_fragility_model.limit_states \
                from jobs_scenario_damage, eng_models_fragility_model \
                where jobs_scenario_damage.fragility_model_id = eng_models_fragility_model.id \
                and jobs_scenario_damage.id = %s', (job_id,))
    model = cur.fetchone()

    cur.execute('select eng_models_building_taxonomy.name, eng_models_taxonomy_fragility_model.dist_type, \
                eng_models_taxonomy_fragility_model.imt, eng_models_taxonomy_fragility_model.sa_period, eng_models_taxonomy_fragility_model.unit, \
                eng_models_taxonomy_fragility_model.min_iml, eng_models_taxonomy_fragility_model.max_iml \
                from jobs_scenario_damage, eng_models_fragility_model, eng_models_building_taxonomy, \
                eng_models_building_taxonomy_source, eng_models_taxonomy_fragility_model \
                where jobs_scenario_damage.fragility_model_id = eng_models_fragility_model.id \
                and eng_models_fragility_model.taxonomy_source_id = eng_models_building_taxonomy_source.id \
                and eng_models_building_taxonomy_source.id = eng_models_building_taxonomy.source_id \
                and jobs_scenario_damage.id = %s \
                and eng_models_taxonomy_fragility_model.taxonomy_id = eng_models_building_taxonomy.id', (job_id,))
    taxonomies = [dict(name = f[0],
                    dist_type = f[1],
                    imt = f[2],
                    sa_period = f[3],
                    unit = f[4],
                    min_iml = f[5],
                    max_iml = f[6]) for f in cur.fetchall()]

    cur.execute('select eng_models_building_taxonomy.name, mean, stddev, limit_state \
                from eng_models_fragility_model, eng_models_building_taxonomy, eng_models_fragility_function, \
                jobs_scenario_damage, eng_models_taxonomy_fragility_model \
                where eng_models_fragility_function.tax_frag_id = eng_models_taxonomy_fragility_model.id \
                and eng_models_taxonomy_fragility_model.model_id = eng_models_fragility_model.id \
                and eng_models_taxonomy_fragility_model.taxonomy_id = eng_models_building_taxonomy.id \
                and jobs_scenario_damage.id = %s \
                and jobs_scenario_damage.fragility_model_id = eng_models_fragility_model.id', (job_id,))

    functions = [dict(name = f[0],
                    mean = f[1],
                    stddev = f[2],
                    limit_state = f[3]) for f in cur.fetchall()]

    frag_template = templateEnv.get_template('fragility_model.jinja')
    frag_output = frag_template.render(dict(functions=functions, model= model, taxonomies= taxonomies))

    new_output = []

    output_list = frag_output.split('\n')
    for e in output_list:
        if e.isspace():
            pass
        else:
            new_output.append(e)

    frag_output = "\n".join(new_output)

    with open(folder+"/fragility_model.xml", "wb+") as file:
        file.write(frag_output)
        file.close()


def create_exposure_model(id, con, folder):
    print "-------"
    print "Creating Exposure Model"

    cur = con.cursor()
    cur.execute('select eng_models_exposure_model.id, eng_models_exposure_model.name, area_type, area_unit, \
                struct_cost_type, struct_cost_currency, non_struct_cost_type, non_struct_cost_currency, \
                contents_cost_type, contents_cost_currency, business_int_cost_type, business_int_cost_currency, \
                deductible, insurance_limit, eng_models_building_taxonomy_source.name \
                from eng_models_exposure_model, eng_models_building_taxonomy_source \
                where eng_models_exposure_model.id = %s \
                and eng_models_exposure_model.taxonomy_source_id = eng_models_building_taxonomy_source.id', (id,))
    e =  cur.fetchone()

    model = dict(id = e[0],
                name = e[1],
                area_type = e[2],
                area_unit = e[3],
                struct_cost_type = e[4],
                struct_cost_currency = e[5],
                non_struct_cost_type = e[6],
                non_struct_cost_currency = e[7],
                contents_cost_type = e[8],
                contents_cost_currency = e[9],
                business_int_cost_type = e[10],
                business_int_cost_currency = e[11],
                deductible = e[12],
                insurance_limit = e[13],
                taxonomy_source = e[14])

    cur.execute('select eng_models_asset.id, st_x(location), st_y(location), \
                eng_models_asset.name, n_buildings, area, struct_cost, struct_deductible, struct_insurance_limit, retrofitting_cost, \
                non_struct_cost, non_struct_deductible, non_struct_insurance_limit, \
                contents_cost, contents_deductible, contents_insurance_limit, business_int_cost, business_int_deductible, business_int_insurance_limit, \
                oc_day, oc_night, oc_transit, eng_models_building_taxonomy.name \
                from eng_models_building_taxonomy , eng_models_asset, eng_models_exposure_model \
                where eng_models_exposure_model.id = eng_models_asset.model_id \
                and eng_models_exposure_model.id = %s \
                and eng_models_asset.taxonomy_id = eng_models_building_taxonomy.id', (id,))

    assets = [dict(id = asset[0],
                    lon = asset[1],
                    lat = asset[2],
                    name = asset[3],
                    n_buildings = asset[4],
                    area = asset[5],
                    struct_cost = asset[6],
                    struct_deductible = asset[7],
                    struct_insurance_limit = asset[8],
                    retrofitting_cost = asset[9],
                    non_struct_cost = asset[10],
                    non_struct_deductible = asset[11],
                    non_struct_insurance_limit = asset[12],
                    contents_cost = asset[13],
                    contents_deductible = asset[14],
                    contents_insurance_limit = asset[15],
                    business_int_cost = asset[16],
                    business_int_deductible = asset[17],
                    business_int_insurance_limit = asset[18],
                    oc_day = asset[19],
                    oc_night = asset[20],
                    oc_transit = asset[21],
                    taxonomy = asset[22],
                    ) for asset in cur.fetchall()]

    exp_template = templateEnv.get_template('exposure_model.jinja')
    exp_output = exp_template.render(dict(model=model, assets=assets))

    with open(folder+"/exposure_model.xml", "wb") as file:
        file.write(exp_output)
        file.close()



def create_ini_file(job_id, con, folder):
    print "-------"
    print "Creating .ini file"

    cur = con.cursor()
    cur.execute("select name, max_hazard_dist, st_astext(region) from jobs_scenario_damage")
    data = cur.fetchone()

    params = dict(name= data[0],
                max_hazard_dist = data[1],
                region = data[2].split('(')[2].split(')')[0],
                    )

    conf_template = templateEnv.get_template('configuration_scenario_damage.jinja')
    conf_output = conf_template.render(params)

    with open(folder+"/configuration.ini", "wb") as file:
        file.write(conf_output)
        file.close()



def run(job_id, con, folder):
    print "-------"
    print "Running scenario damage..."

    cur = con.cursor()
    cur.execute('select jobs_scenario_hazard.oq_id from jobs_scenario_damage, jobs_scenario_hazard \
                where jobs_scenario_damage.hazard_job_id = jobs_scenario_hazard.id \
                and jobs_scenario_damage.id = %s', (job_id,))
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
    cur.execute("INSERT INTO jobs_scenario_damage_results (job_id, asset_id, limit_state, mean, stddev) \
                SELECT %s, eng_models_asset.id, foreign_dmg_state.dmg_state, mean, stddev \
                FROM foreign_dmg_dist_per_asset, foreign_exposure_data, foreign_exposure_model, \
                foreign_dmg_state, eng_models_asset \
                WHERE foreign_dmg_dist_per_asset.dmg_state_id = foreign_dmg_state.id \
                AND foreign_dmg_dist_per_asset.exposure_data_id = foreign_exposure_data.id \
                AND foreign_exposure_data.asset_ref = eng_models_asset.name \
                AND foreign_exposure_model.id = foreign_exposure_data.exposure_model_id \
                AND foreign_dmg_state.risk_calculation_id = %s", (job_id, oq_id))
    con.commit()


def start(id, connection):

    print "-------"
    print "Starting calculating scenario damage: "+str(id)

    cur = connection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchone()[0]

    FOLDER = PATH + db_name + "/scenario_damage/"+str(id)

    try:
        os.makedirs(FOLDER)
    except:
        pass

    
    create_fragility_model(id, connection, FOLDER)

    cur.execute('select exposure_model_id from jobs_scenario_damage where id = %s', (id,))
    exposure_model_id = cur.fetchone()[0]
    create_exposure_model(exposure_model_id, connection, FOLDER)
    
    create_ini_file(id, connection, FOLDER)
    oq_id = run(id, connection, FOLDER)
    save(id, oq_id, connection)

    cur.execute("update jobs_scenario_damage set status = 'FINISHED' where id = %s", (id,))
    connection.commit()
    
