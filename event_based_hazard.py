#! /usr/bin/python

import jinja2
import os
import json
import subprocess

from scenario_hazard import create_site_model
from psha_hazard import create_logic_tree_sm, create_logic_tree_gmpe, create_source_model, run, save

PATH = os.path.dirname(os.path.realpath(__file__)) + '/files/'

templateLoader = jinja2.FileSystemLoader(PATH + 'templates/')
templateEnv = jinja2.Environment(loader=templateLoader)



def create_ini_file(params, folder):
    print "-------"
    print "Creating .ini file"

    conf_template = templateEnv.get_template('configuration_psha_hazard.jinja')
    conf_output = conf_template.render(params)

    with open(folder+"/configuration.ini", "wb") as file:
        file.write(conf_output)
        file.close()


def start(id, connection):
    print "-------"
    print "Starting calculating Event Based Hazard PSHA hazard: "+str(id)

    cur = connection.cursor()
    cur.execute('SELECT jobs_classical_psha_hazard.name, st_astext(jobs_classical_psha_hazard.region), jobs_classical_psha_hazard.grid_spacing, \
                jobs_classical_psha_hazard.sites_type, jobs_classical_psha_hazard.vs30, jobs_classical_psha_hazard.vs30type, jobs_classical_psha_hazard.z1pt0, jobs_classical_psha_hazard.z2pt5, jobs_classical_psha_hazard.site_model_id, \
                jobs_classical_psha_hazard.random_seed, jobs_classical_psha_hazard.rupture_mesh_spacing, jobs_classical_psha_hazard.truncation_level, jobs_classical_psha_hazard.max_distance, \
                jobs_classical_psha_hazard.n_lt_samples, jobs_classical_psha_hazard.width_of_mfd_bin, jobs_classical_psha_hazard.area_source_discretization, jobs_classical_psha_hazard.investigation_time, \
                jobs_classical_psha_hazard.imt_l, jobs_classical_psha_hazard.poes, jobs_classical_psha_hazard.quantile_hazard_curves, jobs_classical_psha_hazard.gmpe_logic_tree_id, jobs_classical_psha_hazard.sm_logic_tree_id, \
                jobs_event_based_hazard.ses_per_logic_tree_path \
                FROM jobs_classical_psha_hazard, jobs_event_based_hazard \
                WHERE jobs_event_based_hazard.classical_psha_hazard_ptr_id = %s \
                AND jobs_event_based_hazard.classical_psha_hazard_ptr_id = jobs_classical_psha_hazard.id', (id,))
    data = cur.fetchone()


    params = dict(type='event_based',
                name = data[0],
                region = data[1].split('(')[2].split(')')[0],
                grid_spacing = data[2],
                sites_type = data[3],
                vs30 = data[4],
                vs30type = data[5],
                z1pt0 = data[6],
                z2pt5 = data[7],
                site_model_id = data[8],
                random_seed = data[9],
                rupture_mesh_spacing = data[10],
                truncation_level = data[11],
                max_distance = data[12],
                n_lt_samples = data[13],
                width_of_mfd_bin = data[14],
                area_source_discretization = data[15],
                investigation_time = data[16],
                imt_l = data[17],
                poes = data[18],
                quantile_hazard_curves = data[19],
                gmpe_logic_tree_id = data[20],
                sm_logic_tree_id = data[21],
                ses_per_logic_tree_path= data[22])
    
    cur = connection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchone()[0]

    FOLDER = PATH + db_name + "/event_based_hazard/"+str(id)

    try:
        os.makedirs(FOLDER)
    except:
        pass

    if params['sites_type'] == 'VARIABLE_CONDITIONS':
        create_site_model(params['site_model_id'], connection, FOLDER)
    

    cur.execute('SELECT source_model_id \
                FROM eng_models_logic_tree_sm_source_models \
                WHERE logic_tree_sm_id = %s', (params['sm_logic_tree_id'],))

    for source in cur.fetchall():
        create_source_model(source[0], connection, FOLDER)

    create_logic_tree_sm(params['sm_logic_tree_id'], connection, FOLDER)
    create_logic_tree_gmpe(params['gmpe_logic_tree_id'], connection, FOLDER)


    create_ini_file(params, FOLDER)
    oq_curves_ids, oq_map_ids = run(id, connection, FOLDER)
    save(id, oq_curves_ids, oq_map_ids, connection)


    cur.execute("update jobs_classical_psha_hazard set status = 'FINISHED' where id = %s", (id,))
    connection.commit()
