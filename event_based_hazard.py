#! /usr/bin/python

import jinja2
import os
import json
import subprocess

from scenario_hazard import create_site_model
from psha_hazard import create_logic_tree_sm, create_logic_tree_gmpe, create_source_model

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



def run(job_id, con, folder):
    print "-------"
    print "Running Event Based PSHA hazard..."
    
    cur = con.cursor()
    proc_hazard = subprocess.Popen(["oq-engine", "--log-file", folder+"/log.txt", "--rh", folder+"/configuration.ini"], stdout=subprocess.PIPE)
    proc_hazard.wait()
    
    output_proc_hazard = proc_hazard.stdout.read().split("\n")

    for e in output_proc_hazard:
        try:
            a = e.split(' | ')
            curve_id = int(a[0])
        except:
            pass

    cur.execute("SELECT oq_job_id FROM foreign_output WHERE id = %s", (curve_id,))
    hazard_output_id = cur.fetchone()[0]

    cur.execute("SELECT foreign_hazard_curve.id, foreign_hazard_curve.statistics, \
                foreign_hazard_curve.quantile, foreign_hazard_curve.imt, foreign_hazard_curve.sa_period  \
                FROM foreign_hazard_curve, foreign_output \
                WHERE foreign_output.oq_job_id = %s \
                AND foreign_hazard_curve.output_id = foreign_output.id \
                AND foreign_output.output_type = 'hazard_curve'", (hazard_output_id,))

    oq_curves_ids = [ { 'id':e[0],
                        'statistics': e[1],
                        'quantile': e[2],
                        'imt': e[3],
                        'sa_period': e[4] } for e in cur.fetchall() ]

    cur.execute("SELECT foreign_hazard_map.id, foreign_hazard_map.statistics, \
                foreign_hazard_map.quantile, foreign_hazard_map.imt, foreign_hazard_map.sa_period  \
                FROM foreign_hazard_map, foreign_output \
                WHERE foreign_output.oq_job_id = %s \
                AND foreign_hazard_map.output_id = foreign_output.id \
                AND foreign_output.output_type = 'hazard_map'\
                AND foreign_hazard_map.lt_realization_id is null", (hazard_output_id,))

    oq_map_ids = [ { 'id':e[0],
                        'statistics': e[1],
                        'quantile': e[2],
                        'imt': e[3],
                        'sa_period': e[4] } for e in cur.fetchall() ]


    cur.execute('update jobs_classical_psha_hazard set oq_id = %s where id = %s', (hazard_output_id, job_id))
    con.commit()

    return oq_curves_ids, oq_map_ids


def save(job_id, oq_curves_ids, oq_map_ids, con):
    print "-------"
    print "Storing curves"

    for e in oq_curves_ids:
        
        print " * OQ id: "+str(e['id'])
        
        cur = con.cursor()

        if e['statistics'] == None:
            cur.execute("INSERT INTO jobs_classical_psha_hazard_curves (job_id, location, cell_id, imt, sa_period, sa_damping, \
                        weight, statistics, quantile, sm_lt_path, gsim_lt_path, imls, poes) \
                        SELECT %s, foreign_hazard_curve_data.location::geometry, world_fishnet.id, \
                        foreign_hazard_curve.imt, foreign_hazard_curve.sa_period, foreign_hazard_curve.sa_damping, \
                        foreign_hazard_curve_data.weight, foreign_hazard_curve.statistics, foreign_hazard_curve.quantile, \
                        foreign_lt_realization.gsim_lt_path, foreign_lt_source_model.sm_lt_path, \
                        foreign_hazard_curve.imls, foreign_hazard_curve_data.poes \
                        FROM world_fishnet, foreign_hazard_curve, foreign_hazard_curve_data, foreign_lt_realization, foreign_lt_source_model \
                        WHERE foreign_hazard_curve.id = %s \
                        AND foreign_hazard_curve_data.hazard_curve_id = foreign_hazard_curve.id \
                        AND foreign_lt_realization.id = foreign_hazard_curve.lt_realization_id \
                        AND foreign_lt_realization.lt_model_id = foreign_lt_source_model.id \
                        AND ST_Intersects(foreign_hazard_curve_data.location::geometry, world_fishnet.cell)", (job_id, e['id']))
            con.commit()

        else:
            cur.execute("INSERT INTO jobs_classical_psha_hazard_curves (job_id, location, cell_id, imt, sa_period, sa_damping, \
                        weight, statistics, quantile, sm_lt_path, gsim_lt_path, imls, poes) \
                        SELECT %s, foreign_hazard_curve_data.location::geometry, world_fishnet.id, \
                        foreign_hazard_curve.imt, foreign_hazard_curve.sa_period, foreign_hazard_curve.sa_damping, \
                        foreign_hazard_curve_data.weight, foreign_hazard_curve.statistics, foreign_hazard_curve.quantile, \
                        null, null, \
                        foreign_hazard_curve.imls, foreign_hazard_curve_data.poes \
                        FROM world_fishnet, foreign_hazard_curve, foreign_hazard_curve_data \
                        WHERE foreign_hazard_curve.id = %s \
                        AND foreign_hazard_curve_data.hazard_curve_id = foreign_hazard_curve.id \
                        AND ST_Intersects(foreign_hazard_curve_data.location::geometry, world_fishnet.cell)", (job_id, e['id']))
            con.commit()

    print "-------"
    print "Storing maps"

    for e in oq_map_ids:
    
        print " * OQ id: "+str(e['id'])

        cur = con.cursor()
        cur.execute("SELECT lons, lats, imls, poe \
                    FROM foreign_hazard_map \
                    WHERE id = %s ", (e['id'],))

        data = cur.fetchone()
        poe = data[3]
        i=0
        for lon, lat, iml in zip(data[0], data[1], data[2]):

            point = "POINT("+str(lon)+" "+str(lat)+")"

            if e['statistics'] == 'quantile':

                if e['imt'] != 'SA':

                    cur.execute("SELECT id \
                                FROM jobs_classical_psha_hazard_curves \
                                WHERE statistics    = 'quantile' \
                                AND quantile        = %s \
                                AND imt             = %s \
                                AND ST_Equals(location, st_geomfromtext(%s, 4326)) \
                                AND job_id = %s", (e['quantile'], e['imt'], point, job_id))

                else:

                    cur.execute("SELECT id \
                                FROM jobs_classical_psha_hazard_curves \
                                WHERE statistics = 'quantile' \
                                AND quantile     = %s \
                                AND imt          = %s \
                                AND sa_period    = %s \
                                AND ST_Equals(location, st_geomfromtext(%s, 4326)) \
                                AND job_id = %s", (e['quantile'], e['imt'], e['sa_period'], point, job_id))
            
            else: #mean

                if e['imt'] != 'SA':

                    cur.execute("SELECT id \
                                FROM jobs_classical_psha_hazard_curves \
                                WHERE statistics = 'mean' \
                                AND imt          = %s \
                                AND ST_Equals(location, st_geomfromtext(%s, 4326)) \
                                AND job_id = %s", (e['imt'], point, job_id))

                else:

                    cur.execute("SELECT id \
                                FROM jobs_classical_psha_hazard_curves \
                                WHERE statistics = 'mean' \
                                AND imt          = %s \
                                AND sa_period    = %s \
                                AND ST_Equals(location, st_geomfromtext(%s, 4326)) \
                                AND job_id = %s", (e['imt'], e['sa_period'], point, job_id))


            location = cur.fetchone()[0]
            if location != None:
                cur = con.cursor()
                cur.execute("INSERT INTO jobs_classical_psha_hazard_maps (location_id, iml, poe) \
                            VALUES (%s, %s, %s) ", (location, iml, poe))
                con.commit()
                i+=1
            else:
                print "Couldn't find the location for lon:", lon, ", lat:", lat, "IML:", iml, "poe:", poe

        print "Inserted:", i, "of", len(data[2]) 




def start(id, connection):
    print "-------"
    print "Starting calculating Classical PSHA hazard: "+str(id)

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


    params = dict(name = data[0],
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
    # oq_curves_ids, oq_map_ids = run(id, connection, FOLDER)
    # save(id, oq_curves_ids, oq_map_ids, connection)


    cur.execute("update jobs_classical_psha_hazard set status = 'FINISHED' where id = %s", (id,))
    connection.commit()
