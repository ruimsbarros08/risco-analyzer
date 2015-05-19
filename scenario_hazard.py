#! /usr/bin/python

import jinja2
import os
import json
import subprocess

PATH = os.path.dirname(os.path.realpath(__file__)) + '/files/'

templateLoader = jinja2.FileSystemLoader(PATH + 'templates/')
templateEnv = jinja2.Environment(loader=templateLoader)


def create_site_model(job_id, con, folder):
    print "-------"
    print "Creating Site Model"

    cur = con.cursor()
    cur.execute('select st_x(eng_models_site.location), st_y(eng_models_site.location), \
                eng_models_site.vs30, eng_models_site.vs30type, eng_models_site.z1pt0, eng_models_site.z2pt5 \
                from eng_models_site, jobs_scenario_hazard \
                where eng_models_site.model_id = jobs_scenario_hazard.site_model_id \
                and jobs_scenario_hazard.id = %s', (job_id,))

    sites = [dict(lon = site[0],
                    lat = site[1],
                    vs30 = site[2],
                    vs30type = site[3],
                    z1pt0 = site[4],
                    z2pt5 = site[5]) for site in cur.fetchall()]

    site_template = templateEnv.get_template('site_model.jinja')
    site_output = site_template.render(dict(sites=sites))

    with open(folder+"/site_model.xml", "wb") as file:
        file.write(site_output)
        file.close()



def create_rupture_model(job_id, con, folder):
    print "-------"
    print "Creating Rupture Model"

    cur = con.cursor()
    cur.execute('select st_x(location), st_y(location), rupture_type, magnitude, depth, rake, upper_depth, lower_depth, dip, st_asgeojson(rupture_geom) \
                from eng_models_rupture_model, jobs_scenario_hazard \
                where eng_models_rupture_model.id = jobs_scenario_hazard.rupture_model_id \
                and jobs_scenario_hazard.id = %s', (job_id,))
    data = cur.fetchone()

    if data[2] == 'POINT':
        rupt = dict(lon = data[0],
                    lat = data[1],
                    magnitude = data[3],
                    depth = data[4],
                    rake = data[5],
                    dip = data[8])
        rupture_template = templateEnv.get_template('rupture_point_source.jinja')
        rupture_output = rupture_template.render(rupt)

    else:
        rupt = dict(lon = data[0],
                    lat = data[1],
                    rupture_type = data[2],
                    magnitude = data[3],
                    depth = data[4],
                    rake = data[5],
                    upper_depth = data[6],
                    lower_depth = data[7],
                    dip = data[8],
                    fault = json.loads(data[9]))
        rupture_template = templateEnv.get_template('rupture_fault_source.jinja')
        rupture_output = rupture_template.render(rupt)

    with open(folder+"/rupture_model.xml", "wb") as file:
        file.write(rupture_output)
        file.close()



def create_ini_file(job_id, params, folder):
    print "-------"
    print "Creating .ini file"

    conf_template = templateEnv.get_template('configuration_scenario_hazard.jinja')
    conf_output = conf_template.render(params)

    with open(folder+"/configuration.ini", "wb") as file:
        file.write(conf_output)
        file.close()



def run(job_id, con, folder):
    print "-------"
    print "Running scenario hazard..."
    
    cur = con.cursor()
    proc_hazard = subprocess.Popen(["/usr/local/openquake/oq-engine/bin/openquake", "--log-file", "/dev/null", "--rh", folder+"/configuration.ini"], stdout=subprocess.PIPE)
    proc_hazard.wait()
    output_proc_hazard = proc_hazard.stdout.read().split("\n")
    hazard_output_id = output_proc_hazard[2].split()[0]
    cur.execute('update jobs_scenario_hazard set oq_id = %s where id = %s', (hazard_output_id, job_id))
    con.commit()
    return hazard_output_id


def save(job_id, oq_id, con):
    print "-------"
    print "Storing the results. OQ_id: "+str(oq_id)
    
    cur = con.cursor()
    cur.execute("INSERT INTO jobs_scenario_hazard_results (job_id, location, imt, sa_period, sa_damping, gmvs, cell_id) \
            SELECT %s, location::geometry, imt, sa_period, sa_damping, gmvs[1], world_fishnet.id \
            FROM foreign_hazard_site, foreign_gmf_data, foreign_gmf, world_fishnet \
            WHERE foreign_hazard_site.id = foreign_gmf_data.site_id \
            AND foreign_gmf_data.gmf_id = foreign_gmf.id \
            AND foreign_gmf.output_id = %s \
            AND ST_Intersects(foreign_hazard_site.location::geometry, world_fishnet.cell) ", (job_id, oq_id))

    con.commit()


def aggregate(job_id, pga, sa_periods, con):
    print "-------"
    print "Making Aggregations"

    cursor = con.cursor()

    if pga:
        print '* PGA'
        cursor.execute("INSERT INTO jobs_scenario_hazard_results_by_cell (imt, sa_period, gmvs_mean, cell_id, job_id) \
                        SELECT 'PGA', NULL, AVG(gmvs), world_fishnet.id, %s  \
                        FROM world_fishnet, jobs_scenario_hazard_results \
                        WHERE jobs_scenario_hazard_results.job_id = %s \
                        AND jobs_scenario_hazard_results.cell_id = world_fishnet.id \
                        AND jobs_scenario_hazard_results.imt = 'PGA' \
                        GROUP BY world_fishnet.id", [job_id, job_id])
        con.commit()

    for e in sa_periods:
        print '*', e
        cursor.execute("INSERT INTO jobs_scenario_hazard_results_by_cell (imt, sa_period, gmvs_mean, cell_id, job_id) \
                        SELECT 'SA', %s, AVG(gmvs), world_fishnet.id, %s  \
                        FROM world_fishnet, jobs_scenario_hazard_results \
                        WHERE jobs_scenario_hazard_results.job_id = %s \
                        AND jobs_scenario_hazard_results.cell_id = world_fishnet.id \
                        AND jobs_scenario_hazard_results.imt = 'SA' \
                        GROUP BY world_fishnet.id", [e, job_id, job_id])
        con.commit()


def start(id, connection):
    print "-------"
    print "Starting calculating the scenario hazard: "+str(id)

    cur = connection.cursor()
    cur.execute('select name, st_astext(region), grid_spacing, sites_type, vs30, vs30type, z1pt0, z2pt5, \
                random_seed, rupture_mesh_spacing, pga, sa_periods, \
                truncation_level, max_distance, gmpe, correlation_model, vs30_clustering, n_gmf \
                from jobs_scenario_hazard \
                where id = %s', (id,))
    data = cur.fetchone()

    imt_list = []
    
    pga = data[10]
    sa_periods = data[11]

    if data[10]:
        imt_list.append('PGA')
    
    imt_list = imt_list + ['SA('+str(i)+')' for i in data[11]]
    imt_str = ', '.join([str(i) for i in imt_list])


    params = dict(name = data[0],
                region = data[1].split('(')[2].split(')')[0],
                grid_spacing = data[2],
                sites_type = data[3],
                vs30 = data[4],
                vs30type = data[5],
                z1pt0 = data[6],
                z2pt5 = data[7],
                random_seed = data[8],
                rupture_mesh_spacing = data[9],
                imt_list = imt_str,
                truncation_level = data[12],
                max_distance = data[13],
                gmpe = data[14],
                correlation_model = data[15],
                vs30_clustering = data[16],
                n_gmf = data[17])
    
    cur = connection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchone()[0]

    FOLDER = PATH + db_name + "/scenario_hazard/"+str(id)

    try:
        os.makedirs(FOLDER)
    except:
        pass

    if params['sites_type'] == 'VARIABLE_CONDITIONS':
        create_site_model(id, connection, FOLDER)
    
    create_rupture_model(id, connection, FOLDER)
    create_ini_file(id, params, FOLDER)
    oq_id = run(id, connection, FOLDER)
    save(id, oq_id, connection)

    aggregate(id, pga, sa_periods, connection)

    cur.execute("update jobs_scenario_hazard set status = 'FINISHED' where id = %s", (id,))
    connection.commit()
