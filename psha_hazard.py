#! /usr/bin/python

import jinja2
import os
import json
import subprocess

from scenario_hazard import create_site_model

PATH = os.path.dirname(os.path.realpath(__file__)) + '/files/'

templateLoader = jinja2.FileSystemLoader(PATH + 'templates/')
templateEnv = jinja2.Environment(loader=templateLoader)



def create_logic_tree(id, type, con, folder):
    print "-------"
    print "Creating Logic Tree: "+type


    cur = con.cursor()
    cur.execute('SELECT id, level FROM eng_models_logic_tree_level WHERE logic_tree_id = %s ', (id,))
    levels = [ {'id': e[0], 'level': e[1]} for e in cur.fetchall()]

    for level in levels:
        cur.execute('SELECT id, uncertainty_type, level_id, origin_id \
                    FROM eng_models_logic_tree_branch_set \
                    WHERE level_id = %s ', (level['id'],))
        branch_sets = [{'id': e[0], 'uncertainty_type': e[1], 'sources': [], 'origin': e[3]} for e in cur.fetchall()]
        level['branch_sets'] = branch_sets

        for branch_set in level['branch_sets']:
            cur.execute('SELECT source_id \
                        FROM eng_models_logic_tree_branch_set_sources \
                        WHERE logic_tree_branch_set_id = %s', (branch_set['id'],))
            sources = [s[0] for s in cur.fetchall()]
            branch_set['sources'] = sources
            
            cur.execute('SELECT id, weight, a_b, b_inc, gmpe, max_mag, max_mag_inc, source_model_id \
                    FROM eng_models_logic_tree_branch \
                    WHERE branch_set_id = %s ', (branch_set['id'],))

            branches = [ {'id': b[0],
                            'weight': b[1],
                            'a_b': b[2],
                            'b_inc': b[3],
                            'gmpe': b[4],
                            'max_mag': b[5],
                            'max_mag_inc': b[6],
                            'source_model_id': b[7]} for b in cur.fetchall() ]

            branch_set['branches'] = branches

    logic_tree_template = templateEnv.get_template('logic_tree.jinja')
    logic_tree_output = logic_tree_template.render({'levels': levels})

    with open(folder+"/"+type+"_logic_tree.xml", "wb") as file:
        file.write(logic_tree_output)
        file.close()


def create_source_model(id, con, folder):
    print "-------"
    print "Creating Source Model"

    cur = con.cursor()
    cur.execute('SELECT id, name, tectonic_region, mag_scale_rel, rupt_aspect_ratio,  \
                mag_freq_dist_type, a, b, min_mag, max_mag, source_type, upper_depth, lower_depth, \
                nodal_plane_dist, hypo_depth_dist, dip, rake, st_astext(point), st_astext(fault), st_astext(area), \
                bin_width, occur_rates \
                FROM eng_models_source \
                WHERE model_id = %s ', (id,))

    sources = []

    for source in cur.fetchall():

        source_type = source[10]
        if source_type == 'POINT':
            geom = source[17].split('(')[2].split(')')[0]
        if source_type == 'SIMPLE_FAULT':
            geom = source[18].split('(')[1].split(')')[0]
        if source_type == 'AREA':
            geom = source[19].split('(')[2].split(')')[0]

        sources.append( {'id': source[0],
                        'name': source[1],
                        'tectonic_region': source[2],
                        'mag_scale_rel': source[3],
                        'rupt_aspect_ratio': source[4],
                        'mag_freq_dist_type': source[5],
                        'a': source[6],
                        'b': source[7],
                        'min_mag': source[8],
                        'max_mag': source[9],
                        'source_type': source_type,
                        'upper_depth': source[11],
                        'lower_depth': source[12],
                        'nodal_plane_dist': source[13],
                        'hypo_depth_dist': source[14],
                        'dip': source[15],
                        'rake': source[16],
                        'geom': geom,
                        'bin_width': source[20],
                        'occur_rates': source[21]})

    source_model_template = templateEnv.get_template('source_model.jinja')
    source_model_output = source_model_template.render({'model_id': id, 'sources': sources })

    with open(folder+"/source_model_"+str(id)+".xml", "wb") as file:
        file.write(source_model_output)
        file.close()



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
    print "Running Classical PSHA hazard..."
    
    cur = con.cursor()
    proc_hazard = subprocess.Popen(["/usr/local/openquake/oq-engine/bin/openquake", "--log-file", folder+"/log.txt", "--rh", folder+"/configuration.ini"], stdout=subprocess.PIPE)
    proc_hazard.wait()
    output_proc_hazard = proc_hazard.stdout.read().split("\n")
    hazard_output_id = output_proc_hazard[2].split()[0]

    oq_curves_ids = []
    oq_map_ids = []
    for e in output_proc_risk:
        try:
            a = e.split(' | ')
            if a[1] == 'Hazard Curve':
                oq_curves_ids.append(a[0])
            if a[1] == 'Hazard Map':
                oq_map_ids.append(a[0])
        except:
            pass

    return oq_curves_ids, oq_map_ids

    cur.execute('update jobs_classical_psha_hazard set oq_id = %s where id = %s', (hazard_output_id, job_id))
    con.commit()
    return hazard_output_id


def save(job_id, oq_curves_ids, oq_map_ids, con):
    print "-------"
    print "Storing curves"

    for id in oq_curves_ids:
        
        print " * OQ id: "+str(id)
        
        cur = con.cursor()
        cur.execute("INSERT INTO jobs_classical_psha_hazard_curves (job_id, location, cell_id, imt, sa_period, sa_damping, \
                    weight, statistics, quantile, sm_lt_path, gsim_lt_path, imls, poes) \
                    SELECT %s, foreign_hazard_curve_data.location::geometry, world_fishnet.id, \
                    foreign_hazard_curve.imt, foreign_hazard_curve.sa_period, foreign_hazard_curve.sa_damping, \
                    foreign_hazard_curve_data.weight, foreign_hazard_curve.statistics, foreign_hazard_curve.quantile, \
                    foreign_lt_realization.gsim_lt_path, foreign_lt_source_model.sm_lt_path, \
                    foreign_hazard_curve.imls, foreign_hazard_curve_data.poes \
                    FROM world_fishnet, foreign_hazard_curve, foreign_hazard_curve_data, foreign_lt_realization, foreign_lt_source_model \
                    WHERE foreign_hazard_curve.output_id = %s \
                    AND foreign_hazard_curve_data.hazard_curve_id = foreign_hazard_curve.id \
                    AND foreign_lt_realization.id = foreign_hazard_curve.lt_realization_id \
                    AND foreign_lt_realization.lt_model_id = foreign_lt_source_model.id \
                    AND ST_Intersects(foreign_hazard_curve_data.location::geometry, world_fishnet.cell)", (job_id, id))
        con.commit()

    print "-------"
    print "Storing maps"
    
    for id in oq_map_ids:

        print " * OQ id: "+str(id)


        cur = con.cursor()
        cur.execute("SELECT lons, lats, imls, poe \
                    FROM foreign_hazard_map \
                    WHERE output_id = %s ", (id))

        data = cur.fetchone()
        poe = data[3]
        i=0
        for lon, lat, iml in zip(data[0], data[1], data[2]):

            point = "POINT("+str(lon)+" "+str(lat)+")"
            cur.execute('SELECT id \
                        FROM jobs_classical_psha_hazard_curves \
                        WHERE ST_Equals(location, st_geomfromtext(%s)) \
                        AND job_id = %s', (point, job_id))

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
    cur.execute('select name, st_astext(region), grid_spacing, \
                sites_type, vs30, vs30type, z1pt0, z2pt5, site_model_id, random_seed, \
                rupture_mesh_spacing, truncation_level, max_distance, \
                n_lt_samples, width_of_mfd_bin, area_source_discretization, investigation_time, \
                imt_l, poes, quantile_hazard_curves \
                from jobs_classical_psha_hazard \
                where id = %s', (id,))
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
                quantile_hazard_curves = data[19])
    
    cur = connection.cursor()
    cur.execute('select current_database()')
    db_name = cur.fetchone()[0]

    FOLDER = PATH + db_name + "/psha_hazard/"+str(id)

    try:
        os.makedirs(FOLDER)
    except:
        pass

    if params['sites_type'] == 'VARIABLE_CONDITIONS':
        create_site_model(params['site_model_id'], connection, FOLDER)
    


    cur.execute('SELECT jobs_classical_psha_hazard_logic_trees.logic_tree_id, eng_models_logic_tree.type \
                FROM jobs_classical_psha_hazard_logic_trees, eng_models_logic_tree \
                WHERE jobs_classical_psha_hazard_logic_trees.classical_psha_hazard_id = %s \
                AND eng_models_logic_tree.id = jobs_classical_psha_hazard_logic_trees.logic_tree_id', (id,))

    for tree in cur.fetchall():
        create_logic_tree(tree[0], tree[1], connection, FOLDER)

        if tree[1] == 'source':

            cur.execute('SELECT source_model_id \
                    FROM eng_models_logic_tree_source_models \
                    WHERE logic_tree_id = %s ', (tree[0],))

            for source in cur.fetchall():
                create_source_model(source[0], connection, FOLDER)


    create_ini_file(params, FOLDER)
    oq_curves_ids, oq_map_ids = run(id, connection, FOLDER)
    save(id, oq_curves_ids, oq_map_ids, connection)


    cur.execute("update jobs_classical_psha_hazard set status = 'FINISHED' where id = %s", (id,))
    connection.commit()
