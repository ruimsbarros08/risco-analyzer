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


def create_ini_file(params, folder):
    print "-------"
    print "Creating .ini file"

    conf_template = templateEnv.get_template('configuration_psha_risk.jinja')
    conf_output = conf_template.render(params)

    with open(folder+"/configuration.ini", "wb") as file:
        file.write(conf_output)
        file.close()



def run(job_id, con, folder):
    print "-------"
    print "Running Classical PSHA risk..."
    
    cur = con.cursor()
    proc = subprocess.Popen(["oq-engine", "--log-file", folder+"/log.txt", "--rr", folder+"/configuration.ini", "--hazard-output-id", str(hazard_id)], stdout=subprocess.PIPE)
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

    cur.execute('update jobs_classical_psha_risk set oq_id = %s where id = %s', (risk_output_id, job_id))
    con.commit()


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

    params = dict(name = data[0],
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


    cur.execute('select vulnerability_model_id from jobs_classical_psha_risk_vulnerability_models \
                where classical_psha_risk_id = %s', [id])
    for model in cur.fetchall():
        create_vulnerability_model(model[0], connection, FOLDER)

    create_exposure_model(params['exposure_model_id'], connection, FOLDER, region_wkt)

    create_ini_file(params, FOLDER)
    run(id, connection, FOLDER)
    #save(id, oq_curves_ids, oq_map_ids, connection)


    #cur.execute("update jobs_classical_psha_risk set status = 'FINISHED' where id = %s", (id,))
    #connection.commit()
