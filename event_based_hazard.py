#! /usr/bin/python

import jinja2
import os
import json
import subprocess

from scenario_hazard import create_site_model
from scenario_damage import create_exposure_model
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

def save_ses_ruptures(job_id, hazard_output_id, con):
	print "-------"
	print "Storing SES Ruptures"
	
	cur = con.cursor()

	cur.execute("SELECT id FROM foreign_output WHERE oq_job_id = %s AND output_type = 'ses'", (hazard_output_id,))
	data = cur.fetchall()

	for id in data:
		print "output id: "+str(id[0])

		# cur.execute("INSERT INTO jobs_event_based_hazard_ses_rupture (job_id, output_id, ses_id, rupture_id, rake, magnitude, location, depth) \
		# 			SELECT %s, c.output_id, a.ses_id, a.rupture_id, b.rake, b.magnitude, ST_GeomFromText('POINT(' || b._hypocenter[1] || ' ' || b._hypocenter[2] || ')', 4326), b._hypocenter[3]  \
		# 			FROM foreign_ses_rupture as a, foreign_probabilistic_rupture as b, foreign_ses_collection as c \
		# 			WHERE c.output_id = %s \
		# 			AND b.ses_collection_id = c.id \
		# 			AND a.rupture_id = b.id", (job_id, id[0],))
		# con.commit()

		cur.execute("INSERT INTO jobs_event_based_hazard_ses_rupture (job_id, output_id, ses_id, rupture_id, rake, magnitude, location, depth, weight, rupture_oq) \
					SELECT %s, foreign_ses_collection.output_id, foreign_ses_rupture.ses_id, foreign_ses_rupture.id, foreign_probabilistic_rupture.rake, \
					foreign_probabilistic_rupture.magnitude, \
					ST_GeomFromText('POINT(' || foreign_probabilistic_rupture._hypocenter[1] || ' ' || foreign_probabilistic_rupture._hypocenter[2] || ')', 4326), \
					foreign_probabilistic_rupture._hypocenter[3], foreign_lt_realization.weight, foreign_ses_rupture.rupture_id  \
					FROM foreign_ses_rupture, foreign_probabilistic_rupture, foreign_ses_collection, \
					foreign_assoc_lt_rlz_trt_model, foreign_gmf, foreign_lt_realization \
					WHERE foreign_ses_collection.output_id = %s \
					AND foreign_probabilistic_rupture.ses_collection_id = foreign_ses_collection.id \
					AND foreign_ses_rupture.rupture_id = foreign_probabilistic_rupture.id \
					AND foreign_assoc_lt_rlz_trt_model.trt_model_id = foreign_ses_collection.trt_model_id \
					AND foreign_gmf.lt_realization_id = foreign_assoc_lt_rlz_trt_model.rlz_id \
					AND foreign_lt_realization.id = foreign_gmf.lt_realization_id", (job_id, id[0],))
		con.commit()




def start(id, connection):
	print "-------"
	print "Starting calculating Event Based Hazard PSHA hazard: "+str(id)

	cur = connection.cursor()

	cur.execute('select current_database()')
	db_name = cur.fetchone()[0]

	FOLDER = PATH + db_name + "/event_based_hazard/"+str(id)

	try:
		os.makedirs(FOLDER)
	except:
		pass

	cur.execute('SELECT jobs_classical_psha_hazard.name, st_astext(jobs_classical_psha_hazard.region), jobs_classical_psha_hazard.grid_spacing, \
				jobs_classical_psha_hazard.sites_type, jobs_classical_psha_hazard.vs30, jobs_classical_psha_hazard.vs30type, jobs_classical_psha_hazard.z1pt0, jobs_classical_psha_hazard.z2pt5, jobs_classical_psha_hazard.site_model_id, \
				jobs_classical_psha_hazard.random_seed, jobs_classical_psha_hazard.rupture_mesh_spacing, jobs_classical_psha_hazard.truncation_level, jobs_classical_psha_hazard.max_distance, \
				jobs_classical_psha_hazard.n_lt_samples, jobs_classical_psha_hazard.width_of_mfd_bin, jobs_classical_psha_hazard.area_source_discretization, jobs_classical_psha_hazard.investigation_time, \
				jobs_classical_psha_hazard.imt_l, jobs_classical_psha_hazard.poes, jobs_classical_psha_hazard.quantile_hazard_curves, jobs_classical_psha_hazard.gmpe_logic_tree_id, jobs_classical_psha_hazard.sm_logic_tree_id, \
				jobs_event_based_hazard.ses_per_logic_tree_path, jobs_classical_psha_hazard.locations_type, \
				st_astext(jobs_classical_psha_hazard.locations), jobs_classical_psha_hazard.exposure_model_id \
				FROM jobs_classical_psha_hazard, jobs_event_based_hazard \
				WHERE jobs_event_based_hazard.classical_psha_hazard_ptr_id = %s \
				AND jobs_event_based_hazard.classical_psha_hazard_ptr_id = jobs_classical_psha_hazard.id', (id,))
	data = cur.fetchone()

	params = dict(type='event_based',
				name = data[0],
				region = data[1],
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
				ses_per_logic_tree_path= data[22],
				locations_type= data[23],
				locations= data[24],
				exposure_model= data[25])

	if params['locations_type'] == 'EXPOSURE':
		create_exposure_model(params['exposure_model'], connection, FOLDER, params['region'])
		# params['region'] = params['region'].split('(')[2].split(')')[0]

	elif params['locations_type'] == 'GRID':
		params['region'] = params['region'].split('(')[2].split(')')[0]

	else:
		params['locations'] = params['locations'].replace('MULTIPOINT (', '').replace(')', '').split(',')


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
	oq_curves_ids, oq_map_ids, hazard_output_id = run(id, connection, FOLDER)
	save(id, oq_curves_ids, oq_map_ids, connection)
	save_ses_ruptures(id, hazard_output_id, connection)

	cur.execute("update jobs_classical_psha_hazard set status = 'FINISHED' where id = %s", (id,))
	connection.commit()
