#! /usr/bin/python

import jinja2
import os
import json
import subprocess
import numpy
import csv

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




# def save_event_loss_table(job_id, vulnerability_models, hzrdr_oq_id, riskr_oq_id, exp_model_id, connection):
def save_event_loss_table(oq_job_id, vulnerability_models,hazard_job_id, investigation_time, nr_ses, connection, folder):
	print "-------"
	print "Storing Event Loss Tables"

	cur = connection.cursor()

	for model in vulnerability_models:
		if model['type'] == 'occupants'+'_vulnerability':
			loss_type = 'fatalities'
		else:
			loss_type =  model['type'].split('_vulnerability')[0]
		
		print '* '+loss_type

		#SAVE ALL ALTERNATIVE

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



		#SAVE LIST ALTERNATIVE

		default_vector = numpy.array([10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000, 10000])

		print 'Aggregate'
		#AGGREGATE
		cur.execute("SELECT sum(foreign_event_loss_asset.loss * jobs_event_based_hazard_ses_rupture.weight) as l \
					FROM foreign_output, foreign_event_loss, foreign_event_loss_asset, jobs_event_based_hazard_ses_rupture  \
					WHERE foreign_output.oq_job_id = %s \
					AND foreign_output.output_type = 'event_loss_asset' \
					AND foreign_event_loss.output_id = foreign_output.id \
					AND foreign_event_loss.loss_type = %s \
					AND foreign_event_loss_asset.event_loss_id = foreign_event_loss.id \
					AND foreign_event_loss_asset.rupture_id = jobs_event_based_hazard_ses_rupture.rupture_id \
					AND jobs_event_based_hazard_ses_rupture.job_id = %s \
					GROUP BY jobs_event_based_hazard_ses_rupture.ses_id \
					ORDER BY l DESC", (oq_job_id, loss_type, hazard_job_id))

		investigation_time_loss_values = [ loss[0] for loss in cur.fetchall() ]
		annual_time_loss_rates=(numpy.arange(1,nr_ses+1)/float(nr_ses))/float(investigation_time)
		period = 1/annual_time_loss_rates


		rows = zip(annual_time_loss_rates.tolist(), period.tolist(), investigation_time_loss_values)
		with open(folder+'/results_'+loss_type+'_aggrgate.csv', 'wb') as f:
			writer = csv.writer(f)
			for row in rows:
				writer.writerow(row)
		f.close()


		agg_def_periods = []
		for year in default_vector:
			if year > min(period) and year < max(period):
				agg_def_periods.append(year)

		agg_def_periods = numpy.array(agg_def_periods)
		investigation_time_loss_values_table = numpy.interp(agg_def_periods, period[::-1],investigation_time_loss_values[::-1])[::-1]
		a = investigation_time_loss_values * annual_time_loss_rates
		aal_agg = sum(a)
		tce_agg = numpy.interp(agg_def_periods, period[::-1], numpy.cumsum(a)[::-1])[::-1]

		print 'Occurrences'
		#OCCURRENCES
		cur.execute("SELECT foreign_event_loss_asset.loss * jobs_event_based_hazard_ses_rupture.weight AS l \
					FROM foreign_output, foreign_event_loss, foreign_event_loss_asset, jobs_event_based_hazard_ses_rupture  \
					WHERE foreign_output.oq_job_id = %s \
					AND foreign_output.output_type = 'event_loss_asset' \
					AND foreign_event_loss.output_id = foreign_output.id \
					AND foreign_event_loss.loss_type = %s \
					AND foreign_event_loss_asset.event_loss_id = foreign_event_loss.id \
					AND foreign_event_loss_asset.rupture_id = jobs_event_based_hazard_ses_rupture.rupture_id \
					AND jobs_event_based_hazard_ses_rupture.job_id = %s \
					ORDER BY l DESC", (oq_job_id, loss_type, hazard_job_id))

		#TOTAL VALUES
		investigation_time_loss_values_occ = [ loss[0] for loss in cur.fetchall() ]
		annual_time_loss_rates_occ=(numpy.arange(1,len(investigation_time_loss_values_occ)+1)/float(len(investigation_time_loss_values_occ)))/float(investigation_time)
		period_occ = 1/annual_time_loss_rates_occ

		rows = zip(annual_time_loss_rates_occ.tolist(), period_occ.tolist(), investigation_time_loss_values_occ)
		with open(folder+'/results_'+loss_type+'_occurences.csv', 'wb') as f:
			writer = csv.writer(f)
			for row in rows:
				writer.writerow(row)
		f.close()


		occ_def_periods = []
		for year in default_vector:
			if year > min(period_occ) and year < max(period_occ):
				occ_def_periods.append(year)

		occ_def_periods = numpy.array(occ_def_periods)
		investigation_time_loss_values_occ_table = numpy.interp(occ_def_periods, period_occ[::-1],investigation_time_loss_values_occ[::-1])[::-1]
		a = investigation_time_loss_values_occ * annual_time_loss_rates_occ
		aal_occ = sum(a)
		tce_occ = numpy.interp(occ_def_periods, period_occ[::-1], numpy.cumsum(a)[::-1])[::-1]


		#100 VALUES
		investigation_time_loss_values_occ = numpy.interp(numpy.arange(period_occ[-1], period_occ[0], (period_occ[0]-period_occ[-1])/100), period_occ[::-1], investigation_time_loss_values_occ[::-1])[::-1]
		annual_time_loss_rates_occ=(numpy.arange(1,len(investigation_time_loss_values_occ)+1)/float(len(investigation_time_loss_values_occ)))/float(investigation_time)
		period_occ = 1/annual_time_loss_rates_occ


		cur.execute("UPDATE jobs_classical_psha_risk_vulnerability \
					SET it_loss_values_agg = %s, \
					at_loss_rates_agg = %s, \
					periods_agg = %s, \
					default_periods_agg = %s, \
					it_loss_values_table_agg = %s, \
					aal_agg = %s, \
					tce_agg = %s, \
					it_loss_values_occ = %s, \
					at_loss_rates_occ = %s, \
					periods_occ = %s, \
					default_periods_occ = %s, \
					it_loss_values_table_occ = %s, \
					aal_occ = %s, \
					tce_occ = %s \
					WHERE id = %s", (investigation_time_loss_values,
									annual_time_loss_rates.tolist(),
									period.tolist(),
									agg_def_periods.tolist(),
									investigation_time_loss_values_table.tolist(),
									float(aal_agg),
									tce_agg.tolist(),
									investigation_time_loss_values_occ.tolist(),
									annual_time_loss_rates_occ.tolist(),
									period_occ.tolist(),
									occ_def_periods.tolist(),
									investigation_time_loss_values_occ_table.tolist(),
									float(aal_occ),
									tce_occ.tolist(),
									model['job_vul']) )
		connection.commit()

	cur.execute("DELETE FROM foreign_event_loss_asset \
				WHERE id IN \
				( SELECT  foreign_event_loss_asset.id \
				FROM foreign_output, foreign_event_loss, foreign_event_loss_asset \
				WHERE foreign_output.oq_job_id = %s \
				AND foreign_output.output_type = 'event_loss_asset' \
				AND foreign_event_loss.output_id = foreign_output.id \
				AND foreign_event_loss_asset.event_loss_id = foreign_event_loss.id )", (oq_job_id, ))
	connection.commit()


		#MARIO
		# print 'Populating SES_To_Risk'
		# cur.execute("INSERT INTO jobs_ses_to_risk (ses_collection_id, ses_output_id, gmf_id, hzrdr_job_id, riskr_job_id, weight) \
		#             SELECT B.id AS ses_collection_id, \
		#                     A.id AS ses_output_id, \
		#                     D.output_id AS gmf_id, %s, %s, \
		#                     E.weight \
		#             FROM foreign_output AS A, \
		#                 foreign_ses_collection AS B, \
		#                 foreign_assoc_lt_rlz_trt_model AS C, \
		#                 foreign_gmf AS D, \
		#                 foreign_lt_realization AS E \
		#             WHERE A.oq_job_id = %s \
		#             AND A.output_type = 'ses' \
		#             AND B.output_id = A.id \
		#             AND C.trt_model_id = B.trt_model_id \
		#             AND D.lt_realization_id = C.rlz_id \
		#             AND E.id = D.lt_realization_id;", (hzrdr_oq_id, riskr_oq_id, hzrdr_oq_id,))
		# connection.commit()

		# print 'Populating Risk'
		# cur.execute("INSERT INTO jobs_risk (rupture_id, loss, hazard_output_id, asset_id, job_vulnerability_id) \
		#             SELECT C.rupture_id, C.loss, B.hazard_output_id, E.id, %s \
		#             FROM foreign_output AS A, foreign_event_loss AS B, foreign_event_loss_asset AS C, \
		#             foreign_exposure_data AS D, eng_models_asset AS E \
		#             WHERE A.oq_job_id = %s \
		#             AND A.output_type = 'event_loss_asset' \
		#             AND B.loss_type = %s \
		#             AND B.output_id = A.id \
		#             AND C.event_loss_id = B.id \
		#             AND D.id = C.asset_id \
		#             AND D.asset_ref = E.name \
		#             AND E.model_id = %s ", (model['job_vul'], riskr_oq_id, loss_type, exp_model_id,))
		# connection.commit()


		# print 'Populating Loss'
		# cur.execute("INSERT INTO jobs_loss (ses_id, loss_total, hazard_output_id, job_vulnerability_id, asset_id, weight) \
		#             SELECT A.ses_id, sum(C.loss) AS loss_total, C.hazard_output_id, C.job_vulnerability_id, C.asset_id, B.weight \
		#             FROM hzrdr_eb AS A, ses_to_risk_eb AS B, riskr_eb AS C \
		#             WHERE B.riskr_job_id = %s \
		#             AND B.gmf_id = C.hazard_output_id \
		#             AND C.rupture_id = A.id \
		#             GROUP BY A.ses_id,B.weight, C.hazard_output_id,C.loss_type,C.asset_ref,C.taxonomy,C.site \
		#             ORDER BY A.ses_id;", (riskr_oq_id,))
		# connection.commit()


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


	# cur.execute('SELECT st_astext(region), max_distance FROM jobs_classical_psha_hazard WHERE id =%s', (params['hazard_id'], ))
	# hazard_data = cur.fetchone()

	# cur.execute('SELECT st_astext( ST_Intersection( ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326) ) )', (hazard_data[0], region_wkt))
	# assets_region = cur.fetchone()[0]

	assets = create_exposure_model(params['exposure_model_id'], connection, FOLDER, data[1])

	create_ini_file(params, vulnerability_models, assets, FOLDER)

	cur.execute('SELECT oq_id FROM jobs_classical_psha_hazard WHERE id = %s', (params['hazard_id'],))
	hazard_calculation_id = cur.fetchone()[0]

	oq_curves_ids, oq_map_ids, oq_job_id = run(id, hazard_calculation_id, connection, FOLDER)
	save(id, oq_curves_ids, oq_map_ids, connection)

	# save_event_loss_table(risk_output_id, vulnerability_models, params['exposure_model_id'], params['hazard_id'], connection)
	save_event_loss_table(oq_job_id, vulnerability_models, params['hazard_id'], params['investigation_time'], params['nr_ses'], connection, FOLDER)



	cur.execute("UPDATE jobs_classical_psha_risk SET status = 'FINISHED' WHERE id = %s", (id,))
	connection.commit()



