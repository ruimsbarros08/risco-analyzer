
import controller
import scenario_risk
import scenario_hazard

job_id = 8
job_type = 'scenario_risk'
database = {'HOST': 'priseDB.fe.up.pt', 'NAME': 'riscodb_dev', 'USER': 'postgres', 'PASSWORD': 'prisefeup'}

con = controller.connect(database['HOST'], database['NAME'], database['USER'], database['PASSWORD'])

#controller.start(job_id, job_type, database) 

data = ['Calculation 111 completed in 617 seconds. Results:',
		'  id | output_type | name', '1846 | Aggregate Losses | aggregate loss. type=fatalities',
		'1847 | Aggregate Losses | aggregate loss. type=nonstructural',
		'1848 | Aggregate Losses | aggregate loss. type=structural',
		'1843 | Loss Map | Loss Map',
		'1844 | Loss Map | Loss Map',
		'1845 | Loss Map | Loss Map',
		'']

def parse_risk_output(data):
	oq_ids = []
	for e in data:
		try:
			a = e.split(' | ')
			if a[1] == 'Loss Map':
				oq_ids.append(a[0])
		except:
			pass
	return oq_ids

#print parse_risk_output(data)


def test_save_scenario_hazard(job_id, oq_id, con):
	scenario_hazard.save(job_id, oq_id, con)

#oq_id = 1830
#test_save_scenario_hazard(job_id, oq_id, con)

def test_save_scenario_risk(id_list, job_id, con):
    for oq_id in id_list:
		scenario_risk.save(job_id, oq_id, con)

#test_save_scenario_risk(['1843', '1844', '1845'], job_id, con)

pga = True
sa_periods = []

def test_hazard_aggragation(job_id, pga, sa_periods, con):
	scenario_hazard.aggregate(job_id, pga, sa_periods, con)

test_hazard_aggragation(job_id, pga, sa_periods, con)