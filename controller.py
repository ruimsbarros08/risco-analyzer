import scenario_hazard
import scenario_damage
import scenario_risk
import psha_hazard
import psha_risk
import event_based_hazard
import event_based_risk
import psycopg2

def connect(host, dbname, user, password):
        conn_string = 'host=' + host + ' dbname=' + dbname + ' user=' + user + ' password=' + password
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        return conn

def start(job_id, job_type, database):
	connection = connect(database['HOST'], database['NAME'], database['USER'], database['PASSWORD'])
	if job_type == 'scenario_hazard':
		scenario_hazard.start(job_id, connection)

	if job_type == 'scenario_damage':
		scenario_damage.start(job_id, connection)

	if job_type == 'scenario_risk':
		scenario_risk.start(job_id, connection)

	if job_type == 'psha_hazard':
		psha_hazard.start(job_id, connection)

	if job_type == 'psha_risk':
		psha_risk.start(job_id, connection)

	if job_type == 'event_based_hazard':
		event_based_hazard.start(job_id, connection)

	if job_type == 'event_based_risk':
		event_based_risk.start(job_id, connection)