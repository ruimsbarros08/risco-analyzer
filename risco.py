import scenario_hazard
import scenario_damage
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
