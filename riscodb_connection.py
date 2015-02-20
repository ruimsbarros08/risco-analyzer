#!/usr/bin/python
import psycopg2
 
def connect():
	conn_string = "host='priseDB.fe.up.pt' dbname='riscodb' user='postgres' password='prisefeup'"
	conn = psycopg2.connect(conn_string)
 	cursor = conn.cursor()
	return conn