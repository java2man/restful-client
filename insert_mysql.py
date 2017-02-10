# -*- coding: utf-8 -*-
import sys
import mysql.connector
from mysql.connector import conversion

class DBOperator:
	def __init__(self, user, password, host, database):
		self.conn = mysql.connector.connect(user=user, password=password, host=host, database=database)
		self.cur = self.conn.cursor()

	def myprint(self, s):
	    sys.stdout.buffer.write(s.encode('cp932', errors='replace'))

	def createTable(self, table_name, json):
		sql = "create table IF NOT EXISTS " + table_name + "(" 
		keys = json.keys()
		for key in keys:
			if(key == 'links'):
				continue
			if(key == 'group'):
				key = '_group'
			if(key == '_id'):
				sql = sql + key + " INT NOT NULL PRIMARY KEY,"
			else:
				sql = sql + key + " " + "TEXT,"
		sql = sql[:-1] + ")"
		
		#self.myprint(sql)
		self.cur.execute(sql)
		self.conn.commit()
		self.cur.close
		self.conn.close

	def insertTable(self, table_name, json):
		sql_insert = "insert ignore into " + table_name + "("
		sql_values = "values("
		keys = json.keys()
		for key in keys:
			value = str(json[key])
			if(key == 'links'):
				continue
			if(key == 'group'):
				key = '_group'
			sql_insert = sql_insert + key + ","
			sql_values = sql_values + "'" + (value.replace("'", "''")).replace("\\", "\\\\") + "',"

		sql = sql_insert[:-1] + ") " + sql_values[:-1] + ")"

		#self.myprint(sql)
		self.addColumnIfNeed(table_name, sql)

		#self.cur.execute(sql)
		self.conn.commit()
		self.cur.close
		self.conn.close

	def alterTable(self, table_name, column_name):
		sql_alter = "ALTER TABLE " + table_name + " ADD COLUMN " + column_name + " TEXT"

		self.cur.execute(sql_alter)
		self.conn.commit()

	def addColumnIfNeed(self, table_name, sql):
		try:
			self.cur.execute(sql)
		except mysql.connector.ProgrammingError as e:
			str1 = "Unknown column '"
			str2 = "' in 'field list'"
			field = ''
			if(str1 in str(e) and str2 in str(e)):
				index1 = str(e).index(str1) + len(str1)
				field = str(e)[index1:len(str(e)) - len(str2)]
				print(field)
				self.alterTable(table_name, field)
				self.addColumnIfNeed(table_name, sql)