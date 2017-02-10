# -*- coding: utf-8 -*-
import sys, getopt
import requests
import json
import codecs
# import csv
from insert_mysql import DBOperator

class Migrate:

	url_base = ''
	trans_table_name = ''
	lines_table_name = ''
	s = None
	dbo = None
	# transactinIdList = []
	# f_tran_list = None
	# f_tran = None
	# f_tran_lines = None
	count = 1
	lines_table_exists = False

	def __init__(self, mo):
		setup = self.getMOSetup(mo)
		self.url_base = setup['url_base']
		self.trans_table_name = setup['trans_table_name']
		self.lines_table_name = setup['lines_table_name']
		self.s = requests.Session()
		if(mo == 'mtjp' or mo == 'mtaus' or mo == 'mtkr'):
			self.s.auth = ('dummyusername1', 'dummypassword1')
		elif(mo == 'mtsea'):
			self.s.auth = ('dummyusername2', 'dummypassword2')
		self.dbo = DBOperator('root','dummyrootpassword','localhost','mtbmdb')

	def myprint(self, s):
	    sys.stdout.buffer.write(s.encode('cp932', errors='replace'))

	def getMOSetup(self, mo):
		url_base = ''
		trans_table_name = ''
		lines_table_name = ''

		if(mo == 'mtjp'):
			url_base = 'https://test.com/rest/v1/process1'
			trans_table_name = 'mtjp_transactions'
			lines_table_name = 'mtjp_lines'
		elif(mo == 'mtaus'):
			url_base = 'https://test.com/rest/v1/process2'
			trans_table_name = 'mtaus_transactions'
			lines_table_name = 'mtaus_lines'
		elif(mo == 'mtkr'):
			url_base = 'https://test.com/rest/v1/process3'
			trans_table_name = 'mtkr_transactions'
			lines_table_name = 'mtkr_lines'
		elif(mo == 'mtsea'):
			url_base = 'https://test.com/rest/v1/process4'
			trans_table_name = 'mtsea_transactions'
			lines_table_name = 'mtsea_lines'

		return {'url_base':url_base, 'trans_table_name':trans_table_name, 'lines_table_name':lines_table_name}

	def getTransactions(self, offset, limit, date, orderby):
		offset_param = ''
		offset_concat = ''
		limit_param = ''
		limit_concat = ''
		date_param = ''
		date_concat = ''
		orderby_param = ''
		orderby_concat = ''

		if(offset != 0):
			offset_param = "offset=" + str(offset)
			offset_concat = "&"
		if(limit != 0):
			limit_param = "limit=" + str(limit)
			limit_concat = "&"
		if(date != ''):
			date_param = "q={createdDate_quote:{$gte:'" + str(date) + " 00:00:00.0'}}"
			date_concat = "&"
		if(orderby != ''):
			orderby_param = "orderby=" + str(orderby)
			orderby_concat = "&"

		if(offset == 0 and limit == 0 and date == '' and orderby == ''):
			url = self.url_base
		else:
			url = self.url_base + "?" + date_param + date_concat + orderby_param + orderby_concat + limit_param + limit_concat + offset_param + offset_concat + "totalResults=true"

		offset = int(offset)
		print(url)
		r = self.s.get(url, timeout=None)
		print(r)
		JSON_object_lot = r.json()

		# json.dump(JSON_object_lot, self.f_tran_list, ensure_ascii=False, indent=4, sort_keys=True)

		for item in JSON_object_lot['items']:
			transacton_id = item['_id']
			# self.transactinIdList.append(transacton_id)
			url_transaction = self.url_base + '/' + transacton_id
			url_transaction_lines = url_transaction + '/line_process'

			r_tran = self.s.get(url_transaction, timeout=None)
			r_tran_lines = self.s.get(url_transaction_lines, timeout=None)

			JSON_object_tran = r_tran.json()
			JSON_object_tran_lines = r_tran_lines.json()

			print('Record #: ' + str(offset))
			print(url_transaction)
			print(url_transaction_lines)

			# json.dump(JSON_object_tran, self.f_tran, ensure_ascii=False, indent=4, sort_keys=True)
			# json.dump(JSON_object_tran_lines, self.f_tran_lines, ensure_ascii=False, indent=4, sort_keys=True)

			if(offset == 0):
				self.dbo.createTable(self.trans_table_name, JSON_object_tran)
			self.dbo.insertTable(self.trans_table_name, JSON_object_tran)

			for JSON_line in JSON_object_tran_lines['items']:
				if(self.lines_table_exists == False):
					self.dbo.createTable(self.lines_table_name, JSON_line)
					self.lines_table_exists = True
				self.dbo.insertTable(self.lines_table_name, JSON_line)

			offset = offset + 1

		if('hasMore' in JSON_object_lot):
			if(JSON_object_lot['hasMore'] == True):
				self.getTransactions(offset, limit, date, orderby)

def main(argv):
	mo = ''
	offset = 0
	limit = 0
	date = ''
	orderby = ''

	usage = "Usage: python %s -m <MO> -O <offset> -L <limit> -d <greater than or equal to the date added> -o <orderby field:asc or desc>" %argv[0]
	example = "For example: python %s -m mtjp -O 1001 -L 1000 -d 2015-01-01 -o createdDate_quote:asc" %argv[0]

	if(len(argv) == 1):
		print(usage)
		print(example)
		sys.exit(1)

	try:
		opts, args = getopt.getopt(argv[1:], "m:O:L:d:o:")
	except getopt.GetoptError as e:
		print(str(e))
		print(usage)
		print(example)
		sys.exit(1)

	for o, a in opts:
		if(o == '-m'):
			if(a not in ['mtjp', 'mtaus', 'mtkr','mtsea']):
				print("Please only input 'mtjp', 'mtaus', 'mtkr', or 'mtsea' as argument of -m")
				print("For example: python %s -m mtjp" %argv[0])
				sys.exit(1)

			mo = a
			print('MO: ' + mo)
			#sys.exit(0)

		if(o == '-O'):
			offset = a
			print('offset: ' + offset)

		if(o == '-L'):
			limit = a
			print('limit: ' + limit)

		if(o == '-d'):
			date = a
			print('greater than or equal to the date added: ' + date)

		if(o == '-o'):
			orderby = a
			print('orderby: ' + orderby)

	migrate = Migrate(mo)

	# migrate.f_tran_list = codecs.open("transaction_list-" + mo + ".json", "w", "utf-8")
	# migrate.f_tran = codecs.open("transactions-" + mo + ".json", "w", "utf-8")
	# migrate.f_tran_lines = codecs.open("transaction_lines-" + mo + ".json", "w", "utf-8")

	migrate.getTransactions(offset, limit, date, orderby)

	# migrate.f_tran_list.close()
	# migrate.f_tran.close()
	# migrate.f_tran_lines.close()

"""
	with codecs.open("transaction_Ids.csv", "w", "utf-8") as output:
		outputwriter = csv.writer(output)
		for row in migrate.transactinIdList:
			outputwriter.writerow([row])
"""
if __name__ == "__main__":
	main(sys.argv)