# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import calendar
import logging
import re

from itemadapter import ItemAdapter, adapter
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


class DataCleanPipeline(object):
	def __init__(self):		
		logging.info("****Data cleaning****")		
	
	def process_item(self, item, spider):
		adapter = ItemAdapter(item)

		def removeGr(text):
			text = text.replace('.', '').replace('Gr', '').replace(' ', '')

			return text

		def removePoint(text):
			text = text.replace('.', '')

			return text

		item['mpCode'] = item['mpCode'][0]
		item['periodeRekon'] = item['periodeRekon'][0]

		if adapter.get('berat'):
			item['berat'] = removeGr(item['berat'][0])
		else:
			item['berat'] = 0

		if adapter.get('beaDasar'):
			item['beaDasar'] = removePoint(item['beaDasar'][0])
		else:
			item['beaDasar'] = 0
		
		if adapter.get('nilaiBarang'):
			item['nilaiBarang'] = removePoint(item['nilaiBarang'][0])
		else:
			item['nilaiBarang'] = 0

		if not adapter.get('isiKiriman'):
			item['isiKiriman'] = ''
		else:
			item['isiKiriman'] = item['isiKiriman'][0]
		
		if not adapter.get('htnb'):
			item['htnb'] = ''
		else:
			item['htnb'] = removeGr(item['htnb'][0])

		if not adapter.get('awbKendali'):
			item['awbKendali'] = ''
		else:
			item['awbKendali'] = item['awbKendali'][0]

		if not adapter.get('awbRekon'):
			item['awbRekon'] = ''
		else:
			item['awbRekon'] = item['awbRekon'][0]

		if not adapter.get('jenisLayanan'):
			item['jenisLayanan'] = ''
		else:
			item['jenisLayanan'] = item['jenisLayanan'][0]			

		if not adapter.get('statusAkhir'):
			item['statusAkhir'] = ''
		else:
			item['statusAkhir'] = item['statusAkhir'][0]
		
		if not adapter.get('tanggalPosting'):
			item['tanggalPosting'] = ''
		else:
			item['tanggalPosting'] = item['tanggalPosting']

		if not adapter.get('tanggalStatusAkhir'):
			item['tanggalStatusAkhir'] = ''
		else:
			item['tanggalStatusAkhir'] = item['tanggalStatusAkhir'][0]

		if not adapter.get('tanggalKirim'):
			item['tanggalKirim'] = ''
		else:
			item['tanggalKirim'] = item['tanggalKirim'][0]

		if not adapter.get('tanggalPosting'):
			item['tanggalPosting'] = ''
		else:
			item['tanggalPosting'] = item['tanggalPosting'][0]

		if not adapter.get('jenisKiriman'):
			item['jenisKiriman'] = ''
		else:
			item['jenisKiriman'] = item['jenisKiriman'][0]
		
		item['tahunRekon'] = item['tahunRekon'][0]

		return item


class DataParsePipeline(object):
	def __init__(self):		
		logging.info("****Data parsing****")

	def process_item(self, item, spider):
		adapter = ItemAdapter(item)

		def getPengirimPenerima(text):
			x = text.split(';')

			return x[0]

		def getKodePos(text):
			text = re.findall(';[0-9][0-9][0-9][0-9][0-9];', text)
			if (len(text) > 0):
				text = text[0].replace(';', '')
			else:
				text = ''

			return text

		def getNopendKantor(text):
			text = re.findall('^[0-9A-Z]*', text)

			return text[0]

		def getKotaPengirimPenerima(text):
			text = text.split(';')

			return text[3]

		def getNamaKantor(text):
			text = re.split('^[0-9A-Z]*', text)[1].lstrip()

			return text

		def removeColon(text):
			x = text.split(':')

			return x[1].lstrip()

		if adapter.get('pengirim'):			
			item['pengirim'] = getPengirimPenerima(item['pengirim'][0])
		else:
			item['pengirim'] = ''

		if adapter.get('penerima'):
			item['penerima'] = getPengirimPenerima(item['penerima'][0])
		else:
			item['penerima'] = ''

		if adapter.get('kodePosPenerima'):
			item['kodePosPenerima'] = getKodePos(item['kodePosPenerima'][0])
		else:
			item['kodePosPenerima'] = ''

		if adapter.get('kodePosPengirim'):
			item['kodePosPengirim'] = getKodePos(item['kodePosPengirim'][0])
		else:
			item['kodePosPengirim'] = ''

		if adapter.get('nopendKantorKirim'):
			item['nopendKantorKirim'] = getNopendKantor(item['nopendKantorKirim'][0])
		else:
			item['nopendKantorKirim'] = ''

		if adapter.get('nopendKantorAkhir'):
			item['nopendKantorAkhir'] = getNopendKantor(item['nopendKantorAkhir'][0])
		else:
			item['nopendKantorAkhir'] = ''

		if adapter.get('kotaPengirim'):			
			item['kotaPengirim'] = getKotaPengirimPenerima(item['kotaPengirim'][0])
		else:
			item['kotaPengirim'] = ''

		if adapter.get('kotaPenerima'):			
			item['kotaPenerima'] = getKotaPengirimPenerima(item['kotaPenerima'][0])
		else:
			item['kotaPenerima'] = ''

		if adapter.get('kantorKirim'):
			item['kantorKirim'] = getNamaKantor(item['kantorKirim'][0])
		else:
			item['kantorKirim'] = ''
		
		if adapter.get('kantorAkhir'):
			item['kantorAkhir'] = getNamaKantor(item['kantorAkhir'][0])
		else:
			item['kantorAkhir'] = ''

		# sum up beaDasar & htnb
		if adapter.get('beaDasar'):
			if adapter.get('htnb'):
				item['beaTotal'] = int(item['beaDasar']) + int(item['htnb'])	
			else:
				item['beaTotal'] = int(item['beaDasar'])
		else:
			item['beaTotal'] = 0		

		# get delivery status remark
		if adapter.get('statusAkhir'):
			if ('SELESAI ANTAR' in item['statusAkhir']):
				try:
					item['statusAntar'] = item['statusAkhir']
					if ('WAKTU' in item['ketStatusAkhir'][1]):
						item['waktuUpdateStatus'] = removeColon(item['ketStatusAkhir'][1])

					if ('STATUS' in item['ketStatusAkhir'][2]):
						item['ketStatusAntar'] = removeColon(item['ketStatusAkhir'][2])					

					if ('PENERIMA' in item['ketStatusAkhir'][3]):
						item['penerimaKiriman'] = removeColon(item['ketStatusAkhir'][3])							
				except IndexError:					
					item['waktuUpdateStatus'] = ''
					item['ketStatusAntar'] = ''			
					item['penerimaKiriman'] = ''
					#raise e
		else:	
			item['statusAkhir'] = ''
			item['statusAntar'] = ''
			item['ketStatusAntar'] = ''
			item['penerimaKiriman'] = ''

		return item

		
class RekonStatusPipeline(object):
	def __init__(self):		
		logging.info("****Rekon status****")

	def process_item(self, item, spider):
		adapter = ItemAdapter(item)
		
		def getStatusRekon(tglKirim, periodeRekon, tahunRekon):
			#periodeRekon = '12'
			x = tglKirim.split('-')

			#if (x[1] == periodeRekon):
			if (x[1] == periodeRekon) & (x[2] == tahunRekon):
				return True
			else:
				return False

		# check package validity
		if adapter.get('tanggalKirim') and adapter.get('jenisLayanan'):							
			if (getStatusRekon(item['tanggalKirim'], item['periodeRekon'], item['tahunRekon']) == True):
				if (('KILAT KHUSUS' in item['jenisLayanan']) == True) | (('Express' in item['jenisLayanan']) == True):
					item['statusRekon'] = 'VALID'
					item['ketRekon'] = 'VALID'
				else:
					item['statusRekon'] = 'INVALID'
					item['ketRekon'] = 'DI LUAR PRODUK'
			else:
				item['statusRekon'] = 'INVALID'
				item['ketRekon'] = 'DI LUAR PERIODE'				
		else:
			item['statusRekon'] = 'INVALID'
			item['ketRekon'] = 'PRODUK TIDAK DIKENAL'

		return item


class RekonStatusTopedPipeline(object):
	def __init__(self):		
		logging.info("****Rekon status Tokopedia****")

	def process_item(self, item, spider):
		adapter = ItemAdapter(item)

		def getStatusRekon(tglKirim, periodeRekon, tahunRekon):
			x = tglKirim.split('-')

			#if (x[1] == periodeRekon):
			if (x[1] == periodeRekon) & (x[2] == tahunRekon):
				return True
			else:
				dateList = []
				isValid = False
				#firstDate = date.today().replace(day=1) - relativedelta(months=2)
				lastDate = date.today().replace(day=calendar.monthrange(date.today().year, date.today().month)[1])
				lastDatePreviousMonth = lastDate - relativedelta(months=2)
				#print('Today date', firstDate)
				#print('Last date', lastDate)
				#print('Last date previous month', lastDatePreviousMonth)				

				for i in range(4):
					tempLastDate = lastDatePreviousMonth - relativedelta(days=i)
					#tempLastDate = tempLastDate.strftime('%d-%m-%Y')
					dateList.append(tempLastDate.strftime('%d-%m-%Y'))

				for x in dateList:
					if tglKirim in x:
						isValid = True

				return isValid			

		if adapter.get('tanggalKirim') and adapter.get('jenisLayanan'):			
			if (getStatusRekon(item['tanggalKirim'], item['periodeRekon'], item['tahunRekon']) == True):
				if (('KILAT KHUSUS' in item['jenisLayanan']) == True):
					item['statusRekon'] = 'VALID'
					item['ketRekon'] = 'VALID'
				else:
					item['statusRekon'] = 'INVALID'
					item['ketRekon'] = 'DI LUAR PRODUK'
			else:
				item['statusRekon'] = 'INVALID'
				item['ketRekon'] = 'DI LUAR PERIODE'
		else:
			item['statusRekon'] = 'INVALID'
			item['ketRekon'] = 'DI LUAR PRODUK'

		return item