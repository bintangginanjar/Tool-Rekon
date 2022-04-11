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
    def removeGr(self, text):
        text = text.replace('.', '').replace('Gr', '').replace(' ', '')

        return text

    def removePoint(self, text):
        text = text.replace('.', '')

        return text

    def dataCleanKendali(self, item, spider):
        adapter = ItemAdapter(item)

        item['mpCode'] = item['mpCode'][0]
        item['periodeRekon'] = item['periodeRekon'][0]

        if adapter.get('berat'):
            item['berat'] = self.removeGr(item['berat'][0])
        else:
            item['berat'] = 0

        if adapter.get('beaDasar'):
            item['beaDasar'] = self.removePoint(item['beaDasar'][0])
        else:
            item['beaDasar'] = 0

        if adapter.get('nilaiBarang'):
            item['nilaiBarang'] = self.removePoint(item['nilaiBarang'][0])
        else:
            item['nilaiBarang'] = 0

        if not adapter.get('isiKiriman'):
            item['isiKiriman'] = ''
        else:
            item['isiKiriman'] = item['isiKiriman'][0]

        if not adapter.get('htnb'):
            item['htnb'] = ''
        else:
            item['htnb'] = self.removeGr(item['htnb'][0])

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

    def dataCleanPid(self, item, spider):
        adapter = ItemAdapter(item)

        item['mpCode'] = item['mpCode'][0]
        item['periodeRekon'] = item['periodeRekon'][0]

        if adapter.get('beaDasar'):
            item['beaDasar'] = self.removePoint(item['beaDasar'][0])
        else:
            item['beaDasar'] = 0

        if adapter.get('nilaiBarang'):
            item['nilaiBarang'] = self.removePoint(item['nilaiBarang'][0])
        else:
            item['nilaiBarang'] = 0

        if not adapter.get('isiKiriman'):
            item['isiKiriman'] = ''
        else:
            item['isiKiriman'] = item['isiKiriman'][0]

        if not adapter.get('htnb'):
            item['htnb'] = ''
        else:
            item['htnb'] = self.removeGr(item['htnb'][0])

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

        if not adapter.get('jenisKiriman'):
            item['jenisKiriman'] = ''
        else:
            item['jenisKiriman'] = item['jenisKiriman'][0]

        item['tahunRekon'] = item['tahunRekon'][0]

        return item

    def __init__(self):
        logging.info("****Data cleaning****")

    def process_item(self, item, spider):
        if (item['source'][0] == 'kendali'):
            return self.dataCleanKendali(item, spider)

        if (item['source'][0] == 'pid'):
            return self.dataCleanPid(item, spider)


class DataParsePipeline(object):
    def getPengirimPenerima(self, text):
        x = text.split(';')

        return x[0]

    def getKodePos(self, text, source):
        if (source == 'kendali'):
            kodepos = re.findall(';[0-9][0-9][0-9][0-9][0-9];', text)
            if (len(kodepos) > 0):
                kodepos = kodepos[0].replace(';', '')
            else:
                kodepos = ''

            return kodepos
        else:
            kodepos = text.split(';')
            if (len(kodepos) < 4):
                kodepos = ''
            else:
                kodepos = kodepos[3].replace(' ', '')

            return kodepos

    def getNopendKantor(self, text):
        text = re.findall('^[0-9A-Z]*', text)

        return text[0]

    def getKotaPengirimPenerima(self, text):
        text = text.split(';')

        return text[3]

    def getNamaKantor(self, text):
        text = re.split('^[0-9A-Z]*', text)[1].lstrip()

        return text

    def removeColon(self, text):
        x = text.split(':')

        return x[1].lstrip()

    def getTanggalKirim(self, text):
        text = text.split(' ')
        text = text[0].split('-')

        tglKirim = text[2] + '-' + text[1] + '-' + text[0]

        return tglKirim

    def getStatusAkhir(self, text):
        text = text.split(' ')

        return text[0]

    def getTanggalStatusAkhir(self, text):
        tanggal = re.findall(
            '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][ ][0-9][0-9]:[0-9][0-9]:[0-9][0-9]', text)

        return tanggal

    def getBerat(self, text):
        berat = re.findall('[0-9.] Kg', text)
        berat = berat[0].replace(' Kg', '')
        berat = round(float(berat) * 1000)

        return berat

    def dataParseKendali(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('pengirim'):
            item['pengirim'] = self.getPengirimPenerima(item['pengirim'][0])
        else:
            item['pengirim'] = ''

        if adapter.get('penerima'):
            item['penerima'] = self.getPengirimPenerima(item['penerima'][0])
        else:
            item['penerima'] = ''

        if adapter.get('kodePosPenerima'):
            item['kodePosPenerima'] = self.getKodePos(
                item['kodePosPenerima'][0], 'kendali')
        else:
            item['kodePosPenerima'] = ''

        if adapter.get('kodePosPengirim'):
            item['kodePosPengirim'] = self.getKodePos(
                item['kodePosPengirim'][0], 'kendali')
        else:
            item['kodePosPengirim'] = ''

        if adapter.get('nopendKantorKirim'):
            item['nopendKantorKirim'] = self.getNopendKantor(
                item['nopendKantorKirim'][0])
        else:
            item['nopendKantorKirim'] = ''

        if adapter.get('nopendKantorAkhir'):
            item['nopendKantorAkhir'] = self.getNopendKantor(
                item['nopendKantorAkhir'][0])
        else:
            item['nopendKantorAkhir'] = ''

        if adapter.get('kotaPengirim'):
            item['kotaPengirim'] = self.getKotaPengirimPenerima(
                item['kotaPengirim'][0])
        else:
            item['kotaPengirim'] = ''

        if adapter.get('kotaPenerima'):
            item['kotaPenerima'] = self.getKotaPengirimPenerima(
                item['kotaPenerima'][0])
        else:
            item['kotaPenerima'] = ''

        if adapter.get('kantorKirim'):
            item['kantorKirim'] = self.getNamaKantor(item['kantorKirim'][0])
        else:
            item['kantorKirim'] = ''

        if adapter.get('kantorAkhir'):
            item['kantorAkhir'] = self.getNamaKantor(item['kantorAkhir'][0])
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
                        item['waktuUpdateStatus'] = self.removeColon(
                            item['ketStatusAkhir'][1])

                    if ('STATUS' in item['ketStatusAkhir'][2]):
                        item['ketStatusAntar'] = self.removeColon(
                            item['ketStatusAkhir'][2])

                    if ('PENERIMA' in item['ketStatusAkhir'][3]):
                        item['penerimaKiriman'] = self.removeColon(
                            item['ketStatusAkhir'][3])
                except IndexError:
                    item['waktuUpdateStatus'] = ''
                    item['ketStatusAntar'] = ''
                    item['penerimaKiriman'] = ''
                    # raise e
        else:
            item['statusAkhir'] = ''
            item['statusAntar'] = ''
            item['ketStatusAntar'] = ''
            item['penerimaKiriman'] = ''

        return item

    def dataParsePid(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('berat'):
            item['berat'] = self.getBerat(item['berat'][0])
        else:
            item['berat'] = 0

        if adapter.get('pengirim'):
            item['pengirim'] = self.getPengirimPenerima(item['pengirim'][0])
        else:
            item['pengirim'] = ''

        if adapter.get('penerima'):
            item['penerima'] = self.getPengirimPenerima(item['penerima'][0])
        else:
            item['penerima'] = ''

        if adapter.get('kodePosPenerima'):
            item['kodePosPenerima'] = self.getKodePos(
                item['kodePosPenerima'][0], 'pid')
        else:
            item['kodePosPenerima'] = ''

        if not adapter.get('tanggalKirim'):
            item['tanggalKirim'] = ''
        else:
            item['tanggalKirim'] = self.getTanggalKirim(
                item['tanggalKirim'][0])

        # sum up beaDasar & htnb
        if adapter.get('beaDasar'):
            if adapter.get('htnb'):
                item['beaTotal'] = int(item['beaDasar']) + int(item['htnb'])
            else:
                item['beaTotal'] = int(item['beaDasar'])
        else:
            item['beaTotal'] = 0

        if not adapter.get('statusAkhir'):
            item['statusAkhir'] = ''
        else:
            item['statusAkhir'] = self.getStatusAkhir(item['statusAkhir'][0])

        if not adapter.get('tanggalStatusAkhir'):
            item['tanggalStatusAkhir'] = ''
        else:
            item['tanggalStatusAkhir'] = self.getTanggalStatusAkhir(
                item['tanggalStatusAkhir'][0])

        return item

    def __init__(self):
        logging.info("****Data parsing****")

    def process_item(self, item, spider):
        if (item['source'][0] == 'kendali'):
            return self.dataParseKendali(item, spider)

        if (item['source'][0] == 'pid'):
            return self.dataParsePid(item, spider)


class RekonStatusPipeline(object):

    # for toped only
    def isTanggalToped(self, tglKirim, periodeRekon, tahunRekon):
        x = tglKirim.split('-')

        # if (x[1] == periodeRekon):
        if (x[1] == periodeRekon) & (x[2] == tahunRekon):
            return True
        else:
            dateList = []
            isValid = False
            # firstDate = date.today().replace(day=1) - relativedelta(months=2)
            lastDate = date.today().replace(day=calendar.monthrange(
                date.today().year, date.today().month)[1])
            lastDatePreviousMonth = lastDate - relativedelta(months=2)
            # print('Today date', firstDate)
            # print('Last date', lastDate)
            # print('Last date previous month', lastDatePreviousMonth)

            for i in range(4):
                tempLastDate = lastDatePreviousMonth - \
                    relativedelta(days=i-1)
                # tempLastDate = tempLastDate.strftime('%d-%m-%Y')
                # print('tempLastDate', tempLastDate.strftime('%d-%m-%Y'))
                dateList.append(tempLastDate.strftime('%d-%m-%Y'))

            for x in dateList:
                if tglKirim in x:
                    isValid = True

            return isValid

    # for non toped
    def getStatusRekon(self, tglKirim, periodeRekon, tahunRekon):
        # periodeRekon = '12'
        x = tglKirim.split('-')

        # if (x[1] == periodeRekon):
        if (x[1] == periodeRekon) & (x[2] == tahunRekon):
            return True
        else:
            return False

    def rekonStatusToped(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('tanggalKirim') and adapter.get('jenisLayanan'):
            if (self.isTanggalToped(item['tanggalKirim'], item['periodeRekon'], item['tahunRekon']) == True):
                if (('KILAT KHUSUS' in item['jenisLayanan']) == True) | (('PKH' in item['jenisLayanan']) == True):
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

    def rekonStatusMp(self, item, spider):
        adapter = ItemAdapter(item)

        # check package validity
        if adapter.get('tanggalKirim') and adapter.get('jenisLayanan'):
            if (self.getStatusRekon(item['tanggalKirim'], item['periodeRekon'], item['tahunRekon']) == True):
                if (('KILAT KHUSUS' in item['jenisLayanan']) == True) | (('Express' in item['jenisLayanan']) == True) | (('PKH' in item['jenisLayanan']) == True) | (('PE' in item['jenisLayanan']) == True):
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

    def rekonStatusPid(self, item, spider):
        return item

    def __init__(self):
        logging.info("****Rekon status****")

    def process_item(self, item, spider):
        if (item['mpCode'] == 'toped'):
            return self.rekonStatusToped(item, spider)
        else:
            return self.rekonStatusMp(item, spider)


class RekonStatusTopedPipeline(object):
    def getStatusTanggal(self, tglKirim, periodeRekon, tahunRekon):
        x = tglKirim.split('-')

        # if (x[1] == periodeRekon):
        if (x[1] == periodeRekon) & (x[2] == tahunRekon):
            return True
        else:
            dateList = []
            isValid = False
            # firstDate = date.today().replace(day=1) - relativedelta(months=2)
            lastDate = date.today().replace(day=calendar.monthrange(
                date.today().year, date.today().month)[1])
            lastDatePreviousMonth = lastDate - relativedelta(months=2)
            # print('Today date', firstDate)
            # print('Last date', lastDate)
            # print('Last date previous month', lastDatePreviousMonth)

            for i in range(4):
                tempLastDate = lastDatePreviousMonth - \
                    relativedelta(days=i-1)
                # tempLastDate = tempLastDate.strftime('%d-%m-%Y')
                # print('tempLastDate', tempLastDate.strftime('%d-%m-%Y'))
                dateList.append(tempLastDate.strftime('%d-%m-%Y'))

            for x in dateList:
                if tglKirim in x:
                    isValid = True

            return isValid

    def __init__(self):
        logging.info("****Rekon status Tokopedia****")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get('tanggalKirim') and adapter.get('jenisLayanan'):
            if (self.getStatusTanggal(item['tanggalKirim'], item['periodeRekon'], item['tahunRekon']) == True):
                if (('KILAT KHUSUS' in item['jenisLayanan']) == True) | (('PKH' in item['jenisLayanan']) == True):
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

        '''
        if (item['source'][0] == 'kendali'):
            return self.rekonStatusTopedKendali(item, spider)

        if (item['source'][0] == 'pid'):
            return self.rekonStatusTopedPid(item, spider)
        '''
