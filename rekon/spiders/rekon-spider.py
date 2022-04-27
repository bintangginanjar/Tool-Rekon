import scrapy
import urllib
import pandas as pd
import json
import re

from scrapy.loader import ItemLoader
from rekon.items import RekonItem
from bs4 import BeautifulSoup
from scrapy.http import FormRequest


class RekonSpider(scrapy.Spider):
    name = 'rekon'
    #filePath = '../Tokopedia'
    #filePath = '../Bukalapak'
    #filePath = '../Shopee'
    filePath = '../Lion Parcel'

    #fileName = '/data_rekon_toped_maret_failed_2022.csv'
    #fileName = '/data_rekon_bl_maret_2022.csv'
    fileName = '/data_rekon_lp_jan_okt_ext_230221.csv'

    targetFile = filePath + fileName
    periodeRekon = '01'
    tahunRekon = '2022'
    pidPatt = 'P2[0-9]'

    BASE_URL_PID = 'https://pid.posindonesia.co.id/lacak/admin/'
    BASE_URL_KENDALI = 'https://kendali-ipos.posindonesia.co.id/assets/modules/main-login-dashboard/modul/lacak-kiriman-403/model.php'

    custom_settings = {
        'FEED_EXPORT_FIELDS': [
            'awbRekon',
            'awbKendali',
            'mpCode',
            'jenisLayanan',
            'tanggalKirim',
            'isiKiriman',
            'berat',
            'jenisKiriman',
            'beaDasar',
            'nilaiBarang',
            'htnb',
            'beaTotal',
            'pengirim',
            'kotaPengirim',
            'kodePosPengirim',
            'penerima',
            'kotaPenerima',
            'kodePosPenerima',
            'kantorKirim',
            'nopendKantorKirim',
            'tanggalPosting',
            'statusAkhir',
            'ketStatusAkhir',
            'tanggalStatusAkhir',
            'kantorAkhir',
            'nopendKantorAkhir',
            'statusAntar',
            'waktuUpdateStatus',
            'ketStatusAntar',
            'penerimaKiriman',
            'statusRekon',
            'ketRekon'
        ],
        'ITEM_PIPELINES': {
            'rekon.pipelines.DataCleanPipeline': 100,
            'rekon.pipelines.DataParsePipeline': 200,
            #'rekon.pipelines.RekonStatusTopedPipeline': 300,
            'rekon.pipelines.RekonStatusPipeline': 300,
        }
    }

    rekonItem = RekonItem()

    def start_requests(self):
        self.logger.info('Start request Toped')

        def getMpCode(text):
            x = text.split('_')

            return x[2]

        dfAwb = pd.read_csv(self.targetFile)
        i = 1

        for index, row in dfAwb.iterrows():
            awb = row['awb']
            #awb = row['AWB Number']

            #if re.search(self.pidPatt, awb):
            if re.search(self.pidPatt, awb.decode('utf-8')):
            #if self.pidPatt in awb:
                self.logger.info('PID')

                params = {
                    'vBarcode': awb,
                }

                targetUrl = self.BASE_URL_PID + 'lacak_item_banyak.php'

                prodResponse = FormRequest(
                    targetUrl,
                    formdata=params,
                    callback=self.parsePid,
                    meta={
                        'awbRekon': awb,
                        'mpCode': getMpCode(self.fileName),
                        'periodeRekon': self.periodeRekon,
                        'tahunRekon': self.tahunRekon,
                        'source': 'pid'}
                )

                yield prodResponse

            else:
                self.logger.info('Kendali')

                params = {
                    'q': 'UxSR3i~ooj74Ifff5fpj4Nv9VZp4BFArp24qtUkyR7k=',
                    'folder_modul': 'modul',
                    'str1': awb,
                    'str2': 'detailContent_1',
                    'str3': '5'
                }

                targetUrl = f'{self.BASE_URL_KENDALI}/?{urllib.parse.urlencode(params)}'

                prodResponse = scrapy.Request(
                    targetUrl,
                    callback=self.parseKendali,
                    meta={
                        'awbRekon': awb,
                        'mpCode': getMpCode(self.fileName),
                        'periodeRekon': self.periodeRekon,
                        'tahunRekon': self.tahunRekon,
                        'source': 'kendali'
                    }
                )
                prodResponse.meta['dont_cache'] = True
                yield prodResponse

            logMessage = 'Data number ' + str(i)
            self.logger.info(logMessage)
            i = i + 1

    def parseKendali(self, response):
        self.logger.info('Parse Toped')

        loader = ItemLoader(item=RekonItem(), response=response)
        loader.add_value('awbRekon', response.meta['awbRekon'])
        loader.add_value('mpCode', response.meta['mpCode'])
        loader.add_value('periodeRekon', response.meta['periodeRekon'])
        loader.add_value('tahunRekon', response.meta['tahunRekon'])
        loader.add_value('source', response.meta['source'])
        loader.add_xpath(
            'awbKendali', '//tr[contains(@bgcolor, "white")][2]/td[2]/text()')
        loader.add_xpath(
            'jenisLayanan', '//tr[contains(@bgcolor, "white")][3]/td[2]/text()')
        loader.add_xpath(
            'tanggalKirim', '//tr[contains(@bgcolor, "white")][4]/td[2]/text()')
        loader.add_xpath(
            'isiKiriman', '//tr[contains(@bgcolor, "white")][5]/td[2]/text()')
        loader.add_xpath(
            'berat', '//tr[contains(@bgcolor, "white")][6]/td[2]/text()')
        loader.add_xpath(
            'jenisKiriman', '//tr[contains(@bgcolor, "white")][7]/td[2]/text()')
        loader.add_xpath(
            'beaDasar', '//tr[contains(@bgcolor, "white")][8]/td[2]/text()')
        loader.add_xpath(
            'nilaiBarang', '//tr[contains(@bgcolor, "white")][9]/td[2]/text()')
        loader.add_xpath(
            'htnb', '//tr[contains(@bgcolor, "white")][10]/td[2]/text()')
        loader.add_xpath(
            'pengirim', '//tr[contains(@bgcolor, "white")][11]/td[2]/text()')
        loader.add_xpath(
            'kotaPengirim', '//tr[contains(@bgcolor, "white")][11]/td[2]/text()')
        loader.add_xpath('kodePosPengirim',
                         '//tr[contains(@bgcolor, "white")][11]/td[2]/text()')
        loader.add_xpath(
            'penerima', '//tr[contains(@bgcolor, "white")][12]/td[2]/text()')
        loader.add_xpath(
            'kotaPenerima', '//tr[contains(@bgcolor, "white")][12]/td[2]/text()')
        loader.add_xpath('kodePosPenerima',
                         '//tr[contains(@bgcolor, "white")][12]/td[2]/text()')
        loader.add_xpath('nopendKantorKirim', '//center//tr[1]//td[1]/text()')
        loader.add_xpath('kantorKirim', '//center//tr[1]//td[1]/text()')
        loader.add_xpath('tanggalPosting', '//center//tr[1]//td[3]/text()')
        loader.add_xpath('statusAkhir', '//center//tr[last()-1]//td[2]/text()')
        loader.add_xpath('kantorAkhir', '//center//tr[last()-1]//td[1]/text()')
        loader.add_xpath('ketStatusAkhir',
                         '//center//tr[last()-1]//td[4]/text()')
        loader.add_xpath('nopendKantorAkhir',
                         '//center//tr[last()-1]//td[1]/text()')
        loader.add_xpath('tanggalStatusAkhir',
                         '//center//tr[last()-1]//td[3]/text()')

        yield loader.load_item()

        pass

    def parsePid(self, response):
        self.logger.info('Parse PID')
        jsonResponse = json.loads(response.body)
        # self.logger.info(jsonResponse['desk_mess'])

        soup = BeautifulSoup(jsonResponse['desk_mess'], 'html.parser')
        awbList = soup.find_all('a')

        urlParam = awbList[0]['href']
        urlParam = urlParam.split('=')
        urlParam = urlParam[1]
        self.logger.info(urlParam)

        targetUrl = self.BASE_URL_PID + 'detail_lacak_banyak.php?id=' + urlParam
        self.logger.info(targetUrl)
        # self.logger.info(response.meta['awbRekon'])

        prodResponse = scrapy.Request(
            targetUrl,
            callback=self.parsePidDetail,
            meta={
                'awbRekon': response.meta['awbRekon'],
                'mpCode': response.meta['mpCode'],
                'periodeRekon': response.meta['periodeRekon'],
                'tahunRekon': response.meta['tahunRekon'],
                'source': response.meta['source']
            }
        )

        prodResponse.meta['dont_cache'] = True

        yield prodResponse

    def parsePidDetail(self, response):
        self.logger.info('Parse PID detail')
        # self.logger.info(response.xpath('//table[1]'))
        # /html/body/table[1]/tbody/tr[4]/td[2]/font

        loader = ItemLoader(item=RekonItem(), response=response)
        loader.add_value('awbRekon', response.meta['awbRekon'])
        loader.add_value('mpCode', response.meta['mpCode'])
        loader.add_value('periodeRekon', response.meta['periodeRekon'])
        loader.add_value('tahunRekon', response.meta['tahunRekon'])
        loader.add_value('source', response.meta['source'])
        loader.add_xpath(
            'awbKendali', '//table[contains(@class, "bg1")][1]/tr[2]/td[2]/font/text()')
        loader.add_xpath(
            'jenisLayanan', '//table[contains(@class, "bg1")][1]/tr[4]/td[2]/font/text()')
        loader.add_xpath(
            'tanggalKirim', '//table[contains(@class, "bg1")][1]/tr[6]/td[2]/font/text()')
        loader.add_xpath(
            'isiKiriman', '//table[contains(@class, "bg1")][1]/tr[7]/td[2]/font/text()')
        loader.add_xpath(
            'berat', '//table[contains(@class, "bg1")][1]/tr[8]/td[2]/font/text()')
        loader.add_xpath(
            'jenisKiriman', '//table[contains(@class, "bg1")][1]/tr[9]/td[2]/font/text()')
        loader.add_xpath(
            'beaDasar', '//table[contains(@class, "bg1")][1]/tr[10]/td[2]/font/text()')
        loader.add_xpath(
            'nilaiBarang', '//table[contains(@class, "bg1")][1]/tr[11]/td[2]/font/text()')
        loader.add_xpath(
            'htnb', '//table[contains(@class, "bg1")][1]/tr[12]/td[2]/font/text()')
        loader.add_xpath(
            'pengirim', '//table[contains(@class, "bg1")][1]/tr[13]/td[2]/font/text()')
        loader.add_xpath(
            'penerima', '//table[contains(@class, "bg1")][1]/tr[14]/td[2]/font/text()')
        loader.add_xpath(
            'kodePosPenerima', '//table[contains(@class, "bg1")][1]/tr[14]/td[2]/font/text()')
        loader.add_xpath(
            'statusAkhir', '//table[contains(@class, "bg1")][1]/tr[15]/td[2]/font/text()')
        loader.add_xpath(
            'tanggalStatusAkhir', '//table[contains(@class, "bg1")][1]/tr[15]/td[2]/font/text()')

        yield loader.load_item()

        pass
