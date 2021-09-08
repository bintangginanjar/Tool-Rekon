import scrapy
import urllib
import pandas as pd
import json

from scrapy.loader import ItemLoader
from scrapy.http import FormRequest
from bs4 import BeautifulSoup
from rekon.items import RekonItem


class PidSpider(scrapy.Spider):
    name = 'pid'
    filePath = '../Tokopedia'

    BASE_URL = 'https://pid.posindonesia.co.id/lacak/admin/'

    fileName = '/data_rekon_toped_juli_2021_pid.csv'

    targetFile = filePath + fileName
    periodeRekon = '07'
    tahunRekon = '2021'

    def start_requests(self):
        self.logger.info('Start request PID')

        def getMpCode(text):
            x = text.split('_')

            return x[2]

        dfAwb = pd.read_csv(self.targetFile)
        i = 1

        for index, row in dfAwb.iterrows():
            awb = row['awb']

            params = {
                'vBarcode': awb,
            }

            logMessage = 'Data number ' + str(i)
            self.logger.info(logMessage)
            targetUrl = self.BASE_URL + 'lacak_item_banyak.php'
            #prodResponse = scrapy.Request(BASE_URL, callback=self.parse, method="POST", body=json.dumps(params))
            prodResponse = FormRequest(targetUrl, formdata=params, callback=self.parse, meta={'awbRekon': awb, 'mpCode': getMpCode(
                self.fileName), 'periodeRekon': self.periodeRekon, 'tahunRekon': self.tahunRekon})
            # prodResponse = FormRequest(BASE_URL, callback=self.parse, meta={'awbRekon': awb, 'mpCode': getMpCode(
            #    self.fileName), 'periodeRekon': self.periodeRekon, 'tahunRekon': self.tahunRekon})

            yield prodResponse
            i = i + 1

            #targetUrl = f'{BASE_URL}/?{urllib.parse.urlencode(params)}'

            '''
            
            prodResponse = scrapy.Request(targetUrl, callback=self.parse, meta={'awbRekon': awb, 'mpCode': getMpCode(
                self.fileName), 'periodeRekon': self.periodeRekon, 'tahunRekon': self.tahunRekon})
            prodResponse.meta['dont_cache'] = True
            yield prodResponse
            i = i + 1
            '''

    def parse(self, response):
        self.logger.info('Parse Toped')
        jsonResponse = json.loads(response.body)
        # self.logger.info(jsonResponse['desk_mess'])

        soup = BeautifulSoup(jsonResponse['desk_mess'], 'html.parser')
        awbList = soup.find_all('a')

        urlParam = awbList[0]['href']
        urlParam = urlParam.split('=')
        urlParam = urlParam[1]
        self.logger.info(urlParam)

        targetUrl = self.BASE_URL + 'detail_lacak_banyak.php?id=' + urlParam
        self.logger.info(targetUrl)

        prodResponse = scrapy.Request(targetUrl, callback=self.parseDetail)
        prodResponse.meta['dont_cache'] = True

        yield prodResponse

        # self.logger.info(awbList[0]['href'])
        # self.logger.info(soup.prettify())
        # self.logger.info(jsonResponse['desk_mess'])

    def parseDetail(self, response):
        self.logger.info('Parse detail')
        # self.logger.info(response.xpath('//table[1]'))
        # /html/body/table[1]/tbody/tr[4]/td[2]/font

        loader = ItemLoader(item=RekonItem(), response=response)
        loader.add_xpath(
            'awbKendali', '//table[contains(@class, "bg1")][1]/tr[2]/td[2]/font/text()')
        loader.add_xpath(
            'jenisLayanan', '//table[contains(@class, "bg1")][1]/tr[4]/td[2]/font/text()')
        loader.add_xpath(
            'tanggalKirim', '//table[contains(@class, "bg1")][1]/tr[5]/td[2]/font/text()')
        loader.add_xpath(
            'isiKiriman', '//table[contains(@class, "bg1")][1]/tr[6]/td[2]/font/text()')
        loader.add_xpath(
            'berat', '//table[contains(@class, "bg1")][1]/tr[7]/td[2]/font/text()')
        loader.add_xpath(
            'jenisKiriman', '//table[contains(@class, "bg1")][1]/tr[8]/td[2]/font/text()')
        loader.add_xpath(
            'beaDasar', '//table[contains(@class, "bg1")][1]/tr[9]/td[2]/font/text()')
        loader.add_xpath(
            'nilaiBarang', '//table[contains(@class, "bg1")][1]/tr[10]/td[2]/font/text()')
        loader.add_xpath(
            'htnb', '//table[contains(@class, "bg1")][1]/tr[11]/td[2]/font/text()')
        loader.add_xpath(
            'pengirim', '//table[contains(@class, "bg1")][1]/tr[12]/td[2]/font/text()')
        loader.add_xpath(
            'penerima', '//table[contains(@class, "bg1")][1]/tr[13]/td[2]/font/text()')
        loader.add_xpath(
            'statusAkhir', '//table[contains(@class, "bg1")][1]/tr[14]/td[2]/font/text()')

        yield loader.load_item()

        pass
