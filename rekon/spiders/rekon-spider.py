import scrapy
import urllib
import pandas as pd

from scrapy.loader import ItemLoader
from rekon.items import RekonItem


class RekonSpider(scrapy.Spider):
    name = 'rekon'    
    filePath = '../Bukalapak'
    
    fileName = '/data_rekon_bl_juli_2021.csv'    

    targetFile = filePath + fileName
    periodeRekon = '07'
    tahunRekon = '2021'

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
            'rekon.pipelines.RekonStatusPipeline': 300,
        }
    }

    rekonItem = RekonItem()

    def start_requests(self):

        def getMpCode(text):
            x = text.split('_')

            return x[2]

        dfAwb = pd.read_csv(self.targetFile)
        i = 1

        for index, row in dfAwb.iterrows():
            awb = row['awb']

            BASE_URL = 'https://kendali-ipos.posindonesia.co.id/assets/modules/main-login-dashboard/modul/lacak-kiriman-403/model.php'

            params = {
                'q': 'UxSR3i~ooj74Ifff5fpj4Nv9VZp4BFArp24qtUkyR7k=',
                'folder_modul': 'modul',
                'str1': awb,
                'str2': 'detailContent_1',
                'str3': '5'
            }

            targetUrl = f'{BASE_URL}/?{urllib.parse.urlencode(params)}'

            logMessage = 'Data number ' + str(i)
            self.logger.info(logMessage)
            prodResponse = scrapy.Request(targetUrl, callback=self.parse, meta={'awbRekon': awb, 'mpCode': getMpCode(
                self.fileName), 'periodeRekon': self.periodeRekon, 'tahunRekon': self.tahunRekon})
            prodResponse.meta['dont_cache'] = True
            yield prodResponse
            i = i + 1

    def parse(self, response):

        loader = ItemLoader(item=RekonItem(), response=response)
        loader.add_value('awbRekon', response.meta['awbRekon'])
        loader.add_value('mpCode', response.meta['mpCode'])
        loader.add_value('periodeRekon', response.meta['periodeRekon'])
        loader.add_value('tahunRekon', response.meta['tahunRekon'])
        loader.add_xpath('awbKendali', '//tr[contains(@bgcolor, "white")][2]/td[2]/text()')
        loader.add_xpath('jenisLayanan', '//tr[contains(@bgcolor, "white")][3]/td[2]/text()')
        loader.add_xpath('tanggalKirim', '//tr[contains(@bgcolor, "white")][4]/td[2]/text()')
        loader.add_xpath('isiKiriman', '//tr[contains(@bgcolor, "white")][5]/td[2]/text()')
        loader.add_xpath('berat', '//tr[contains(@bgcolor, "white")][6]/td[2]/text()')
        loader.add_xpath('jenisKiriman', '//tr[contains(@bgcolor, "white")][7]/td[2]/text()')
        loader.add_xpath('beaDasar', '//tr[contains(@bgcolor, "white")][8]/td[2]/text()')
        loader.add_xpath('nilaiBarang', '//tr[contains(@bgcolor, "white")][9]/td[2]/text()')
        loader.add_xpath('htnb', '//tr[contains(@bgcolor, "white")][10]/td[2]/text()')
        loader.add_xpath('pengirim', '//tr[contains(@bgcolor, "white")][11]/td[2]/text()')
        loader.add_xpath('kotaPengirim', '//tr[contains(@bgcolor, "white")][11]/td[2]/text()')
        loader.add_xpath('kodePosPengirim', '//tr[contains(@bgcolor, "white")][11]/td[2]/text()')
        loader.add_xpath('penerima', '//tr[contains(@bgcolor, "white")][12]/td[2]/text()')
        loader.add_xpath('kotaPenerima', '//tr[contains(@bgcolor, "white")][12]/td[2]/text()')
        loader.add_xpath('kodePosPenerima', '//tr[contains(@bgcolor, "white")][12]/td[2]/text()')
        loader.add_xpath('nopendKantorKirim', '//center//tr[1]//td[1]/text()')
        loader.add_xpath('kantorKirim', '//center//tr[1]//td[1]/text()')
        loader.add_xpath('tanggalPosting', '//center//tr[1]//td[3]/text()')
        loader.add_xpath('statusAkhir', '//center//tr[last()-1]//td[2]/text()')
        loader.add_xpath('kantorAkhir', '//center//tr[last()-1]//td[1]/text()')
        loader.add_xpath('ketStatusAkhir', '//center//tr[last()-1]//td[4]/text()')
        loader.add_xpath('nopendKantorAkhir', '//center//tr[last()-1]//td[1]/text()')
        loader.add_xpath('tanggalStatusAkhir', '//center//tr[last()-1]//td[3]/text()')

        yield loader.load_item()

        pass
