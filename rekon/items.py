# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import re

from scrapy.item import Item, Field
from itemloaders.processors import MapCompose, TakeFirst, Identity
from datetime import datetime

class RekonItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    awbRekon = Field()
    mpCode = Field()
    periodeRekon = Field()
    tahunRekon = Field()
    awbKendali = Field()
    jenisLayanan = Field()
    tanggalKirim = Field()
    isiKiriman = Field()
    berat = Field()
    jenisKiriman = Field()
    beaDasar = Field()
    nilaiBarang = Field()
    htnb = Field()
    pengirim = Field()
    kotaPengirim = Field()
    kodePosPengirim = Field()
    penerima = Field()
    kotaPenerima = Field()
    kodePosPenerima = Field()
    statusRekon = Field()
    ketRekon = Field()
    beaTotal = Field()
    kantorKirim = Field()
    nopendKantorKirim = Field()
    kantorKirim = Field()
    tanggalPosting = Field()
    statusAkhir = Field()
    kantorAkhir = Field()
    ketStatusAkhir = Field()
    nopendKantorAkhir = Field()
    tanggalStatusAkhir = Field()
    statusAntar = Field()
    ketStatusAntar = Field()
    penerimaKiriman = Field()
    waktuUpdateStatus = Field()
    
    pass
