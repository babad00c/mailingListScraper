# -*- coding: utf-8 -*-
# pylint: disable=too-many-ancestors,unnecessary-pass
"""
Items and ItemLoaders
"""
import scrapy
from itemloaders.processors import Join, MapCompose
from w3lib.html import replace_entities


class RawEmlMessage(scrapy.Item):
    raw_message = scrapy.Field(output_processor=Join())
    id = scrapy.Field()

class Email(scrapy.Item):
    "The main item to store our emails"

    emailId = scrapy.Field()

    senderName = scrapy.Field(
        input_processor=MapCompose(replace_entities),
        output_processor=Join()
        )
    senderEmail = scrapy.Field(
        input_processor=MapCompose(replace_entities),
        output_processor=Join()
        )

    timeSent = scrapy.Field(
        input_processor=MapCompose(replace_entities),
        output_processor=Join()
        )
    timeReceived = scrapy.Field(
        input_processor=MapCompose(replace_entities),
        output_processor=Join()
        )
    timestampSent = scrapy.Field()
    timestampReceived = scrapy.Field()

    body = scrapy.Field(
        input_processor=MapCompose(replace_entities),
        output_processor=Join(),
        )

    url = scrapy.Field(output_processor=Join())
    replyto = scrapy.Field(output_processor=Join())
    subject = scrapy.Field(output_processor=Join())
    mailingList = scrapy.Field(output_processor=Join())

    pass
