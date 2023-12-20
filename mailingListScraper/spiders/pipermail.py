from bs4 import BeautifulSoup
from mailingListScraper.items import Email, RawEmlMessage
from mailingListScraper.spiders.ArchiveSpider import ArchiveSpider
from requests import Response
import re
import scrapy
from scrapy.loader import ItemLoader


class PipermailSpider(ArchiveSpider):
    name = "pipermail"
    mailing_lists = {
        'exi-az': 'http://lists.extropy.org/pipermail/exi-az',
        'exi-bay-announce': 'http://lists.extropy.org/pipermail/exi-bay-announce',
        'exi-bay': 'http://lists.extropy.org/pipermail/exi-bay',
        'exi-bay-chat': 'http://lists.extropy.org/pipermail/exi-bay-chat',
        'exi-can': 'http://lists.extropy.org/pipermail/exi-can',
        'exi-east': 'http://lists.extropy.org/pipermail/exi-east',
        # 'extropy-chat': 'http://lists.extropy.org/pipermail/extropy-chat',
        'exi-euro': 'http://lists.extropy.org/pipermail/exi-euro',
        'exi-la': 'http://lists.extropy.org/pipermail/exi-la',
        'exi-midwest': 'http://lists.extropy.org/pipermail/exi-midwest',
        'exi-russia': 'http://lists.extropy.org/pipermail/exi-russia',
        'exi-tex': 'http://lists.extropy.org/pipermail/exi-tex',
    }
    default_list = 'exi-bay-chat'
    
    custom_settings = {
        'ITEM_PIPELINES': {
            'mailingListScraper.pipelines.EmlExport': 500,
        }
    }

    # TODO write middleware to deal with webarchive urls
    def parse(self, response):
        """
        Extract all of the messages lists (grouped by year-months).

        @url http://lkml.iu.edu/pipermail/linux/kernel/
        @returns requests 1002
        """
        
        body = response.text
        text_archive_pattern = r'\d{4}-[a-zA-Z]+.txt'
        matches = re.findall(text_archive_pattern, body)
        use_text_archives = True
        
        if len(matches) == 0:
            pattern = r'\d{4}-[a-zA-Z]+'
            matches = re.findall(pattern, body)
            use_text_archives = False
            # note that the match does not have ".html" at the end
        
        message_list_urls = []
        for match in matches:
            full_url = f"{response.url}/{match}"
            if full_url not in message_list_urls:
                message_list_urls.append(full_url)

        if any(self.years):
            urls = []
            for year in self.years:
                pattern = response.url + year[2:]
                urls.extend([u for u in message_list_urls if pattern in u])

            message_list_urls = urls

        for url in message_list_urls:
            if use_text_archives:
                yield scrapy.Request(url, callback=self.parse_raw_message_list)
            else:
                yield scrapy.Request(url, callback=self.parse_message_list)

    def parse_message_list(self, response):
        """
        Extract all relative URLs to the individual messages.

        @url http://lkml.iu.edu/pipermail/linux/kernel/9506/index.html
        @returns requests 199
        """
        body = response.text
        pattern = '\"(\d+\.html)\"'
        matches = re.findall(pattern, body)

        urls = set()
        for match in matches:
            urls.add(response.url + '/' + match)

        # logger.info(f'Found {len(urls)} message urls for month at {response.url.split("/")[-1]}')
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_item)
        return list(urls)
    
    def parse_item(self, response):
        response_body = response.text
        dom = BeautifulSoup(response_body, 'html.parser')
        
        load = ItemLoader(item=Email(), selector=response)
        
        
        specific_email_fields = {
            'senderName': dom.select('html body b').pop(0).text.strip(),
            'senderEmail': dom.select('html body a').pop(0).text.strip().replace(' at ', '@'),
            'timeSent': (dom.select('html body i').pop(0).text.strip()),
            'subject': dom.select('html body h1').pop(0).text.strip(),
            'replyto': dom.select('html body a').pop(0).get('href') or '',
            'body': dom.select('html body p pre').pop(0).text.strip(),
            'url': response.url,
        }
        # Load all the values from these specific fields
        for field_name, field_value in specific_email_fields.items():
            load.add_value(field_name, field_value)
        
        return load.load_item()
    
    def parse_raw_message_list(self, response):
        email_pattern = re.compile(r'From .*? \w+ \w+ \d+ \d+:\d+:\d+ \d{4}\n')
        emails = re.split(email_pattern, response.text)
        for i, email in enumerate(emails):
            apart = email.splitlines()
            if i > 1:
                apart = apart[1:]
            if len(apart) > 0:
                apart[0] = apart[0].replace(' at ', '@')
                emails[i] = '\n'.join(apart)
            # could combine with the below line
        for email in emails:
            load = ItemLoader(item=RawEmlMessage())
            load.add_value('raw_message',email)
            yield load.load_item()