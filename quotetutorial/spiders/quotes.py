# -*- coding: utf-8 -*-
import scrapy

from quotetutorial.items import QuoteItem


class QuotesSpider(scrapy.Spider):
    name = 'quotes'
    allowed_domains = ['quotes.toscrape.com']
    start_urls = ['http://quotes.toscrape.com/']

    def parse(self, response):
        quotes = response.css('.quote')
        for quote in quotes:
            text = quote.css('.text::text').extract_first()
            author = quote.css('.author::text').extract_first()
            tags = quote.css('.tags .tag::text').extract()

            item = QuoteItem()
            # item['text'] = text
            # item['author'] = author
            # item['tags'] = tags
            for field in item.fields:
                try:
                    item[field] = eval(field)
                except NameError:
                    self.logger.debug('Field is not Defined' + field)
            yield item

        next_page = response.css('.next a::attr(href)').extract_first()
        url = response.urljoin(next_page)
        yield scrapy.Request(url=url, callback=self.parse)
