# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os

import pymongo
from scrapy.exceptions import DropItem


class TextPipeline(object):
    def __init__(self):
        self.limit = 50

    def process_item(self, item, spider):
        if item['text']:
            if len(item['text']) > self.limit:
                item['text'] = item['text'][:self.limit].strip() + '...'
            return item
        else:
            return DropItem('Missing Text')


class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )

    def open_spider(self, spider):
        """
        在爬虫启动的时候执行 open_spider 方法
        创建 mongodb 连接客户端
        """
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        col_name = item.__class__.__name__
        self.db[col_name].insert(dict(item))
        return item

    def close_spider(self, spider):
        """
        爬虫结束的时候执行 close_spider 方法
        关闭 mongodb 连接客户端
        """
        self.client.close()


class CrawlabMongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )

    def open_spider(self, spider):
        """
        在爬虫启动的时候执行 open_spider 方法
        创建 mongodb 连接客户端
        """
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        item['task_id'] = os.environ.get('CRAWLAB_TASK_ID')  # 传入 task_id
        col_name = os.environ.get('CRAWLAB_COLLECTION')  # 使用环境变量的 collection name
        if not col_name:  # 环境变量没有设定 collection name，就以 item 类名为 collection name
            col_name = item.__class__.__name__
        self.db[col_name].insert(dict(item))
        return item

    def close_spider(self, spider):
        """
        爬虫结束的时候执行 close_spider 方法
        关闭 mongodb 连接客户端
        """
        self.client.close()
