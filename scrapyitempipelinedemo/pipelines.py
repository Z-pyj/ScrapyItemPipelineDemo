# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from elasticsearch import Elasticsearch
import pymongo
from scrapyitempipelinedemo.items import MovieItem
from scrapy import Request
from scrapy.exceptions import DropItem
from scrapy.pipelines.images import ImagesPipeline


# 存储到mongodb
class MongoDBPipeline(object):

    # 将连接字符串\数据库名\集合名赋值为类属性
    @classmethod
    def from_crawler(cls, crawler):
        # mongodb://localhost:27017
        cls.connection_string = crawler.settings.get('MONGODB_CONNECTION_STRING')
        # 数据库名
        cls.database = crawler.settings.get('MONGODB_DATABASE')
        # 集合名
        cls.collection = crawler.settings.get('MONGODB_COLLECTION')
        return cls()

    # 连接数据库
    def open_spider(self, spider):
        # 利用类属性self.connection_string创建一个数据库连接
        self.client = pymongo.MongoClient(self.connection_string)
        # 声明一个数据库操作对象
        self.db = self.client[self.database]

    # 将Item存储到mongodb中
    def process_item(self, item, spider):
        # update_one():存在就更新,不存在就插入
        self.db[self.collection].update_one({
            'name': item['name']
        }, {
            '$set': dict(item)
        }, True)
        return item

    # spider运行结束后关闭数据库连接
    def close_spider(self, spider):
        self.client.close()


# 存储到Elasticsearch
class ElasticsearchPipeline(object):

    @classmethod
    def from_crawler(cls, crawler):
        # 数据库连接
        cls.connection_string = crawler.settings.get('ELASTICSEARCH_CONNECTION_STRING')
        # 索引
        cls.index = crawler.settings.get('ELASTICSEARCH_INDEX')
        return cls()

    def open_spider(self, spider):
        # 声明一个数据库操作对象
        self.conn = Elasticsearch([self.connection_string])
        # 判断索引是否存在,不存在就创建
        if not self.conn.indices.exists(self.index):
            self.conn.indices.create(index=self.index)

    def process_item(self, item, spider):
        # index:索引
        # body:代表数据对象,把item对象转为字典类型
        # id:索引数据的id,用item['name']的hash值
        self.conn.index(index=self.index, body=dict(item), id=hash(item['name']))
        return item

    # spider运行结束后关闭数据库连接
    def close_spider(self, spider):
        self.conn.transport.close()


class ImagePipeline(ImagesPipeline):
    def file_path(self, request, response=None, info=None):
        movie = request.meta['movie']
        type = request.meta['type']
        name = request.meta['name']
        file_name = f'{movie}/{type}/{name}.jpg'
        return file_name

    def item_completed(self, results, item, info):
        image_paths = [x['path'] for ok, x in results if ok]
        if not image_paths:
            raise DropItem('Image Downloaded Failed')
        return item

    def get_media_requests(self, item, info):
        for director in item['directors']:
            director_name = director['name']
            director_image = director['image']
            yield Request(director_image, meta={
                'name': director_name,
                'type': 'director',
                'movie': item['name']
            })

        for actor in item['actors']:
            actor_name = actor['name']
            actor_image = actor['image']
            yield Request(actor_image, meta={
                'name': actor_name,
                'type': 'actor',
                'movie': item['name']
            })
