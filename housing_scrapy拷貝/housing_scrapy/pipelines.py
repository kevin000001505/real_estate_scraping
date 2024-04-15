# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector

class HousingScrapyPipeline:
    def open_spider(self, spider):
        self.connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='@America155088',
            database='real_estate_db'
        )
        self.cursor = self.connection.cursor()
    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()
    def process_item(self, item, spider):
        if '年' in item['year']:
            item['year'] = int(item['year'].replace("年", ""))
        if ',' in item['browse_num']:
            item['browse_num'] = int(item['browse_num'].replace(",", ""))
        if '~' in item['public_equipment']:
            parts = item['public_equipment'].replace("%", "").split('~')
            item['public_equipment'] = (int(parts[0])+int(parts[1]))/2
        if '%' in item['cover_percentage']:
            item['cover_percentage'] = int(item['cover_percentage'].replace("%", ""))
        if '戶' in item['total_resident']:
            item['total_resident'] = int(item['total_resident'].replace("戶", ""))

        
        return item

    def store_in_db(self, item):
        
        print(f"Storing in DB: {item}")