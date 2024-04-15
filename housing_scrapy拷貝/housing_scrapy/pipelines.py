# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class HousingScrapyPipeline:
    def process_item(self, item, spider):
        return item

    def store_in_db(self, item):
        # This is a placeholder function to illustrate storing in a database
        print(f"Storing in DB: {item}")