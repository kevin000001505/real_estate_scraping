# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class HousingScrapyPipeline:
    def process_item(self, item, spider):
        if '年' in item['year']:
            item['year'] = int(item['year'].replace("年", ""))

        if '~' in item['public_equipment']:
            parts = item['public_equipment'].replace("%", "").split('~')
            item['public_equipment'] = (int(parts[0])+int(parts[1]))/2
        if '%' in item['cover_percentage']:
            item['cover_percentage'] = int(item['cover_percentage'].replace("%", ""))
        return item

    def store_in_db(self, item):
        # This is a placeholder function to illustrate storing in a database
        print(f"Storing in DB: {item}")