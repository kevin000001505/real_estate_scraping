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
        if '年' in item['year'] and item['year'] is not None:
            item['year'] = int(item['year'].replace("年", ""))

        if ',' in item['browse_num'] and item['browse_num'] is not None:
            item['browse_num'] = int(item['browse_num'].replace(",", ""))

        if '~' in item['public_equipment'] and item['public_equipment'] is not None:
            parts = item['public_equipment'].replace("%", "").split('~')
            item['public_equipment'] = (float(parts[0])+float(parts[1]))/2

        if '%' in item['cover_percentage'] and item['cover_percentage'] is not None:
            item['cover_percentage'] = int(item['cover_percentage'].replace("%", ""))

        if '戶' in item['total_resident'] and item['total_resident'] is not None:
            item['total_resident'] = int(item['total_resident'].replace("戶", ""))
        self.cursor.execute("""
            INSERT INTO properties (name, region, section, simple_address, current_sale_num, building_purpose, 
                                    browse_num, rent_num, agent_company, total_sold, price, station_name, latitude, 
                                    longitude, year, total_resident, building_type, usage_plan, cover_percentage, 
                                    public_equipment, building_structure, foundation_area, management_cost, 
                                    ground_separate_area, battery_filled_equipment, parking_percentage, 
                                    total_parking_amount, building_amount_management, level_management, 
                                    room_management, garbage_management, school_region)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            item.get('name'),
            item.get('region'),
            item.get('section'),
            item.get('simple_address'),
            item.get('current_sale_num'),
            item.get('building_purpose'),
            item.get('browse_num'),
            item.get('rent_num'),
            item.get('agent_company'),
            item.get('total_sold'),
            item.get('price'),
            item.get('station_name'),
            item.get('latitude'),
            item.get('longitude'),
            item.get('year'),
            item.get('total_resident'),
            item.get('building_type'),
            item.get('usage_plan'),
            item.get('cover_percentage'),
            item.get('public_equipment'),
            item.get('building_structure'),
            item.get('foundation_area'),
            item.get('management_cost'),
            item.get('ground_separate_area'),
            item.get('battery_filled_equipment'),
            item.get('parking_percentage'),
            item.get('total_parking_amount'),
            item.get('building_amount_management'),
            item.get('level_management'),
            item.get('room_management'),
            item.get('garbage_management'),
            item.get('school_region')
        ))
        self.connection.commit()
        
        return item

    def store_in_db(self, item):
        
        print(f"Storing in DB: {item}")