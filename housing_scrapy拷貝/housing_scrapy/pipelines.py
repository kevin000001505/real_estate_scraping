# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
from housing_scrapy.items import RealEstatePriceScrapyItem
from housing_scrapy.items import HousingScrapyItem

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
        if isinstance(item, HousingScrapyItem):
            year = item.get('year', None)
            if year is not None and '年' in year:
                item['year'] = int(year.replace("年", ""))

            browse_num = item.get('browse_num', None)
            if browse_num and ',' in browse_num:
                item['browse_num'] = int(browse_num.replace(",", ""))

            public = item.get('public_equipment', None)
            if public is not None and '~' in public:
                parts = item['public_equipment'].replace("%", "").split('~')
                item['public_equipment'] = (float(parts[0])+float(parts[1]))/2

            cover = item.get('cover_percentage', None)
            if cover is not None and '%' in cover:
                item['cover_percentage'] = float(item['cover_percentage'].replace("%", ""))

            resident = item.get('total_resident', None)
            if resident is not None and '戶' in resident:
                item['total_resident'] = int(item['total_resident'].replace("戶", "").replace(",", ""))

            self.cursor.execute("""
                INSERT INTO real_esate_table (name, region, section, simple_address, current_sale_num, building_purpose, 
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
class SaveToRealEstateDealPipeline:
    def open_spider(self, spider):
        self.real_connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='@America155088',
            database='real_estate_db'
        )
        self.real_cursor = self.real_connection.cursor()

    def close_spider(self, spider):
        self.real_cursor.close()
        self.real_connection.close()

    def process_item(self, item, spider):
        if isinstance(item, RealEstatePriceScrapyItem):
            
            # SQL insert
            self.real_cursor.execute("""
                INSERT INTO real_estate_deal (address, build_area, build_total_price, date, floor, park_area, park_price, parking_type, room, total_build_area, total_floor, unit_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    
                ))
            self.real_connection.commit()
        return item