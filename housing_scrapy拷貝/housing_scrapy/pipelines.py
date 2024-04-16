# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
from housing_scrapy.items import RealEstatePriceScrapyItem, HousingScrapyItem
from datetime import datetime

class DataTransformationPipeline:
    def process_item(self, item, spider):
        if isinstance(item, HousingScrapyItem):
            item['price'] = self.transform_price(item.get('price', '0'))
            item['year'] = self.extract_year(item.get('year', ''))
            item['browse_num'] = self.extract_browse_num(item.get('browse_num', ''))
            item['public_equipment'] = self.avg_public_equipment(item.get('public_equipment', None))
            item['cover_percentage'] = self.extract_percentage(item.get('cover_percentage', '0'))
            item['total_resident'] = self.extract_resident(item.get('total_resident', ''))
            return item
        elif isinstance(item, RealEstatePriceScrapyItem):
            item['date'] = self.format_date(item.get('date', ''))
            item['address'] = item.get('address', '').split(' ')[0]
            item['build_area'] = item['build_area'].replace('坪', '')
            item['build_total_price'] = float(item['build_total_price'].replace(',', ''))
            item['floor'] = self.transform_floor(item.get('floor', ''))
            item['total_floor'] = self.transform_total_floor(item.get('total_floor', ''))
            item['park_area'] = float(item.get('park', 0))
            item['park_price'] = self.transform_park_price(item.get('park_price', ''))
            item['total_build_area'] = self.transform_total_build_area(item.get('total_build_area', ''))
            return item
    
    def transform_floor(self, floor_str):
        try:
            return int(floor_str.replace('樓', '')) if '樓' in floor_str else None
        except ValueError:
            return str(floor_str)
    def transform_total_floor(self, total_floor_str):
        return int(total_floor_str.replace('樓', '')) if '樓' in total_floor_str else None

    def transform_total_build_area(self, total_build_area_str):
        return float(total_build_area_str.replace('坪', '')) if '坪' in total_build_area_str else None
    def transform_price(self, price_str):
        try:
            return float(price_str.strip())
        except ValueError:
            print(f"Error converting price to float: {price_str}")
            return 0
    def transform_park_price(self, price_str):
        try:
            float(price_str)
        except ValueError:
            print(f"Error converting park price to float: {price_str}")
            return 0
        
    def extract_year(self, year_str):
        return int(year_str.replace("年", "")) if '年' in year_str else None

    def extract_browse_num(self, browse_num):
        return int(browse_num.replace(",", "")) if ',' in browse_num else None

    def avg_public_equipment(self, public):
        if public and '~' in public:
            parts = public.replace("%", "").split('~')
            return (float(parts[0]) + float(parts[1])) / 2
        return None

    def extract_percentage(self, percent):
        try:
            return float(percent.replace("%", "")) if '%' in percent else 0
        except TypeError:
            print(f"Error converting percentage to float: {percent}")
            return None

    def extract_resident(self, resident):
        try:
            return int(resident.replace("戶", "").replace(",", "")) if '戶' in resident else None
        except TypeError:
            return None
    def format_date(self, taiwan_date_str):
        year, month, day = map(int, taiwan_date_str.split('-'))
        gregorian_year = year + 1911
        date_obj = datetime(gregorian_year, month, day)
        return date_obj.strftime('%Y-%m-%d')


class DatabaseInsertionPipeline:
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
            # Insert into real estate table
            self.insert_real_estate(item)
        elif isinstance(item, RealEstatePriceScrapyItem):
            # Insert into real estate deal table
            self.insert_real_estate_deal(item)
        return item

    def insert_real_estate(self, item):
        # SQL INSERT statement for HousingScrapyItem
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

    def insert_real_estate_deal(self, item):
        # SQL INSERT statement for RealEstatePriceScrapyItem
        self.cursor.execute("""
                INSERT INTO real_estate_deal (address, build_area, build_total_price, date, floor, park_area, park_price, parking_type, room, total_build_area, total_floor, unit_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    item.get('address'),
                    item.get('build_area'),
                    item.get('build_total_price'),
                    item.get('date'),
                    item.get('floor'),
                    item.get('park_area'),
                    item.get('park_price'),
                    item.get('parking_type'),
                    item.get('room'),
                    item.get('total_build_area'),
                    item.get('total_floor'),
                    item.get('unit_price')
                ))
        self.connection.commit()
        return item