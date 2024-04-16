# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
from housing_scrapy.items import RealEstatePriceScrapyItem
from housing_scrapy.items import HousingScrapyItem
from datetime import datetime

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

            price_str = item.get('price', '0').strip()
            if price_str:
                try:
                    price = float(price_str)
                except ValueError:
                    # Log the error and handle cases where conversion fails
                    print(f"Error converting price to float: {price_str}")
                    price = 0  # Set a default value or handle it another way
            else:
                price = 0 
            
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
                price,
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
            # Date
            taiwan_date_str = item.get('date', '')
            year, month, day = map(int, taiwan_date_str.split('-'))
            gregorian_year = year + 1911
            date_obj = datetime(gregorian_year, month, day)
            formatted_date = date_obj.strftime('%Y-%m-%d')
            # Address
            address_str = item.get('address', '').split(' ')[0]
            # Build area
            build_area_str = item.get('build_area', '').replace('坪', '')
            # Build total price
            build_total_price_flo = float(item.get('build_total_price', '').replace(',', ''))
            # Floor
            floor_int = int(item.get('floor', '').replace('樓', ''))
            # Park area
            park_area_flo = float(item.get('park', 0))
            # Park price
            park_price_flo = float(item.get('park_price', ''))
            # Park type
            park_type_str = item.get('parking_type', '')
            # Room 
            room_str = item.get('room', '')
            # Total_build_area
            total_build_area_flo = float(item.get('total_build_area', '').replace('坪', ''))
            # Total_floor
            total_floor_int = int(item.get('total_floor', '').replace('樓', ''))
            # Unit_price
            unit_price_flo = float(item.get('unit_price', ''))
            # SQL insert
            self.real_cursor.execute("""
                INSERT INTO real_estate_deal (address, build_area, build_total_price, date, floor, park_area, park_price, parking_type, room, total_build_area, total_floor, unit_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    address_str,
                    build_area_str,
                    build_total_price_flo,
                    formatted_date,
                    floor_int,
                    park_area_flo,
                    park_price_flo,
                    park_type_str,
                    room_str,
                    total_build_area_flo,
                    total_floor_int,
                    unit_price_flo
                ))
            self.real_connection.commit()
        return item