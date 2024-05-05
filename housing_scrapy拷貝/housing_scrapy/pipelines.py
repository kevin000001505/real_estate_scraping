
from itemadapter import ItemAdapter
import mysql.connector
from housing_scrapy.items import RealEstatePriceScrapyItem, HousingScrapyItem
from datetime import datetime

class DataTransformationPipeline:
    @staticmethod
    def clean_numeric(value, char_to_remove, convert_type=float, default=None):
        try:
            return convert_type(value.replace(char_to_remove, ''))
        except (ValueError, AttributeError):
            return default

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if isinstance(item, HousingScrapyItem):
            adapter['price'] = self.clean_numeric(adapter.get('price', '0'), ',', float, 0)
            adapter['year'] = self.clean_numeric(adapter.get('year', ''), '年', int)
            adapter['browse_num'] = self.clean_numeric(adapter.get('browse_num', ''), ',', int)
            adapter['cover_percentage'] = self.clean_numeric(adapter.get('cover_percentage', '0'), '%', float, 0)
            adapter['total_resident'] = self.clean_numeric(adapter.get('total_resident', ''), '戶', int)
            adapter['public_equipment'] = self.avg_public_equipment(adapter.get('public_equipment'))
        elif isinstance(item, RealEstatePriceScrapyItem):
            adapter['date'] = self.format_date(adapter.get('date', ''))
            adapter['address'] = adapter.get('address', '').split(' ')[0]
            adapter['build_area'] = self.clean_numeric(adapter.get('build_area', ''), '坪', float)
            adapter['build_total_price'] = self.clean_numeric(adapter.get('build_total_price', ''), ',', float)
            adapter['park_area'] = self.clean_numeric(adapter.get('park_area', '0'), '', float)
            adapter['park_price'] = self.clean_numeric(adapter.get('park_price', ''), '', float)
            adapter['total_build_area'] = self.clean_numeric(adapter.get('total_build_area', ''), '坪', float)
            adapter['floor'] = self.clean_numeric(adapter.get('floor', ''), '樓', int)
            adapter['total_floor'] = self.clean_numeric(adapter.get('total_floor', ''), '樓', int)
        return item

    def avg_public_equipment(self, public):
        if public and '~' in public:
            parts = [float(p.replace('%', '')) for p in public.split('~')]
            return sum(parts) / len(parts)
        return None

    def format_date(self, date_str):
        try:
            year, month, day = map(int, date_str.split('-'))
            return datetime(year + 1911, month, day).strftime('%Y-%m-%d')
        except ValueError:
            return None

class DatabaseInsertionPipeline:
    def open_spider(self, spider):
        self.connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='your_own_password',
            database='real_estate_db'
        )
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    def process_item(self, item, spider):
        if isinstance(item, HousingScrapyItem):
            self.insert_real_estate(item)
        elif isinstance(item, RealEstatePriceScrapyItem):
            self.insert_real_estate_deal(item)
        return item

    def insert_real_estate(self, item):
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
            item['name'], 
            item['region'], 
            item['section'], 
            item['simple_address'], 
            item['current_sale_num'], 
            item['school_region'],
            item['browse_num'], 
            item['rent_num'], 
            item['agent_company'], 
            item['total_sold'], 
            item['price'],
            item['station_name'], 
            item['latitude'], 
            item['longitude'], 
            item['year'], 
            item['total_resident'],
            item['building_type'], 
            item['usage_plan'], 
            item['cover_percentage'], 
            item['public_equipment'], 
            item['building_structure'],
            item['foundation_area'], 
            item['management_cost'], 
            item['ground_separate_area'], 
            item.get('battery_filled_equipment', None),
            #item['battery_filled_equipment'], 
            item['parking_percentage'],
            item['total_parking_amount'], 
            item['building_amount_management'], 
            item['level_management'], 
            item['room_management'], 
            item['garbage_management'],
            item['school_region']
        ))
        self.connection.commit()

    def insert_real_estate_deal(self, item):
        self.cursor.execute("""
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