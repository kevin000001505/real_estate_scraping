import scrapy
import json
from housing_scrapy.items import HousingScrapyItem
from housing_scrapy.items import RealEstatePriceScrapyItem
import time
class ScrapyHouseSpider(scrapy.Spider):
    name = 'scrapy_house'
    allowed_domains = ['market.591.com.tw', 'bff.591.com.tw']
    start_urls = ['https://bff.591.com.tw/v2/community/search/list?page=1&regionid=1&from=3']
    
    def __init__(self):
        self.region_num = 1
        self.page = 1
        self.first_id = 0
        
    def parse(self, response):
    
        resp = json.loads(response.body)
        # check if region_number exists
        try:
            items = resp['data']['items']
        except KeyError:
            print('No such region number: ', self.region_num)
            self.region_num += 1
            yield scrapy.Request(
                url = f'https://bff.591.com.tw/v2/community/search/list?page=1&regionid={self.region_num}&from=3',
                callback=self.parse
            )
        # check if api data duplicated or no data
        if not items or items[0]['id'] == self.first_id:
            self.region_num += 1
            self.page = 1
            yield scrapy.Request(
                url = f'https://bff.591.com.tw/v2/community/search/list?page=1&regionid={self.region_num}&from=3',
                callback=self.parse
            )
        else:
            # record the first page first data id
            if self.page == 1:
                self.first_id = items[0]['id']
            # start to get the first page data
            for item_json in items:
                item = HousingScrapyItem()
                self.id = item_json.get('id')
                agent_info = item_json.get('agent', [])

                if isinstance(agent_info, list) and agent_info:
                    # Example: using the first agent's information if available
                    first_agent = agent_info[0]  # Assuming there is at least one agent in the list
                    agent_company = first_agent.get('company', 'No company')
                    sub_company = first_agent.get('sub_company', 'No sub-company')
                else:
                    agent_company = 'No company'
                    sub_company = 'No sub-company'

                item['name'] = item_json.get('name')
                item['region'] = item_json.get('region')
                item['section'] = item_json.get('section')
                item['simple_address'] = item_json.get('simple_address')
                item['current_sale_num'] = int(item_json.get('sale_num', 0))
                item['building_purpose'] = item_json.get('build_purpose_simple')
                item['browse_num'] = item_json.get('browse_num')
                item['rent_num'] = item_json.get('rent_num')
                item['agent_company'] = agent_company
                item['total_sold'] = int(item_json.get('price_num'))
                item['price'] = item_json['price']['price']
                item['station_name'] = item_json.get('station_name', 'No station name')
                item['latitude'] = float(item_json.get('lat'))
                item['longitude'] = float(item_json.get('lng'))

                # call the extract building inform data
                yield scrapy.Request(url=f'https://market.591.com.tw/{self.id}', callback=self.extract_data, meta={'item': item})
                
                # extract the real estate deal data
                self.real_page = 1
                yield scrapy.Request(url=f'https://bff.591.com.tw/v1/community/price/lists?community_id={self.id}&split_park=1&page=1&page_size=20&_source=0', callback=self.extract_real_price_data)
                
                # after extracting turn to next page
                self.page += 1
                yield scrapy.Request(
                    url=f'https://bff.591.com.tw/v2/community/search/list?page={self.page}&regionid={self.region_num}&from=3',
                    callback = self.parse,
                    dont_filter=True
                )

    def extract_data(self, response):
        item = response.meta['item']
        tables = response.xpath("//div[@class='overview-container']/ul")
        for table in tables:
            if table.xpath(".//li[1]/h6/text()").get() == '建案類別':
                item['year'] = table.xpath(".//li[1]/p/text()").get()
                item['total_resident'] = table.xpath(".//li[15]/p/text()").get()
                item['building_type'] = table.xpath(".//li[16]/p/text()").get()
                item['usage_plan'] = table.xpath(".//li[17]/p/text()").get()
                item['cover_percentage'] = table.xpath(".//li[18]/p/text()").get()
                item['public_equipment'] = table.xpath(".//li[19]/p/text()").get()
                item['building_structure'] = table.xpath(".//li[20]/p/text()").get()
                item['foundation_area'] = table.xpath(".//li[21]/p/text()").get()
                item['management_cost'] = table.xpath(".//li[22]/p/text()").get()
                item['ground_separate_area'] = table.xpath(".//li[23]/p/text()").get()
                item['parking_percentage'] = table.xpath(".//li[8]/p/text()").get()
                item['total_parking_amount'] = table.xpath(".//li[9]/p/text()").get()
                item['room_management'] = table.xpath(".//li[10]/p/text()").get()
                item['garbage_management'] = table.xpath(".//li[24]/p/text()").get()
                item['school_region'] = table.xpath(".//li[25]/p/text()").get()
                                                              
            else:
                item['year'] = table.xpath(".//li[1]/p/text()").get()
                item['total_resident'] = table.xpath(".//li[2]/p/text()").get()
                item['building_type'] = table.xpath(".//li[3]/p/text()").get()
                item['usage_plan'] = table.xpath(".//li[4]/p/text()").get()
                item['cover_percentage'] = table.xpath(".//li[5]/p/text()").get()
                item['public_equipment'] = table.xpath(".//li[6]/p/text()").get()
                item['building_structure'] = table.xpath(".//li[7]/p/text()").get()
                item['foundation_area'] = table.xpath(".//li[8]/p/text()").get()
                item['management_cost'] = table.xpath(".//li[9]/p/text()").get()
                item['ground_separate_area'] = table.xpath(".//li[10]/p/text()").get()
                item['battery_filled_equipment'] = table.xpath(".//li[12]/p/text()").get()
                item['parking_percentage'] = table.xpath(".//li[13]/p/text()").get()
                item['total_parking_amount'] = table.xpath(".//li[14]/p/text()").get()
                item['building_amount_management'] = table.xpath(".//li[15]/p/text()").get()
                item['level_management'] = table.xpath(".//li[16]/p/text()").get()
                item['room_management'] = table.xpath(".//li[17]/p/text()").get()
                item['garbage_management'] = table.xpath(".//li[18]/p/text()").get()
                item['school_region'] = table.xpath(".//li[19]/p/text()").get()
        yield item
                

    def extract_real_price_data(self, response):
        real_item = RealEstatePriceScrapyItem()
        real_resp = json.loads(response.body)
        real_items = real_resp['data']['items']
        
        for real_item_json in real_items:
            real_item['date'] = real_item_json.get('date')
            real_item['floor'] = real_item_json.get('shift_floor')
            real_item['unit_price'] = real_item_json['unit_price'].get('price')
            real_item['address'] = real_item_json.get('address')
            real_item['room'] = real_item_json.get('layout_v2')
            real_item['total_build_area'] = real_item_json.get('build_area')
            real_item['build_area'] = real_item_json['building_area'].get('area')
            real_item['park_area'] = real_item_json['real_park_area'].get('area')
            real_item['build_total_price'] = real_item_json['building_total_price'].get('price')
            real_item['park_price'] = real_item_json.get('real_park_total_price')
            real_item['parking_type'] = real_item_json.get('park_type_str')
            real_item['total_floor'] = real_item_json.get('total_floor')
        if not real_items:
            yield real_item
        
        self.real_page += 1
        yield response.follow(url = f'https://bff.591.com.tw/v1/community/price/lists?community_id={self.id}&split_park=1&page={self.real_page}&page_size=20&_source=0', callback=self.extract_real_price_data)
        
        
