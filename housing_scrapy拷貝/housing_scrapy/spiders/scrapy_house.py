import scrapy
import json
from housing_scrapy.items import HousingScrapyItem
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from scrapy.selector import Selector
from scrapy_selenium import SeleniumRequest
import time

class ScrapyHouseSpider(scrapy.Spider):
    name = 'scrapy_house'
    allowed_domains = ['market.591.com.tw']
    start_urls = ['https://bff.591.com.tw/v2/community/search/list?page=1&regionid=1&from=3&post_type=2,8']
    
    def __init__(self):
        self.region_num = 1
        self.page = 1
        
    def parse(self, response):
        
        resp = json.loads(response.body)

        items = resp['data']['items']
        if not items:
            self.region_num += 1
            self.page = 1
            yield scrapy.Request(
                url = f'https://bff.591.com.tw/v2/community/search/list?page={self.page}&regionid={self.region_num}&from=3&post_type=2,8',
                callback=self.parse
            )
        else:
            for item_json in items:
                item = HousingScrapyItem()
                id = item_json.get('id')
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
                item['sale_num'] = item_json.get('sale_num', 0)
                item['building_purpose'] = item_json.get('build_purpose_simple')
                item['browse_num'] = item_json.get('browse_num')
                item['rent_num'] = item_json.get('rent_num')
                item['agent_company'] = agent_company
                item['sub_company'] = sub_company
                tags = item_json.get('tag', [])
                if tags:  # Check if the list is not empty
                    item['current_sold_num'] = tags[0].get('tag_str', 'No tag info')
                else:
                    item['current_sold_num'] = 'No tag info'

                item['total_sold'] = item_json.get('price_num')
                item['price'] = item_json['price']['price']
                item['station_name'] = item_json.get('station_name', 'No station name')

                yield response.follow(url=f'https://market.591.com.tw/{id}', callback=self.extract_data, meta={'item': item})

                self.page += 1
                yield scrapy.Request(
                    url=f'https://bff.591.com.tw/v2/community/search/list?page={self.page}&regionid={self.region_num}&from=3&post_type=2,8',
                    callback = self.parse,
                    dont_filter=True
                )

    def extract_data(self, response):
        item = response.meta['item']
        tables = response.xpath("//div[@class='overview-container']/ul")
        for table in tables:
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