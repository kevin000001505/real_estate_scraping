# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class HousingScrapyItem(scrapy.Item):
    name = scrapy.Field()
    region = scrapy.Field()
    section = scrapy.Field()
    simple_address = scrapy.Field()
    current_sale_num = scrapy.Field()
    building_purpose = scrapy.Field()
    browse_num = scrapy.Field()
    rent_num = scrapy.Field()
    agent_company = scrapy.Field()
    total_sold = scrapy.Field()
    price = scrapy.Field()
    station_name = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    # building details
    year = scrapy.Field()
    total_resident = scrapy.Field()
    building_type = scrapy.Field()
    usage_plan = scrapy.Field()
    cover_percentage = scrapy.Field()
    public_equipment = scrapy.Field()
    building_structure = scrapy.Field()
    foundation_area = scrapy.Field()
    management_cost = scrapy.Field()
    ground_separate_area = scrapy.Field()
    battery_filled_equipment = scrapy.Field()
    parking_percentage = scrapy.Field()
    total_parking_amount = scrapy.Field()
    building_amount_management = scrapy.Field()
    level_management = scrapy.Field()
    room_management = scrapy.Field()
    garbage_management = scrapy.Field()
    school_region = scrapy.Field()


