# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class RedfinItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    _id = scrapy.Field()
    sale_listing = scrapy.Field()    
    sold_date = scrapy.Field()       
    property_type = scrapy.Field()   
    address = scrapy.Field()         
    city = scrapy.Field()            
    state = scrapy.Field()           
    zipcode = scrapy.Field()             
    price = scrapy.Field()           
    beds = scrapy.Field()            
    baths = scrapy.Field()           
    location = scrapy.Field()        
    square_feet = scrapy.Field()     
    lot_size = scrapy.Field()        
    year_build = scrapy.Field()      
    days_on_market = scrapy.Field()  
    square_feet_price = scrapy.Field()
    hoa_month = scrapy.Field()       
    status = scrapy.Field()          
    url = scrapy.Field()             
    source = scrapy.Field()          
    mls = scrapy.Field()             
    latitude = scrapy.Field()        
    longitude = scrapy.Field()                 
    history = scrapy.Field()
    initial_date = scrapy.Field()
    last_update = scrapy.Field()       

