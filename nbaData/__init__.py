import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


process = CrawlerProcess(get_project_settings())

for spider_name in process.spiders.list():

    # Skips teams because it never changes
    if (spider_name != 'teams'):

        print ("Running spider %s" % (spider_name))
        process.crawl(spider_name)
