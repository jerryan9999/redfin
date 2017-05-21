# !/bin/bash
# History:
# 2017/02/28 Jerry First release


ProjectPath="/srv/crawlers/redfin/"

# activate virtual environment
cd '/srv/crawlers/venv/'
. bin/activate

# starts crawling
cd $ProjectPath
scrapy crawl RedfinSpider && python3 "writing_to_db.py" && scrapy crawl RedfinSpiderdb

# email report
python3 "email_report.py"

cd $ProjectPath
# leave virtual environment and exit
deactivate
exit
