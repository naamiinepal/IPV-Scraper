#import Facebook_scraper class from facebook_page_scraper
from facebook_page_scraper import Facebook_scraper

#instantiate the Facebook_scraper class

def main():
    page_name = "metaai"
    posts_count = 10
    browser = "firefox"
    timeout = 600 #600 seconds

    meta_ai = Facebook_scraper(page_name,posts_count,browser,timeout=timeout)

    json_data = meta_ai.scrap_to_json()
    print(json_data)

if __name__ == "__main__":
    main()