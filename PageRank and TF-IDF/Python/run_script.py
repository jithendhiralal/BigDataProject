import webCrawler
import web_text_extract


def run():
    webCrawler.crawl_web("http://cs.utdallas.edu")
    webCrawler.page_rank_format()
    web_text_extract.extract()

# main function
if __name__ == "__main__":
    run()