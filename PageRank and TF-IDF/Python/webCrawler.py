import urllib2
import httplib


# return html content
def get_page(url):
    try:
        response = urllib2.urlopen(url)
        html = response.read()
        return html

    except urllib2.HTTPError:
        return ""
    except urllib2.URLError:
        return ""


# returns next url
def get_next_target(page):

    start_link = page.find('<a href=')
    if start_link == -1:
        return None, 0
    start_quote = page.find('"', start_link)
    end_quote = page.find('"', start_quote + 1)
    url = page[start_quote + 1:end_quote]

    url = url.strip()
    if url.endswith("/"):
        url = url[:-1]

    url = url.replace("faculty-profiles", "faculty")
    url = url.replace(" ", "")
    if not url.__contains__("/people/") and url.__contains__("/faculty/"):
        url = url.replace("/faculty/", "/people/faculty/")

    try:
        if "cs.utdallas.edu/?p" in url:
            url = urllib2.urlopen(url).geturl()
            if url.endswith("/"):
                url = url[:-1]

    except ValueError:
        return "http://dummy.url", end_quote
    except urllib2.HTTPError:
        print url
        return "http://dummy.url", end_quote
    except urllib2.URLError:
        print url
        return "http://dummy.url", end_quote
    except httplib.BadStatusLine:
        print url + "....http....."
        return "http://dummy.url", end_quote

    return url, end_quote


# union of two lists with urls
def union(p, q):
    for e in q:
        if e not in p:
            p.append(e)


# checks for junk url like image links
def is_junk_url(page):
    if page.__contains__('.jpg') or page.__contains__('.jpeg') or page.__contains__('.png') or \
            page.__contains__('.xlsx') or page.__contains__('.pdf') or page.__contains__('.doc') or\
            page.__contains__('.zip'):
        return True
    else:
        return False


# get all links inside a web page
def get_all_links(page):
    links = []

    while True:
        url, end_pos = get_next_target(page)
        if url:
            if url not in links and url.__contains__('http://cs.utdallas') and not is_junk_url(url):
                links.append(url)
            page = page[end_pos:]
        else:
            break
    return links


# web crawler
def crawl_web(seed):
    to_crawl = [seed]
    crawled = []
    links = []
    pr = open('pagerank.txt', 'w+')
    url = open('urllist.txt', 'w+')

    flat = open('flat.txt', 'w+')
    line_number = 0

    while to_crawl:
        page = to_crawl.pop()
        if page not in crawled:
            if page.__contains__('http://cs.utdallas') and not is_junk_url(page):
                links = get_all_links(get_page(page))
                so_far = []

                line_number += 1
                if links:
                    for link in links:
                        if link != page and link not in so_far:
                            so_far.append(link)
                            pr.write(str(line_number) + " " + link + "\n")
                            flat.write(page + "\t" + link + "\n")

                union(to_crawl, links)
                crawled.append(page)

                url.write(str(line_number) + "," + page + "\n")
    print "Web crawling completed"

    pr.close()
    url.close()
    flat.close()


def load_url_list(table):
    url_list = open('urllist.txt', 'r')

    for line in url_list:
        words = line.split(',')
        table[words[1]] = words[0]


def page_rank_format():
    table = dict()
    load_url_list(table)
    page_rank = open('pagerank.txt', 'r')

    out_file = open('prank.txt', 'w+')
    for line in page_rank:
        words = line.split(' ')
        if table.get(words[1]) is not None:
            out_file.write(words[0] + " " + str(table.get(words[1])) + "\n")
    print "Page rank format completed"


# main function
if __name__ == "__main__":
    crawl_web("http://cs.utdallas.edu")   # ut dallas cs homepage as seed url