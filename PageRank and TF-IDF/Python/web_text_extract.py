import urllib
import os, os.path
from bs4 import BeautifulSoup


def get_text(url, name):
    html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html, 'lxml')

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()    # rip it out

    out_file = open("webText/" + name, 'w+')
    text = soup.get_text('|', strip =True)
    lines = text.split('|')

    for line in lines:
        clean_line = line.encode('ascii', 'replace').replace('?', ' ')
        clean_line = clean_line.replace(",", " ").replace("(", "").replace(")", "")
        if clean_line.strip():
            out_file.write(clean_line + "\n")
    out_file.close()


def extract():
    if not os.path.isdir('webText'):
        os.mkdir('webText')

    url_list = open('urllist.txt', 'r')

    for entry in url_list:
        words = entry.split(',')
        get_text(words[1], words[0])
    print "Web Text extraction completed"

if __name__ == "__main__":
    extract()