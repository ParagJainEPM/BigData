from bs4 import BeautifulSoup
import json
import urllib

url = 'https://www.marsja.se/python-manova-made-easy-using-statsmodels/'

headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11' \
                         '(KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
           'Accept-Encoding': 'none',
           'Accept-Language': 'en-US,en;q=0.8',
           'Connection': 'keep-alive'}

req = urllib.request.Request(url, headers=headers)
page = urllib.request.urlopen(req)
text = page.read()

soup = BeautifulSoup(text, 'lxml')
create_nb = {'nbformat': 4, 'nbformat_minor': 2,
             'cells': [], 'metadata':
                 {"kernelspec":
                      {"display_name": "Python 3",
                       "language": "python", "name": "python3"
                       }}}


def get_data(soup, content_class):
    for div in soup.find_all('div',
                             attrs={'class': content_class}):

        code_chunks = div.find_all('code')

        for chunk in code_chunks:
            cell_text = ' '
            cell = {}
            cell['metadata'] = {}
            cell['outputs'] = []
            cell['source'] = [chunk.get_text()]
            cell['execution_count'] = None
            cell['cell_type'] = 'code'
            create_nb['cells'].append(cell)


get_data(soup, 'post-content')

with open('Python_MANOVA.ipynb', 'w') as jynotebook:
    jynotebook.write(json.dumps(create_nb))

create_nb