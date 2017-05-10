import requests


def insert_from_ia(url, title, agency, site):
    # Get a list of all the 'Versions' (timestamps when the url was captured).
    res = requests.get('http://web.archive.org/web/timemap/link/{}'.format(url))
    content = res.iter_lines()
    next(content)  # throw away first line
    for line in content:
        # Work in progress: checking that the parsing works...
        print(line.decode().split('; '))
