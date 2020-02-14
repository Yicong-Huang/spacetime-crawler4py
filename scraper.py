import re
import traceback
from collections import OrderedDict, defaultdict
from functools import reduce
from threading import RLock
from typing import List
from urllib.parse import urlparse, urlunparse

from lxml import html

from tokenizer import tokenize, compute_word_frequencies
from utils import get_logger
from utils.response import Response

state_lock = RLock()
logger = get_logger("scraper")
import html2text

h = html2text.HTML2Text()
h.ignore_links = True

with open("stop_words.txt", 'r') as file:
    stop_words = [line.strip() for line in file.readlines()]

from bs4 import BeautifulSoup
from bs4.element import Comment


def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts)


def scraper(url, resp, state):
    links = extract_next_links(url, resp, state)
    return [link for link in links if is_valid(link)]


def extract_next_links(url: str, resp: Response, state):
    if not resp.raw_response:
        return []
    print('extracting', url)
    output_links = []
    resp.url = stripTrailingSlash(resp.url)
    try:

        doc = html.document_fromstring(resp.raw_response.content)
        doc.make_links_absolute(
            resp.final_url if resp.is_redirected else resp.url)

        urls = [urlunparse(urlparse(i[2])) for i in doc.iterlinks()]
        urls = set(imap_multiple(urls, stripTrailingSlash, removeFragment))
        try:
            # content = h.handle(resp.raw_response.content.decode("utf-8", "ignore"))
            content = text_from_html(resp.raw_response.content)
            tokens = tokenize(content)
            word_count = len(tokens)
            words = [word for word in tokens if word not in stop_words]
            freqs = compute_word_frequencies(words)
        except:
            logger.error(traceback.format_exc())
            freqs = {}
            word_count = 0

        parsed_url = urlparse(url)
        sub_domain = f'{parsed_url.scheme}://{parsed_url.hostname}'

        with state_lock:
            # Count the number of output_links on the page
            counts = state['counts']
            counts[resp.url].outlink_count += len(urls)
            counts[resp.url].download_count += 1

            # Count the common words
            word_rank = state['word_rank']
            for word, freq in freqs.items():
                word_rank[word] += freq

            if len(word_rank) > 5000:
                new_word_rank = defaultdict(int)
                new_word_rank.update(dict(list(sorted(word_rank.items(), key=lambda x: -x[1]))[:3000]))
                del word_rank
                word_rank = new_word_rank
            state['word_rank'] = word_rank

            # Count the sub_domain
            sub_domains = state['sub_domains']
            sub_domains[sub_domain] += 1
            state['sub_domains'] = sub_domains

            # Count the longest page
            max_count, pages = state['longest_page']
            if word_count == max_count:
                pages.append(url)
            elif word_count > max_count:
                pages = [url]
                max_count = word_count
            state['longest_page'] = (max_count, pages)

            # Define url patterns to match and it's max count
            patterns = OrderedDict()
            patterns['news/view_news(php)?'] = 50
            patterns['calendar.ics.uci.edu/calendar.php'] = 0
            patterns['ganglia.ics.uci.edu'] = 0
            patterns['.*'] = -1  # Any number of occurrence

            filters = [isHttpOrHttps,
                       isInDomain(["ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu", ".stat.uci.edu",
                                   "today.uci.edu/department/information_computer_sciences/"]),
                       isNotAsset,
                       queryCount(300, counts),
                       patternCount(patterns, state),
                       linkCount(lambda x: x < 1, counts)
                       ]

            output_links = list(applyFilters(filters, urls))
            state['counts'] = counts
            state.sync()

    except Exception:

        logger.error(traceback.format_exc())

    return output_links


def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print("TypeError for ", parsed)
        raise


def removeFragment(url):
    return url.split("#")[0]


def removeQuery(url):
    return url.split("?")[0]


def isNotAsset(url):
    return not isAsset(url)


def isHttpOrHttps(url):
    return any(url.startswith(x + "://") for x in ["http", "https"])


def subdomain(url):
    return urlparse(url).hostname.split('.')[0]


def isInDomain(domains: List[str]):
    def validator(url):
        return any([domain in urlparse(url).hostname if urlparse(url).hostname else False for domain in domains])

    return validator


def isAsset(url):
    url = removeFragment(removeQuery(url))
    return re.match(
        ".*\.(css|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|ps|eps|tex|ppt|"
        "pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|cnf|tgz|sha1|thmx|mso|arff|"
        "rtf|csv|rm|smil|wmv|swf|wma|zip|rar|gz|pdf|py|java|cc|class|h|cs|dll|hpp|jar|js|vbp|lib|cpp|pyc|)$",
        url)


def linkCount(f, counts):
    def counter(url):
        should_return = f(counts[url].visit_count)
        if should_return:
            counts[url].visit_count += 1
        return should_return

    return counter


def patternCount(lookup, state):
    def counter(url):
        for entry in lookup:
            if re.search(entry, url) is not None:
                pattern_counts = state['pattern_counts']
                pattern_counts[entry] += 1
                state['pattern_counts'] = pattern_counts
                return pattern_counts[entry] <= lookup[entry] or lookup[entry] == -1
        raise KeyError("Cannot find pattern for %s" % url)

    return counter


def applyFilters(filters, iterable):
    return reduce(lambda s, f: filter(f, s), filters, iterable)


def shouldShutdown(total_download_counts):
    return total_download_counts > 5000


def queryCount(num_limit, counts):
    def _count(url):
        url = url.split("?")[0]

        result = counts[url].query_count < num_limit
        if result:
            counts[url].query_count += 1
        return result

    return _count


def stripTrailingSlash(url):
    return url.strip('/')


def imap_multiple(iterable, function, *f):
    if f:
        return imap_multiple(map(function, iterable), *f)
    return map(function, iterable)
