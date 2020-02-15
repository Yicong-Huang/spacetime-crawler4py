from threading import Thread

from requests import HTTPError

from scraper import scraper
from tokenizer import print_freq
from utils import get_logger
from utils.download import download


class Worker(Thread):
    def __init__(self, worker_id, config, frontier, state):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        super().__init__(daemon=True)
        self.state = state

    def run(self):
        i = 0
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            try:
                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                scraped_urls = scraper(tbd_url, resp, self.state)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)

            except HTTPError as err:
                self.logger.error(
                    f"Downloaded {tbd_url}, hitting error {err}")

            self.frontier.mark_url_complete(tbd_url)
            if i % 1000 == 0:
                print(self.state['longest_page'])
                print_freq(self.state['word_rank'], 50)
                for domain, count in self.state['sub_domains'].items():
                    print(domain, count)
                self.frontier.print_saved()

            i += 1
