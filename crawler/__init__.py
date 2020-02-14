import shelve
from collections import defaultdict

from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker


class Counts:
    def __init__(self):
        self.outlink_count = 0
        self.query_count = 0
        self.download_count = 0
        self.visit_count = 0


class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):

        self.config = config
        self.logger = get_logger("CRAWLER")
        self.load_state()
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier, self.state)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()

    def load_state(self):
        self.state = shelve.open(self.config.states_file)

        self.state['counts'] = self.state.get('counts', defaultdict(Counts))
        self.state['pattern_counts'] = self.state.get('pattern_counts', defaultdict(int))
        self.state['longest_page'] = self.state.get('longest_page', (0, ['']))
        self.state['word_rank'] = self.state.get('word_rank', defaultdict(int))
        self.state['sub_domains'] = self.state.get('sub_domains', defaultdict(int))
