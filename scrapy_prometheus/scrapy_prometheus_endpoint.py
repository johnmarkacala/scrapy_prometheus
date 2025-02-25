import logging
from collections import defaultdict

from prometheus_client.twisted import MetricsResource
from prometheus_client import CollectorRegistry
from twisted.web import resource
from twisted.web.server import Site
from scrapy.exceptions import NotConfigured
from scrapy.utils.reactor import listen_tcp, install_reactor
from scrapy import signals

logger = logging.getLogger(__name__)

class ScrapyPrometheusWebServiceMixin(Site):
    """
    Handles hosting and metrics exposure for Prometheus in Scrapy.
    """
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

    def __init__(self, crawler, **kwargs):
        self.crawler = crawler
        self.tasks = []
        self.stats = self
        self.registries = defaultdict(CollectorRegistry)

        # Connect Scrapy signals
        crawler.signals.connect(self.spider_opened, signals.spider_opened)
        crawler.signals.connect(self.spider_closed, signals.spider_closed)
        crawler.signals.connect(self.item_scraped, signals.item_scraped)
        crawler.signals.connect(self.item_dropped, signals.item_dropped)
        crawler.signals.connect(self.response_received, signals.response_received)

        # Prometheus settings
        settings = crawler.settings
        self.prometheus_port = int(settings.get('PROMETHEUS_PORT', '9410'))
        self.prometheus_host = settings.get('PROMETHEUS_HOST', '0.0.0.0')
        self.prometheus_path = settings.get('PROMETHEUS_PATH', 'metrics')
        self.prometheus_interval = settings.get('PROMETHEUS_UPDATE_INTERVAL', 30)
        self.prometheus_endpoint_enabled = settings.getbool('PROMETHEUS_ENDPOINT_ENABLED', True)
        self.reg_name = settings.get("DEFAULT_REGISTRY", "scrapy_prometheus")

        self._start_server()

    def get_registry(self):
        return self.registries[self.reg_name]

    def _start_server(self):
        if not self.prometheus_endpoint_enabled:
            return

        root = resource.Resource()
        registry = self.get_registry()
        self._prom_metrics_resource = MetricsResource(registry=registry)
        root.putChild(self.prometheus_path.encode('utf-8'), self._prom_metrics_resource)
        Site.__init__(self, root)

    def _start_prometheus_endpoint(self):
        if self.prometheus_endpoint_enabled:
            self.prometheus = listen_tcp(self.prometheus_port, self.prometheus_host, self)

    def _stop_prometheus_endpoint(self):
        for task in self.tasks:
            if task.running:
                task.stop()
        
        if hasattr(self, 'prometheus') and self.prometheus:
            self.prometheus.stopListening()
