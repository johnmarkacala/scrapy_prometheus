import socket
import re

import prometheus_client
from scrapy import statscollectors, signals
from .scrapy_prometheus_endpoint import ScrapyPrometheusWebServiceMixin

METRIC_COUNTER = prometheus_client.Counter
METRIC_GAUGE = prometheus_client.Gauge

def push_to_gateway(pushgateway, registry, method, timeout, job, grouping_key):
    push_func = prometheus_client.pushadd_to_gateway if method.upper() == 'POST' else prometheus_client.push_to_gateway
    return push_func(pushgateway, job=job, grouping_key=grouping_key, timeout=timeout, registry=registry)
class PrometheusStatsCollector(ScrapyPrometheusWebServiceMixin, statscollectors.StatsCollector):
    def __init__(self, crawler, **kwargs):
        self.crawler = crawler
        self.crawler.signals.connect(self.engine_stopped, signal=signals.engine_stopped)
        self.crawler.signals.connect(self.engine_started, signals.engine_started)

        statscollectors.StatsCollector.__init__(self, crawler=crawler, **kwargs)
        ScrapyPrometheusWebServiceMixin.__init__(self, crawler=crawler, **kwargs)

        self.registry=self.get_registry()

        self.prometheus_default_labels = self.crawler.settings.getdict('PROMETHEUS_DEFAULT_LABELS', {})
        self.prometheus_default_labels.setdefault('instance', socket.gethostname() or "")

        self.metric_prefix = self.crawler.settings.get('PROMETHEUS_METRIC_PREFIX', 'scrapy_prometheus')
        self.pushgateway=self.crawler.settings.get('PUSHGATEWAY', '127.0.0.1:9091')
        self.pushgateway_method=self.crawler.settings.get('PUSHGATEWAY_PUSH_METHOD', 'POST')
        self.pushgateway_timeout=self.crawler.settings.get('PUSHGATEWAY_PUSH_TIMEOUT', 5)
        self.pushgatewayjob=self.crawler.settings.get('PUSHGATEWAY_JOB', 'pushgateway_scrapy')

    @classmethod
    def from_crawler(cls, crawler):
        stats = cls(crawler)
        crawler.stats = stats
        return stats

    def get_metric(self, key, value, spider=None, labels=None, metric_type=METRIC_GAUGE):
        if not isinstance(value, (int, float)):
            return None

        if 'exception_type' in key:
            return None  # Skip exception-related metrics

        name = f"{self.metric_prefix}_{re.sub(r'[^a-zA-Z0-9_]', '_', key)}"
        labels = labels or self.get_labels(spider)
        
        if name not in self.registry._names_to_collectors:
            metric = metric_type(name, key, labels, registry=self.registry)
        else:
            metric = self.registry._names_to_collectors[name]
        
        return metric.labels(**labels)

    def set_value(self, key, value, spider=None, labels=None):
        super().set_value(key, value, spider)
        if metric := self.get_metric(key, value, spider, labels, METRIC_GAUGE):
            metric.set(value)

    def inc_value(self, key, count=1, start=0, spider=None, labels=None):
        super().inc_value(key, count, start, spider)
        if metric := self.get_metric(key, start, spider, labels, METRIC_COUNTER):
            metric.inc(count)

    def max_value(self, key, value, spider=None, labels=None):
        super().max_value(key, value, spider)
        if metric := self.get_metric(key, value, spider, labels, METRIC_GAUGE):
            metric.set(max(metric._value.get(), value))

    def min_value(self, key, value, spider=None, labels=None):
        super().min_value(key, value, spider)
        if metric := self.get_metric(key, value, spider, labels, METRIC_GAUGE):
            metric.set(min(metric._value.get(), value))

    def get_labels(self, spider=None):
        labels = {'spider': spider.name if spider else 'default', 'job_id': getattr(spider, 'job_id', '')}
        labels.update(self.prometheus_default_labels)
        return labels

    def _persist_stats(self, stats, spider=None):
        super()._persist_stats(stats, spider)
        try:
            push_to_gateway(
                pushgateway=self.pushgateway,
                registry=self.registry,
                method=self.pushgateway_method,
                timeout=self.pushgateway_timeout,
                job=self.pushgatewayjob,
                grouping_key=self.get_labels(spider)
            )
        except Exception:
            if spider:
                spider.logger.exception(f'Failed to push "{spider.name}" spider metrics to pushgateway')
        else:
            if spider:
                spider.logger.info(f'Pushed "{spider.name}" spider metrics to pushgateway')

    def engine_started(self):
        self._start_prometheus_endpoint()

    def engine_stopped(self):
        self._persist_stats(self._stats, None)
        self._stop_prometheus_endpoint()

    # Default Metrics Handling
    def spider_opened(self, spider):
        if self.crawler.settings.getbool('PROMETHEUS_DEFAULT_METRICS', True):
            self.inc_value("spider_opened", spider=spider)

    def spider_closed(self, spider, reason):
        if self.crawler.settings.getbool('PROMETHEUS_DEFAULT_METRICS', True):
            self.inc_value("spider_closed", spider=spider)

    def item_scraped(self, item, spider):
        if self.crawler.settings.getbool('PROMETHEUS_DEFAULT_METRICS', True):
            self.inc_value("item_scraped", spider=spider)

    def response_received(self, spider):
        if self.crawler.settings.getbool('PROMETHEUS_DEFAULT_METRICS', True):
            self.inc_value("response_received", spider=spider)

    def item_dropped(self, item, spider, exception):
        if self.crawler.settings.getbool('PROMETHEUS_DEFAULT_METRICS', True):
            self.inc_value("item_dropped", spider=spider)
