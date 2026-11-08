"""Paginated API client. Headers are never logged."""
import logging
from project5.domain import timestamp, validate_courier, validate_delivery


class DeliveryAPI:
    def __init__(self, base_url, headers, session=None):
        if session is None:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            session = requests.Session()
            retry = Retry(total=4, backoff_factor=1, status_forcelist=[429,500,502,503,504],
                          allowed_methods=['GET'])
            session.mount('https://', HTTPAdapter(max_retries=retry))
        self.session = session
        self.base = base_url.rstrip('/')
        self.headers = headers

    def pages(self, entity, start=None, stop=None):
        params = {'sort_field': 'id' if entity == 'couriers' else '_id',
                  'sort_direction': 'asc', 'limit': 50, 'offset': 0}
        if entity not in ('couriers', 'deliveries'):
            raise ValueError('Unknown entity')
        if entity == 'deliveries':
            if start is None or stop is None:
                raise ValueError('Deliveries require a fixed date window')
            # The training API filters on order_ts despite its delivery_ts specification.
            # No finite lower order bound can guarantee inclusion of late deliveries.
            # Epoch..stop is a safe superset under either API interpretation because
            # validate_delivery checks order_ts <= delivery_ts. Filter locally below.
            params.update({'from': '1970-01-01 00:00:00',
                           'to': stop.strftime('%Y-%m-%d %H:%M:%S')})
        seen = set()
        while True:
            response = self.session.get(self.base + '/' + entity, headers=self.headers,
                                        params=dict(params), timeout=(15,90))
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list) or len(rows) > 50:
                raise ValueError('API must return an array with at most 50 objects')
            for row in rows:
                (validate_courier if entity == 'couriers' else validate_delivery)(row)
                key = row['_id'] if entity == 'couriers' else row['delivery_id']
                if key in seen:
                    raise ValueError('Repeated API key; pagination changed during extraction')
                seen.add(key)
            selected = rows if entity == 'couriers' else [
                row for row in rows if start <= timestamp(row['delivery_ts']) < stop
            ]
            if selected:
                yield selected
            if entity == 'deliveries' and params['offset'] % 500 == 0:
                logging.getLogger(__name__).info('API offset=%s: read=%s, selected=%s',
                    params['offset'],len(rows),len(selected))
            # offset and completion always use the RAW page, even if selected is empty.
            if len(rows) < 50:
                return
            params['offset'] += len(rows)
