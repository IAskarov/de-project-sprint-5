import sys
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'/'dags'))
from project5.api import DeliveryAPI
from project5.domain import order_total,payout,money,window,validate_delivery
from project5.tasks import covers


def delivery(i, **updates):
    row = dict(delivery_id=str(i),order_id='o'+str(i),courier_id='c',
               order_ts='2026-10-31 23:59:00',delivery_ts='2026-11-01 00:30:00',
               address='address',rate=5,tip_sum=10,sum=999999)
    row.update(updates)
    return row


class Response:
    def __init__(self,rows): self.rows=rows
    def raise_for_status(self): pass
    def json(self): return self.rows


class Session:
    def __init__(self,pages): self.pages=iter(pages); self.calls=[]
    def get(self,url,**kwargs):
        self.calls.append((url,kwargs)); return Response(next(self.pages))


class RulesTests(unittest.TestCase):
    def test_thresholds(self):
        for rating,expected in [('3.99','500'),('4','700'),('4.499999','700'),
                                ('4.5','800'),('4.899999','800'),('4.9','1000'),('5','1000')]:
            with self.subTest(rating=rating):
                self.assertEqual(payout('10000',rating),Decimal(expected))

    def test_minimum_per_order(self):
        total=order_total({'order_items':[{'price':600,'quantity':1},{'price':400,'quantity':1}]})
        self.assertEqual(total,1000)
        self.assertEqual(payout(total,5),200)

    def test_quantities_and_equal_price_lines(self):
        self.assertEqual(order_total({'order_items':[{'price':100,'quantity':2},{'price':100,'quantity':3}]}),500)

    def test_mongo_cost_is_authoritative(self):
        self.assertEqual(order_total({'cost':'1200.50','order_items':[{'price':1000,'quantity':1}]}),Decimal('1200.50'))

    def test_no_api_amount_fallback(self):
        with self.assertRaises(ValueError): order_total({'sum':123})

    def test_bad_money(self):
        for value in ['NaN','Infinity','-1','1.001']:
            with self.assertRaises(ValueError): money(value)

    def test_bad_quantity(self):
        for q in [0,-1,'NaN',1.5]:
            with self.assertRaises(ValueError): order_total({'order_items':[{'price':10,'quantity':q}]})

    def test_seven_days_and_year_boundary(self):
        left,right=window('2027-01-03 00:00:00')
        self.assertEqual(left,datetime(2026,12,27)); self.assertEqual(right,datetime(2027,1,3))

    def test_window_override(self):
        a,b=window('2026-11-08 00:00:00','2026-10-01 00:00:00','2026-11-08 00:00:00')
        self.assertEqual((b-a).days,38)
        with self.assertRaises(ValueError): window('2026-11-08 00:00:00',start='2026-10-01')

    def test_coverage(self):
        d=lambda n:datetime(2026,10,n)
        self.assertTrue(covers([(d(1),d(15)),(d(15),d(31))],d(1),d(31)))
        self.assertFalse(covers([(d(1),d(15)),(d(16),d(31))],d(1),d(31)))

    def test_validation(self):
        validate_delivery(delivery(1))
        with self.assertRaises(ValueError): validate_delivery(delivery(1,rate=5.0))
        with self.assertRaises(ValueError): validate_delivery(delivery(1,tip_sum=-1))


class PagingTests(unittest.TestCase):
    def api(self,pages):
        session=Session(pages)
        return DeliveryAPI('https://example.test',{},session),session

    def test_all_pages(self):
        api,s=self.api([[delivery(i) for i in range(50)],[delivery(50)]])
        rows=[r for p in api.pages('deliveries',datetime(2026,11,1),datetime(2026,11,2)) for r in p]
        self.assertEqual(len(rows),51)
        self.assertEqual([call[1]['params']['offset'] for call in s.calls],[0,50])
        self.assertEqual(s.calls[0][1]['params']['sort_field'],'_id')
        self.assertEqual(s.calls[0][1]['params']['from'],'1970-01-01 00:00:00')

    def test_exact_page_then_empty(self):
        api,s=self.api([[{'_id':str(i),'name':'Courier'} for i in range(50)],[]])
        self.assertEqual(sum(len(p) for p in api.pages('couriers')),50)
        self.assertEqual(len(s.calls),2)
        self.assertEqual(s.calls[0][1]['params']['sort_field'],'id')

    def test_page_drift(self):
        api,s=self.api([[delivery(i) for i in range(50)],[delivery(0)]])
        with self.assertRaises(ValueError): list(api.pages('deliveries',datetime(2026,11,1),datetime(2026,11,2)))

    def test_exclusive_right_boundary(self):
        api,s=self.api([[delivery(1)]])
        self.assertEqual(list(api.pages('deliveries',datetime(2026,10,31),datetime(2026,11,1,0,30))),[])

    def test_empty_filtered_page_does_not_stop_paging(self):
        old=[delivery(i,order_ts='2026-10-01 12:00:00',delivery_ts='2026-10-01 13:00:00') for i in range(50)]
        api,s=self.api([old,[delivery(50)]])
        result=[r for page in api.pages('deliveries',datetime(2026,11,1),datetime(2026,11,2)) for r in page]
        self.assertEqual([r['delivery_id'] for r in result],['50'])
        self.assertEqual(s.calls[1][1]['params']['offset'],50)

    def test_delivery_window_uses_delivery_date_with_both_boundaries(self):
        rows=[delivery(1,order_ts='2026-10-31 22:00:00',delivery_ts='2026-11-01 00:00:00'),
              delivery(2,delivery_ts='2026-11-02 00:00:00'),
              delivery(3,order_ts='2026-10-30 12:00:00',delivery_ts='2026-10-31 23:59:59')]
        api,s=self.api([rows])
        result=[r for page in api.pages('deliveries',datetime(2026,11,1),datetime(2026,11,2)) for r in page]
        self.assertEqual([r['delivery_id'] for r in result],['1'])

    def test_error_payload(self):
        api,s=self.api([{'error':'Access denied'}])
        with self.assertRaises(ValueError): list(api.pages('couriers'))


if __name__=='__main__': unittest.main()
