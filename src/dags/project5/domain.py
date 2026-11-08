"""Pure validation and money rules, without Airflow dependencies."""
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation


def timestamp(value):
    # API timestamps are local business times without an offset.
    result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if result.tzinfo is not None:
        raise ValueError('Expected source business timestamp without timezone')
    return result


def window(end, start=None, stop=None):
    if bool(start) != bool(stop):
        raise ValueError('Provide both from and to, or neither')
    right = timestamp(stop or end)
    left = timestamp(start) if start else right - timedelta(days=7)
    if left >= right:
        raise ValueError('from must be earlier than to')
    return left, right


def money(value):
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError('Invalid monetary value')
    if not result.is_finite() or result < 0:
        raise ValueError('Money must be finite and nonnegative')
    if result != result.quantize(Decimal('0.01')):
        raise ValueError('Money has more than two decimal places')
    return result


def order_total(payload):
    # Mongo cost is authoritative; older source formats may only have items.
    if 'cost' in payload:
        return money(payload['cost'])
    # Never accept delivery API `sum` as an order amount.
    items = payload.get('order_items')
    if not isinstance(items, list) or not items:
        raise ValueError('Mongo order must have a nonempty order_items array')
    result = Decimal('0')
    for item in items:
        quantity = Decimal(str(item['quantity']))
        if not quantity.is_finite() or quantity <= 0 or quantity != int(quantity):
            raise ValueError('Order quantity must be a positive integer')
        result += money(item['price']) * quantity
    return money(result)


def payout(total, rating):
    for boundary, percent, minimum in [
        ('4', '0.05', '100'), ('4.5', '0.07', '150'), ('4.9', '0.08', '175')
    ]:
        if Decimal(str(rating)) < Decimal(boundary):
            return max(money(total) * Decimal(percent), Decimal(minimum))
    return max(money(total) * Decimal('0.10'), Decimal('200'))


def validate_courier(row):
    if not isinstance(row, dict) or not row.get('_id') or not row.get('name'):
        raise ValueError('Courier requires _id and name')


def validate_delivery(row):
    for field in ('delivery_id','order_id','courier_id','order_ts','delivery_ts','rate','tip_sum','address'):
        if field not in row or row[field] is None:
            raise ValueError('Delivery missing field: ' + field)
    if any(not row[k] for k in ('delivery_id','order_id','courier_id')):
        raise ValueError('Delivery IDs cannot be empty')
    if type(row['rate']) is not int or not 1 <= row['rate'] <= 5:
        raise ValueError('Rating must be integer 1..5')
    if timestamp(row['delivery_ts']) < timestamp(row['order_ts']):
        raise ValueError('Delivery precedes order')
    money(row['tip_sum'])
