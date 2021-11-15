import logging
import json
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers, send_message


class bookingService(object):
    """
    booking Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('booking-service', [self.create_bookings,
                                                     self.update_booking,
                                                     self.delete_booking])

    @staticmethod
    def _create_entity(_cart_id, _status='CREATED'):
        """
        Create an booking entity.
        :param _cart_id: The cart ID the booking is for.
        :param _status: The current status of the booking, defaults to CREATED.
                        Other options are OUT_OF_STOCK, IN_STOCK, CLEARED, UNCLEARED, SHIPPED and DELIVERED.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'cart_id': _cart_id,
            'status': _status,
        }

    def start(self):
        logging.info('starting ...')
        self.event_store.subscribe('billing', self.billing_created)
        self.event_store.subscribe('billing', self.billing_deleted)
        self.event_store.subscribe('shipping', self.shipping_created)
        self.event_store.subscribe('shipping', self.shipping_updated)
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.event_store.unsubscribe('billing', self.billing_created)
        self.event_store.unsubscribe('billing', self.billing_deleted)
        self.event_store.unsubscribe('shipping', self.shipping_created)
        self.event_store.unsubscribe('shipping', self.shipping_updated)
        self.consumers.stop()
        logging.info('stopped.')

    def create_bookings(self, _req):
        bookings = _req if isinstance(_req, list) else [_req]
        booking_ids = []

        for booking in bookings:
            try:
                new_booking = bookingService._create_entity(booking['cart_id'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'cart_id'"
                }

            # trigger event
            self.event_store.publish('booking', create_event('entity_created', new_booking))

            booking_ids.append(new_booking['entity_id'])

        return {
            "result": booking_ids
        }

    def update_booking(self, _req):
        try:
            booking_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_entity', {'name': 'booking', 'id': booking_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        booking = rsp['result']
        if not booking:
            return {
                "error": "could not find booking"
            }

        # set new props
        booking['entity_id'] = booking_id
        try:
            booking['cart_id'] = _req['cart_id']
            booking['status'] = _req['status']
        except KeyError:
            return {
                "result": "missing mandatory parameter 'cart_id' and/or 'status"
            }

        # trigger event
        self.event_store.publish('booking', create_event('entity_updated', booking))

        return {
            "result": True
        }

    def delete_booking(self, _req):
        try:
            booking_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_entity', {'name': 'booking', 'id': booking_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        booking = rsp['result']
        if not booking:
            return {
                "error": "could not find booking"
            }

        # trigger event
        self.event_store.publish('booking', create_event('entity_deleted', booking))

        return {
            "result": True
        }

    def billing_created(self, _item):
        if _item.event_action != 'entity_created':
            return

        billing = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entity', {'name': 'booking', 'id': billing['booking_id']})
        booking = rsp['result']
        if not booking['status'] == 'IN_STOCK':
            return

        booking['status'] = 'CLEARED'
        self.event_store.publish('booking', create_event('entity_updated', booking))

    def billing_deleted(self, _item):
        if _item.event_action != 'entity_delted':
            return

        billing = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entity', {'name': 'booking', 'id': billing['booking_id']})
        booking = rsp['result']
        if not booking['status'] == 'CLEARED':
            return

        booking['status'] = 'UNCLEARED'
        self.event_store.publish('booking', create_event('entity_updated', booking))

    def shipping_created(self, _item):
        if _item.event_action != 'entity_created':
            return

        shipping = json.loads(_item.event_data)
        rsp = send_message('read-model', 'get_entity', {'name': 'booking', 'id': shipping['booking_id']})
        booking = rsp['result']
        if not booking['status'] == 'CLEARED':
            return

        booking['status'] = 'SHIPPED'
        self.event_store.publish('booking', create_event('entity_updated', booking))

    def shipping_updated(self, _item):
        if _item.event_action != 'entity_updated':
            return

        shipping = json.loads(_item.event_data)
        if not shipping['delivered']:
            return

        rsp = send_message('read-model', 'get_entity', {'name': 'booking', 'id': shipping['booking_id']})
        booking = rsp['result']
        booking['status'] = 'DELIVERED'
        self.event_store.publish('booking', create_event('entity_updated', booking))


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

b = bookingService()

signal.signal(signal.SIGINT, lambda n, h: o.stop())
signal.signal(signal.SIGTERM, lambda n, h: o.stop())

b.start()
