import logging
import signal
import uuid

from event_store.event_store_client import EventStoreClient, create_event
from message_queue.message_queue_client import Consumers, send_message


class MenuService(object):
    """
    Menu Service class.
    """

    def __init__(self):
        self.event_store = EventStoreClient()
        self.consumers = Consumers('menu-service', [self.create_menus,
                                                       self.update_menu,
                                                       self.delete_menu])

    @staticmethod
    def _create_entity(_name, _price):
        """
        Create a menu entity.
        :param _name: The name of the menu.
        :param _price: The price of the menu.
        :return: A dict with the entity properties.
        """
        return {
            'entity_id': str(uuid.uuid4()),
            'name': _name,
            'price': _price
        }

    def start(self):
        logging.info('starting ...')
        self.consumers.start()
        self.consumers.wait()

    def stop(self):
        self.consumers.stop()
        logging.info('stopped.')

    def create_menus(self, _req):
        menus = _req if isinstance(_req, list) else [_req]
        menu_ids = []

        for menu in menus:
            try:
                new_menu = menuService._create_entity(menu['name'], menu['price'])
            except KeyError:
                return {
                    "error": "missing mandatory parameter 'name' and/or 'price'"
                }

            # trigger event
            self.event_store.publish('menu', create_event('entity_created', new_menu))

            menu_ids.append(new_menu['entity_id'])

        return {
            "result": menu_ids
        }

    def update_menu(self, _req):
        try:
            menu_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_entity', {'name': 'menu', 'id': menu_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        menu = rsp['result']
        if not menu:
            return {
                "error": "could not find menu"
            }

        # set new props
        menu['entity_id'] = menu_id
        try:
            menu['name'] = _req['name']
            menu['price'] = _req['price']
        except KeyError:
            return {
                "result": "missing mandatory parameter 'name' and/or 'price"
            }

        # trigger event
        self.event_store.publish('menu', create_event('entity_updated', menu))

        return {
            "result": True
        }

    def delete_menu(self, _req):
        try:
            menu_id = _req['entity_id']
        except KeyError:
            return {
                "error": "missing mandatory parameter 'entity_id'"
            }

        rsp = send_message('read-model', 'get_entity', {'name': 'menu', 'id': menu_id})
        if 'error' in rsp:
            rsp['error'] += ' (from read-model)'
            return rsp

        menu = rsp['result']
        if not menu:
            return {
                "error": "could not find menu"
            }

        # trigger event
        self.event_store.publish('menu', create_event('entity_deleted', menu))

        return {
            "result": True
        }


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)-6s] %(message)s')

p = menuService()

signal.signal(signal.SIGINT, lambda n, h: p.stop())
signal.signal(signal.SIGTERM, lambda n, h: p.stop())

p.start()
