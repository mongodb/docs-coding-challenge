#!/usr/bin/env python3
"""
Usage: hopps-cli [<uri>]

-h --help   show this
<uri>       hopps server to connect to [default: ws://localhost:5919]
"""

try:
    import readline
except ImportError:
    pass

import collections
import concurrent
import json
import logging
import shlex
from typing import Dict, List

import docopt
import tornado.concurrent
import tornado.gen
import tornado.ioloop
import tornado.websocket

logger = logging.getLogger(__name__)


class StopException(Exception):
    pass


class ClosedException(StopException):
    pass


class HoppsConnection:
    def __init__(self, host: str) -> None:
        self.message_id = 0
        self.host = host
        self.conn = None  # type: tornado.websocket.WebSocketClientConnection
        self.pending = {}
        self.messages = collections.deque()

    @tornado.gen.coroutine
    def connect(self) -> 'HoppsConnection':
        self.conn = yield tornado.websocket.websocket_connect(self.host,
                                                              on_message_callback=self._on_message)
        return self

    @tornado.gen.coroutine
    def save(self, collection: str, doc: Dict[str, object]) -> None:
        yield self.__send({'save': [collection, doc]})

    @tornado.gen.coroutine
    def get(self, collection: str, docid: str) -> None:
        yield self.__send({'get': [collection, docid]})

    @tornado.gen.coroutine
    def auth(self, username: str, password: str) -> None:
        yield self.__send({'auth': [username, password]})

    @tornado.gen.coroutine
    def create_user(self, username: str, password: str, roles: List[str]) -> None:
        yield self.__send({'create-user': [username, password, roles]})

    @tornado.gen.coroutine
    def revoke_user(self, username: str) -> None:
        yield self.__send({'revoke-user': [username]})

    @tornado.gen.coroutine
    def list_users(self) -> None:
        yield self.__send({'list-users': []})

    @tornado.gen.coroutine
    def __send(self, msg: Dict[str, object]):
        self.message_id += 1
        msg['i'] = self.message_id
        yield self.conn.write_message(json.dumps(msg))

    @tornado.gen.coroutine
    def _on_message(self, message: str) -> None:
        if message is None:

            self.on_close()
        else:
            self.on_message(message)

    @tornado.gen.coroutine
    def on_close(self) -> None:
        self.messages.append(None)

    def on_message(self, message: str) -> None:
        self.messages.append(message)

    def pop(self) -> str:
        return self.messages.popleft()


class Client:
    executor = concurrent.futures.ThreadPoolExecutor()

    def __init__(self, host: str) -> None:
        self.host = host
        self.conn = None  # type: HoppsConnection

    @tornado.gen.coroutine
    def handle_help(self):
        """help()"""
        props = [p for p in dir(self) if p.startswith('handle_')]
        for attr_name in props:
            attr = getattr(self, attr_name)
            print('  {}'.format(attr.__doc__))

    @tornado.gen.coroutine
    def handle_quit(self):
        """quit()"""
        raise StopException()

    @tornado.gen.coroutine
    def handle_exit(self):
        """exit()"""
        raise StopException()

    @tornado.gen.coroutine
    def handle_save(self, collection, raw_doc: str):
        """save(collection, doc)"""
        doc = json.loads(raw_doc)
        yield self.conn.save(collection, doc)

    @tornado.gen.coroutine
    def handle_get(self, collection, docid: str):
        """get(collection, docid)"""
        yield self.conn.get(collection, docid)

    @tornado.gen.coroutine
    def handle_auth(self, username: str, password: str):
        """auth(username, password)"""
        yield self.conn.auth(username, password)

    @tornado.gen.coroutine
    def handle_create_user(self, username: str, password: str, roles: str):
        """create_user(username, password, roles)"""
        yield self.conn.create_user(username, password, roles.split())

    @tornado.gen.coroutine
    def handle_revoke_user(self, username: str):
        """revoke_user(username)"""
        yield self.conn.revoke_user(username)

    @tornado.gen.coroutine
    def handle_list_users(self):
        """list_users()"""
        yield self.conn.list_users()

    @tornado.gen.coroutine
    def _prompt(self):
        while self.conn.messages:
            message = self.conn.pop()
            if message is None:
                raise ClosedException()

            print('  remote> {}'.format(message))

        raw_stanza = yield self._input()
        parts = shlex.split(raw_stanza)
        if not parts:
            return

        command = parts[0]
        try:
            yield getattr(self, 'handle_{}'.format(command))(*parts[1:])
        except AttributeError:
            logger.error('Unknown method "%s"', command)
        except TypeError as err:
            logger.error(err)

    @tornado.gen.coroutine
    def mainloop(self):
        self.conn = yield HoppsConnection(self.host).connect()

        try:
            while True:
                yield self._prompt()
        except ClosedException:
            print('Host closed connection')
        except StopException:
            pass
        except KeyboardInterrupt:
            pass
        except EOFError:
            print('')

    @tornado.concurrent.run_on_executor
    def _input(self):
        return input('> ')


def main():
    options = docopt.docopt(__doc__)
    host = str(dict(options).get('uri', 'ws://localhost:5919'))
    logging.basicConfig(level=logging.WARNING)

    client = Client(host)
    tornado.ioloop.IOLoop.current().run_sync(client.mainloop)


if __name__ == '__main__':
    main()
