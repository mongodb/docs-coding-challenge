#!/usr/bin/env python3.5
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

    async def connect(self) -> 'HoppsConnection':
        self.conn = await tornado.websocket.websocket_connect(self.host,
                                                              on_message_callback=self._on_message)
        return self

    async def save(self, collection: str, doc: Dict[str, object]) -> None:
        await self.__send({'save': [collection, doc]})

    async def get(self, collection: str, docid: str) -> None:
        await self.__send({'get': [collection, docid]})

    async def auth(self, username: str, password: str) -> None:
        await self.__send({'auth': [username, password]})

    async def create_user(self, username: str, password: str, roles: List[str]) -> None:
        await self.__send({'create-user': [username, password, roles]})

    async def revoke_user(self, username: str) -> None:
        await self.__send({'revoke-user': [username]})

    async def list_users(self) -> None:
        await self.__send({'list-users': []})

    async def __send(self, msg: Dict[str, object]):
        self.message_id += 1
        msg['i'] = self.message_id
        await self.conn.write_message(json.dumps(msg))

    def _on_message(self, message: str) -> None:
        if message is None:

            self.on_close()
        else:
            self.on_message(message)

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

    async def handle_help(self):
        """help()"""
        props = [p for p in dir(self) if p.startswith('handle_')]
        for attr_name in props:
            attr = getattr(self, attr_name)
            print('  {}'.format(attr.__doc__))

    async def handle_quit(self):
        """quit()"""
        raise StopException()

    async def handle_exit(self):
        """exit()"""
        raise StopException()

    async def handle_save(self, collection, raw_doc: str):
        """save(collection, doc)"""
        doc = json.loads(raw_doc)
        await self.conn.save(collection, doc)

    async def handle_get(self, collection, docid: str):
        """get(collection, docid)"""
        await self.conn.get(collection, docid)

    async def handle_auth(self, username: str, password: str):
        """auth(username, password)"""
        await self.conn.auth(username, password)

    async def handle_create_user(self, username: str, password: str, roles: str):
        """create_user(username, password, roles)"""
        await self.conn.create_user(username, password, roles.split())

    async def handle_revoke_user(self, username: str):
        """revoke_user(username)"""
        await self.conn.revoke_user(username)

    async def handle_list_users(self):
        """list_users()"""
        await self.conn.list_users()

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

    async def mainloop(self):
        self.conn = await HoppsConnection(self.host).connect()

        try:
            while True:
                await self._prompt()
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
