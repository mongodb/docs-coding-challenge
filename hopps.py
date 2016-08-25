#!/usr/bin/env python3
import base64
import enum
import json
import logging
import os
from typing import Dict, List, Tuple, Iterable

import tornado.gen
import tornado.web
import tornado.websocket
import pymongo.errors
import motor.motor_tornado

logger = logging.getLogger(__name__)


class HoppsError(Exception):
    code = 'unknown'

    def errdoc(self) -> object:
        return None


class PrimaryKeyViolation(HoppsError):
    code = 'pkey'


class LineageException(HoppsError):
    code = 'lineage'

    def __init__(self, diff: List[Dict[str, object]]) -> None:
        super(LineageException, self).__init__('')
        self.diff = diff

    def errdoc(self) -> object:
        return {'lineage': self.diff}


class NotFound(HoppsError):
    code = 'notfound'


class InvalidDocument(HoppsError):
    code = 'invalid'

    def __init__(self, description: str, doc: Dict[str, object]) -> None:
        super(InvalidDocument, self).__init__(description)
        self.doc = doc

    def errdoc(self) -> object:
        return {'doc': self.doc, 'msg': str(self)}


def lineage_delta(history: Dict[str, Dict[str, object]],
                  ancestor_rev: str,
                  leaf_rev: str) -> List[Dict[str, object]]:
    delta = []  # type: List[Dict[str, object]]
    leaf = history[leaf_rev]
    while leaf['_rev'] != ancestor_rev:
        delta.append(leaf)
        leaf = history[leaf['_parent']]
    delta.append(history[ancestor_rev])
    delta = delta[::-1]
    return delta


def generate_revision() -> str:
    return str(base64.b16encode(os.urandom(16)), 'utf-8')


class Connection:
    MAX_HISTORY = 25

    def __init__(self, name: str, connection_uri: str, logSizeBytes: int=100*1024*1024) -> None:
        self.name = name
        self.conn = motor.motor_tornado.MotorClient(connection_uri)
        self.db = self.conn['hopps_{}'.format(name)]
        self.commit_collection = None
        self.security_collection = self.db['security']

        self.__ready = self.db.create_collection('committed',
                                                 capped=True,
                                                 size=logSizeBytes)

    @tornado.gen.coroutine
    def initialize(self) -> None:
        """Wait until all collections and indexes are ready."""
        if not self.__ready:
            return

        try:
            yield self.__ready
            self.__ready = None
        except pymongo.errors.CollectionInvalid:
            self.__ready = None

        self.commit_collection = self.db['committed']

    @tornado.gen.coroutine
    def save(self, collection: str, doc: Dict[str, object]) -> str:
        if '_id' not in doc:
            raise InvalidDocument('No _id', doc)

        coll = self.db[collection]
        history = yield coll.find_one({'_id': doc['_id']})

        if history is None:
            if '_rev' in doc:
                raise InvalidDocument('Document has revision, but no known parent', doc)

            rev = generate_revision()
            doc['_rev'] = rev
            doc['_parent'] = None
            yield coll.insert({'_id': doc['_id'],
                               'i': 0,
                               'revs': {rev: doc},
                               'leaf': doc}, w='majority')
            yield self._commit(collection, doc)

            return rev

        if '_rev' not in doc:
            raise InvalidDocument('Missing revision', doc)

        # Now check if our parent is the leaf
        leaf_rev = history['leaf']['_rev']
        if doc['_rev'] != leaf_rev:
            raise LineageException(lineage_delta(history['revs'], doc['_rev'], leaf_rev))

        # If there are too many elements in our revision history, cull
        # the oldest to prevent histories from growing monotonically
        # (until the document exceeds the max BSON size)
        to_remove = dict([('revs.{}'.format(x), '') for x in self.prune(history['revs'])])

        rev = generate_revision()
        doc['_parent'] = doc['_rev']
        doc['_rev'] = rev
        update_document = {'$inc': {'i': 1},
                           '$set': {'revs.{}'.format(rev): doc,
                                    'leaf': doc}}
        if to_remove:
            update_document['$unset'] = to_remove

        yield coll.update({'_id': doc['_id'],
                           'i': history['i']},
                          update_document,
                          w='majority')
        yield self._commit(collection, doc)

        return rev

    @tornado.gen.coroutine
    def get(self, collection: str, docid: str) -> object:
        coll = self.db[collection]
        history = yield coll.find_one({'_id': docid},
                                      {'leaf': True})
        if history is None:
            raise NotFound(docid)

        return history['leaf']

    @tornado.gen.coroutine
    def _commit(self, collection: str, doc: Dict[str, object]) -> None:
        """Apply the given document revision to the replication log."""
        yield self.commit_collection.insert({'coll': collection,
                                             'doc': doc}, w='majority')

    @classmethod
    def prune(cls, revs: Dict[str, Dict[str, object]]) -> List[str]:
        if len(revs) > cls.MAX_HISTORY:
            return [rev for rev in revs.values() if not revs[rev['_id']]][0]

        return []


class HoppsHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        self.origin = None
        self.conn = self.settings['conn']  # type: HoppsConnection
        self.closed = False

    def check_origin(self, origin: str) -> bool:
        """WebSocket origin check. See
           http://www.tornadoweb.org/en/stable/websocket.html#tornado.websocket.WebSocketHandler.check_origin"""
        self.origin = origin
        return True

    @tornado.gen.coroutine
    def on_message(self, message):
        parsed = json.loads(message, 'utf-8')
        message_id = parsed['i']
        del parsed['i']

        result = None

        try:
            method = list(parsed.keys())[0]
            args = parsed[method]

            if method == 'save':
                result = yield self.handle_save(message_id, args)
            elif method == 'get':
                result = yield self.handle_get(message_id, args)
            elif method == 'watch':
                result = yield self.handle_watch(message_id, args)
            elif method == 'dump':
                result = yield self.handle_dump(message_id, args)
            else:
                raise ValueError('Unknown method: "{}"'.format(method))
        except HoppsError as err:
            yield self._respond(message_id, err.code, err.errdoc())
            return
        except Exception as err:
            self.log(logger.exception, err)
            yield self._respond(message_id, 'error', None)
            return

        if not self.closed:
            yield self._respond(message_id, 'ok', result)

    def on_close(self):
        self.closed = True

    @tornado.gen.coroutine
    def handle_save(self,
                    message_id: object,
                    args: Tuple[str, Dict[str, object]]) -> None:
        collection, doc = args

        yield self.conn.save(collection, doc)

    @tornado.gen.coroutine
    def handle_get(self, message_id: object, args: str) -> None:
        collection, docid = args

        return (yield self.conn.get(collection, str(docid)))

    def log(self, handler, message, *args) -> None:
        handler('%s: {0}'.format(message), self.origin, *args)

    @tornado.gen.coroutine
    def _respond(self, message_id: object, status: str, doc: object) -> None:
        response = {'i': message_id, 'status': status}
        if doc is not None:
            response['doc'] = doc

        yield self.write_message(json.dumps(response))
