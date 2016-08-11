#!/usr/bin/env python3
import base64
import concurrent.futures
import enum
import json
import logging
import os
from typing import Dict, List, Tuple, Iterable

import argon2
import tornado.gen
import tornado.web
import tornado.websocket
import pymongo.errors
import motor.motor_tornado

logger = logging.getLogger(__name__)


@enum.unique
class Role(enum.Enum):
    """Represents a user's permission level."""
    users = 'users'
    read = 'read'
    write = 'write'


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


class AccessDenied(HoppsError):
    code = 'denied'


class UserRevoked(AccessDenied):
    pass


class BadPassword(HoppsError):
    code = 'invalid'


class InvalidDocument(HoppsError):
    code = 'invalid'

    def __init__(self, description: str, doc: Dict[str, object]) -> None:
        super(InvalidDocument, self).__init__(description)
        self.doc = doc

    def errdoc(self) -> object:
        return {'doc': self.doc, 'msg': str(self)}


# https://codahale.com/a-lesson-in-timing-attacks/
def is_equal(a: bytes, b: bytes) -> bool:
    """Constant-time comparison."""
    if len(a) != len(b):
        return False

    result = 0
    for x, y in zip(a, b):
        result |= x ^ y

    return result == 0


def check_password(password: str) -> None:
    """Ensure that a password meets a minimal standard of quality."""
    if len(password) < 8:
        raise BadPassword('Too short')


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
    executor = concurrent.futures.ThreadPoolExecutor()

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
        yield self.security_collection.ensure_index('username')

    @tornado.gen.coroutine
    def get_roles(self, username: str) -> List[str]:
        """List the roles associated with a given username."""
        doc = yield self.security_collection.find_one({'username': username})
        if doc is None:
            return []

        return [Role[s] for s in doc['roles']]

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
    def authenticate(self, username: str, password: str) -> bool:
        doc = yield self.security_collection.find_one({'username': username})
        if not doc:
            return False

        try:
            hashed = yield self._hash_password(password, doc['salt'])
            return is_equal(hashed, doc['password'])
        except KeyError:
            raise UserRevoked(username)

    @tornado.gen.coroutine
    def create_user(self, username: str, password: str, roles: List[str]) -> None:
        check_password(password)

        salt = os.urandom(16)
        hashed = yield self._hash_password(password, salt)

        yield self.security_collection.update({'username': username},
                                              {'$set': {'password': hashed,
                                                        'salt': salt,
                                                        'roles': roles}},
                                              upsert=True,
                                              w='majority')

    @tornado.gen.coroutine
    def list_users(self) -> List[str]:
        cursor = self.security_collection.find({}, {'username': True})
        user_documents = yield cursor.to_list(100)
        return [doc['username'] for doc in user_documents]

    @tornado.gen.coroutine
    def revoke_user(self, username: str) -> None:
        yield self.security_collection.update({'username': username},
                                              {'$unset': {'password': 1, 'salt': 1,
                                               '$set': {'roles': []}}})

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

    @tornado.concurrent.run_on_executor
    def _hash_password(self, password: str, salt: bytes):
        return argon2.argon2_hash(password, salt)


class HoppsHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        self.origin = None
        self.conn = self.settings['conn']  # type: HoppsConnection
        self.admin_mode = self.settings['admin_mode']  # type: bool
        self.closed = False
        self.user = None

    def check_origin(self, origin: str) -> bool:
        """WebSocket origin check. See
           http://www.tornadoweb.org/en/stable/websocket.html#tornado.websocket.WebSocketHandler.check_origin"""
        self.origin = origin
        return True

    @tornado.gen.coroutine
    def require(self, roles: Iterable[Role]) -> None:
        # In admin mode, only allow user modification
        if self.admin_mode:
            if list(roles) != [Role.users]:
                raise AccessDenied(None, roles)
            return

        if self.user is None:
            raise AccessDenied(None, roles)

        user_roles = set((yield self.conn.get_roles(self.user)))

        if not user_roles.issuperset(set(roles)):
            raise AccessDenied(self.user, roles)

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
            elif method == 'auth':
                result = yield self.handle_auth(message_id, args)
            elif method == 'create-user':
                result = yield self.handle_create_user(message_id, args)
            elif method == 'revoke-user':
                result = yield self.handle_revoke_user(message_id, args)
            elif method == 'list-users':
                result = yield self.handle_list_users(message_id)
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
    def handle_auth(self, message_id: object, args: Tuple[object, object]) -> None:
        username, password = args
        if (yield self.conn.authenticate(str(username), str(password))):
            self.user = username
            return

        raise Exception('bad-auth')

    @tornado.gen.coroutine
    def handle_create_user(self,
                           message_id: object,
                           args: Tuple[object, object, object]) -> None:
        yield self.require([Role.users])
        username, password, roles = args
        roles = [str(role) for role in roles]
        yield self.conn.create_user(str(username), str(password), roles)

    @tornado.gen.coroutine
    def handle_list_users(self, message_id: object):
        yield self.require([Role.users])
        return (yield self.conn.list_users())

    @tornado.gen.coroutine
    def handle_revoke_user(self, message_id: object, args: Tuple[object, object]) -> None:
        yield self.require([Role.users])
        username, = args
        yield self.conn.revoke_user(str(username))

    @tornado.gen.coroutine
    def handle_save(self,
                    message_id: object,
                    args: Tuple[str, Dict[str, object]]) -> None:
        yield self.require([Role.write])
        collection, doc = args

        yield self.conn.save(collection, doc)

    @tornado.gen.coroutine
    def handle_get(self, message_id: object, args: str) -> None:
        yield self.require([Role.read])
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
