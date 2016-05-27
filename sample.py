"""
Usage: hopps [--username=<username>]
             [--hostname=<hostname>]
             [--port=<port>]
             [--mongodb=<hostname>]
             [--admin]

-h --help               show this
--hostname=<hostname>   host to listen on [default: localhost]
--port=<port>           port to listen on [default: 5919]
--mongodb=<hostname>    host to connect to [default: localhost:27017]
--admin                 bypass authentication checks, but only allow
                        user modification.
"""

import docopt
import tornado.ioloop
from typing import Iterable, Tuple
import hopps


class NoSecurityHandler(hopps.HoppsHandler):
    async def require(self, roles: Iterable[hopps.Role]) -> None:
        # In admin mode, only allow user modification
        if self.admin_mode:
            if list(roles) != [Role.users]:
                raise AccessDenied(None, roles)
            return

    async def handle_auth(self, message_id: object, args: Tuple[object, object]) -> None:
        username, password = args
        if await self.conn.authenticate(str(username), str(password)):
            self.user = username
            return

        raise Exception('bad-auth')


def main():
    options = docopt.docopt(__doc__)
    hostname = options['--hostname']
    port = int(options['--port'])
    mongodb_hostname = options['--mongodb']
    admin_mode = bool(options['--admin'])

    connection = hopps.Connection('sample', hostname)
    tornado.ioloop.IOLoop.current().run_sync(connection.initialize)

    application = tornado.web.Application([
        (r'/', NoSecurityHandler)
    ], conn=connection, admin_mode=admin_mode)
    application.listen(port, address=hostname)

    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()
