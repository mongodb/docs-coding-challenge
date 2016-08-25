#!/usr/bin/env python3
"""
Usage: hopps [--hostname=<hostname>]
             [--port=<port>]
             [--mongodb=<hostname>]

-h --help               show this
--hostname=<hostname>   host to listen on [default: localhost]
--port=<port>           port to listen on [default: 5919]
--mongodb=<hostname>    host to connect to [default: localhost:27017]
"""

import docopt
import tornado.ioloop
from typing import Iterable, Tuple
import hopps


def main():
    options = docopt.docopt(__doc__)
    hostname = options['--hostname']
    port = int(options['--port'])
    mongodb_hostname = options['--mongodb']

    connection = hopps.Connection('sample', hostname)
    tornado.ioloop.IOLoop.current().run_sync(connection.initialize)

    application = tornado.web.Application([
        (r'/', hopps.HoppsHandler)
    ], conn=connection)
    application.listen(port, address=hostname)

    print('Running!')
    tornado.ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    main()
