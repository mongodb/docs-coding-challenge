Welcome
=======

Hopps is a simple document history-preserving key-value store built on
top of [MongoDB](https://www.mongodb.com) and
[Python](https://www.python.org).

Your task, should you chose to accept it, is to document the Hopps API
and structure.

Goals
=====

Fill in the file ``docs.txt`` with documentation covering ``hopps.py``.
Use your best judgment! It is deliberately vague and slim.

Setup
=====

To work with Hopps, you will need Python 3.4 or later. On OS X and
Windows, use the installer from
[Python.org](https://www.python.org/downloads/).

Next, you should set up a virtual environment. From within this
repository, run:

    pyvenv venv
    . venv/bin/activate
    pip install argon2 tornado motor docopt typing

Ensure that you have MongoDB 3.2 or later installed from the
[MongoDB Download Center](https://www.mongodb.com/download-center).

Start a MongoDB instance:

    mkdir data
    mongod --dbpath=`pwd`/data --logpath mongod.log --fork

``sample.py`` demonstrates use of ``hopps.py``, and ``cli.py`` provides
a simple prompt interface for saving and retrieving documents.

Start Hopps:

    ./sample.py

And in a new terminal, run the following:

    ./cli.py
    help
