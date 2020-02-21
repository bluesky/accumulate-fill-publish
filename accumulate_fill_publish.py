#!/usr/bin/env python
import argparse
from collections import deque
import functools
import sys

from databroker.core import discover_handlers
from event_model import DocumentRouter, Filler, RunRouter


print = functools.partial(print, file=sys.stderr)


class EmittingDocumentRouter(DocumentRouter):
    def __init__(self, *args, callback, **kwargs):
        self._callback = callback
        super().__init__(*args, **kwargs)

    def emit(self, name, doc):
        self._callback(name, doc)


class Accumulator(EmittingDocumentRouter):
    def __init__(self, callback):
        super().__init__(callback)
        self._cache = deque()

    def __call__(self, name, doc, validate=False):
        self._cache.append((name, doc))
        return super().__call__(name, doc, validate)

    def stop(self, doc):
        while self._cache:
            self.emit(*self._cache.popleft())


class EmittingFiller(EmittingDocumentRouter, Filler):
    def __call__(self, name, doc, validate=False):
        name, doc = super().__call__(name, doc, validate)
        self.emit(name, doc)


def main():
    from bluesky.callbacks.zmq import Publisher, RemoteDispatcher
    parser = argparse.ArgumentParser(
        description='Listen for unfilled documents over 0MQ and emit filled ones.')
    parser.add_argument(
        'receive_from', type=str,
        help="bluesky-0MQ-proxy out address, given as in localhost:5578")
    parser.add_argument(
        'send_to', type=str,
        help="bluesky-0MQ-proxy in address, given as in localhost:5578")
    args = parser.parse_args()

    # Data flows through:
    # * RemoteDispatcher (0MQ)
    # * Accumulator (caches until stop doc is received)
    # * EmittingFiller (fills external data)
    # * Publisher (0MQ)

    publisher = Publisher(args.send_to)

    handler_registry = discover_handlers()

    def factory(name, doc):
        filler = EmittingFiller(handler_registry, callback=publisher)
        accumulator = Accumulator(callback=filler)
        return [accumulator], []

    rr = RunRouter([factory])
    rd = RemoteDispatcher(args.receive_from)
    rd.subscribe(rr)

    print(f'Listening to {args.receive_from}')

    try:
        rd.start()  # runs forever
    except KeyboardInterrupt:
        print('Terminated by user; exiting')


if __name__ == '__main__':
    main()
