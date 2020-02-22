#!/usr/bin/env python
import argparse
from collections import deque
import functools
import sys
import uuid
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
        super().__init__(callback=callback)
        self._cache = deque()

    def __call__(self, name, doc, validate=False):
        self._cache.append((name, doc))
        return super().__call__(name, doc, validate)

    def stop(self, doc):
        print(f"Run {doc['run_start']} is complete.")
        while self._cache:
            self.emit(*self._cache.popleft())


class FilledMigrator(EmittingDocumentRouter):
    def __init__(self, *args, catalog, **kwargs):
        self._catalog = catalog
        super().__init__(*args, **kwargs)

    def start(self, doc):
        self.emit("start", doc)
        self._run_start_uid = doc["uid"]
        # Emit a 'dark' descriptor and event. Same for 'background'.
        background_scan_id = doc.get("bkg_scan_id", None)
        if background_scan_id:
            background_run = self._catalog[background_scan_id]
            self._re_emit(background_run, "flat")

        dark_scan_id = doc.get("dark_scan_id", None)
        if dark_scan_id:
            dark_run = self._catalog[dark_scan_id]
            self._re_emit(dark_run, "dark")

    def __call__(self, name, doc):
        super().__call__(name, doc)
        if name != "start":
            self.emit(name, doc)

    def _re_emit(self, run, stream_name):
        new_uids = {}
        descriptors = run.primary.metadata["descriptors"]
        for desc in descriptors:
            desc = desc.copy()
            desc["run_start"] = self._run_start_uid
            new_uid = str(uuid.uuid4())
            new_uids[desc["uid"]] = new_uid
            desc["uid"] = new_uid
            desc["name"] = stream_name
            self.emit("descriptor", desc)
        for name, doc in run.canonical(fill="yes"):
            if name in ("event", "event_page") and doc["descriptor"] in new_uids:
                doc = dict(doc)
                doc["descriptor"] = new_uids[doc["descriptor"]]
                if name == "event":
                    doc["uid"] = str(uuid.uuid4())
                else:
                    doc["uid"] = [uuid.uuid4() for _ in doc["uid"]]
                self.emit(name, doc)


class EmittingFiller(EmittingDocumentRouter, Filler):
    def __call__(self, name, doc, validate=False):
        name, doc = super().__call__(name, doc, validate)
        self.emit(name, doc)


def main():
    from bluesky.callbacks.zmq import Publisher, RemoteDispatcher

    parser = argparse.ArgumentParser(
        description="Listen for unfilled documents over 0MQ and emit filled ones."
    )
    parser.add_argument(
        "receive_from",
        type=str,
        help="bluesky-0MQ-proxy out address, given as in localhost:5578",
    )
    parser.add_argument(
        "send_to",
        type=str,
        help="bluesky-0MQ-proxy in address, given as in localhost:5578",
    )
    args = parser.parse_args()

    # Data flows through:
    # * RemoteDispatcher (0MQ)
    # * Accumulator (caches until stop doc is received)
    # * EmittingFiller (fills external data)
    # * Publisher (0MQ)

    publisher = Publisher(args.send_to)

    handler_registry = discover_handlers()

    def factory(name, doc):
        filler = EmittingFiller(
            handler_registry, inplace=False, callback=publisher, coerce="force_numpy"
        )
        accumulator = Accumulator(callback=filler)
        return [accumulator], []

    rr = RunRouter([factory])
    rd = RemoteDispatcher(args.receive_from)
    rd.subscribe(rr)

    print(f"Listening to {args.receive_from}")

    try:
        rd.start()  # runs forever
    except KeyboardInterrupt:
        print("Terminated by user; exiting")


if __name__ == "__main__":
    main()
