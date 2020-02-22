# Accumulate Fill Publish

This consumes a stream of bluesky documents and caches them in memory. When the
Run is complete, it fills in any external data and emits the documents.

## Example

Data flow:

```
RunEngine -> proxy -> accumualte_fill_publish.py -> proxy
```

The proxies will log the documents' contents so we can see the filling work.


Run these, each in a separate terminal:

```
bluesky-0MQ-proxy 5577 5578 -vvv
```

```
bluesky-0MQ-proxy 5579 5560 -vvv
```

```
./accumulate_fill_publish.py localhost:5578 localhost:5579
```

In a fourth terminal, push some unfilled documents into port 5577.

```
python generate_example_data.py
```

or to send in live data:

```
from bluesky.callbacks.zmq import Publisher
local_publisher = Publisher('localhost:5577')
RE.subscribe(local_publisher)
```

In the terminal running `bluesky-0MQ-proxy 5577 5578`, we should see logs of
unfilled documents received. In the terminal running
`bluesky-0MQ-proxy 5579 5560` we should see logs filled documents (arrays of
`1.`s).
