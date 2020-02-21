from bluesky.callbacks.zmq import Publisher
from bluesky import RunEngine
from bluesky.plans import scan
from ophyd.sim import img, motor


RE = RunEngine()
publisher = Publisher('localhost:5577')
RE.subscribe(publisher)
RE(scan([img], motor, -1, 1, 3))
