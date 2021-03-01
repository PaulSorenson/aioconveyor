# aioconveyor

Long running, timed producer consumer loop.

Example:

`python -m aioconveyor`

The embedded `main()` has a simple producer that generates strings every 5 seconds and (an arbitrary) two instances of a consumer that prints the string.

## motivation

I have a number of systems around my house and beyond which I just need to poll every 30 seconds or so and then write the data to a number of systems such as postgresql, mqtt, csv, console etc.

I wrote the first one before `asyncio` was even a thing in python to poll my solar PV inverter, and then I rewrote it as an asynchronous model. I have several apps where the loop/scheduler all look the same - only the data and the collection look different.

It was a very simple matter to migrate existing apps to `aioconveyor`. Since it allows an arbitrary number of consumers, I quickly realized adding `mqtt` consumers alongside my postgresql writers I could easily integrate custom data with homeassistant.

Some of these apps will find their way to github in due course.

## plug in your producers (readers) and consumers (writers)

`main()` looks like so:

``` python
async def main():
    conv = AioConveyor(
        produce=partial(produce_arg, "paul"),
        consumers=[consume, consume],
        loop_interval=5.0,
    )

    conv.start()
    log.info("main: thread started")

    while conv.running:
        log.debug("main loop")
        await asyncio.sleep(2)
    log.info("main: conveyor thread no longer running, terminating")
```

If invoked as in the example above, the toy producer and consumer are invoked. Just for fun I plugged in two instances of the consumer. In a more realistic scenario, that could be a `postgresql` and `mqtt` consumer, both have asyncio packages.

``` python
async def produce_arg(name: str, event_time: datetime) -> str:
    """toy producer"""
    data = f"hello {name} {time()}"
    print("<<< produce:", data)
    return data


async def consume(data: str, event_time: datetime) -> int:
    """toy consumer"""
    print(f"consume: {data} @ {event_time} >>>")
    return 0
```

## structure

The code is pretty simple and shouldn't be too hard to follow, although with asyncio, the call flow is sometimes not so obvious if you haven't been exposed to asyncio before.

1. The asyncio loop is run in a daemon thread. That is not abolutely necessary but I wanted the main thread to be isolated from any blocking from badly behaved coroutines.
2. This makes the start up slightly involved beacause aside from a couple of thread safe entry points in the asyncio package - in general it is not thread safe. Having said that you can ignore most of that. The code snippet above shows the minimal inputs for `AioConveyor`.
3. flow:
   1. `start` runs the deamon thread:
   2. `run_loop` which kicks the main asyncio coroutine:
   3. `launcher` is a long running coroutine which kicks off two further long running tasks:
      1. `scheduler` fires of the producers on a regular clock and the passes the output to the consumers.
      2. `watchdog` - to be honest this doesn't do much, more like a blue heeler taking a break behind the shearing shed. As with all of these methods it could be customized to be more aware of what is going on and more importantly, what has gone belly up.
4. random notes:
   1. If you have mutliple consumers, take care not to modify the data, make copies if you need to.
   2. The way it is currently structured, the producer + consumers are not overlapped. Using an `asyncio.Queue` to pass the data from producer to consumers would be one way to run them concurrently. Since my loops run every 10 seconds up to 5 minutes, and the io generally takes well under 1 second, it is not a pressing need for me.
   3. I have consumers that are themselves classes that setup things in the constructor (eg connections). Take care when doing this that the "connection" is threadsafe if you construct it in `main()`.
   4. Since multiple consumers can easily be plugged in, I leverage this for debugging the output with a pretty printer consumer before I start inserting records into a database.
