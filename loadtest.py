"""
Latency test for Watch Party.

Run the server first, then:
    python loadtest.py --clients 10 --trials 200

What it does:
  1. Opens a room and connects N fake viewers.
  2. One viewer (the "controller") sends play / pause / seek over and over.
  3. For each action we measure how long it takes to reach every viewer.
  4. Then it makes everyone seek at the same moment, and finally drops one
     viewer and checks that they get the full room state back when they return.

All the fake viewers run in this one script, so they share a clock. That is
what makes the timing numbers trustworthy.
"""
import argparse
import asyncio
import json
import statistics
import time
import urllib.request

import websockets


def percentile(numbers, p):
    ordered = sorted(numbers)
    index = min(len(ordered) - 1, int(len(ordered) * p / 100))
    return ordered[index]


async def run_test(host, client_count, trials):
    # make a fresh room to test in
    request = urllib.request.Request("http://" + host + "/api/rooms", method="POST")
    code = json.load(urllib.request.urlopen(request))["code"]

    received_at = {}     # action id -> {viewer number: time it arrived (ms)}
    connections = []

    async def listen(viewer_number, connection):
        async for text in connection:
            message = json.loads(text)
            if message.get("t") == "ctl" and message.get("ts"):
                arrivals = received_at.setdefault(message["ts"], {})
                arrivals[viewer_number] = time.time() * 1000

    for i in range(client_count):
        url = "ws://" + host + "/ws/" + code + "?uid=u" + str(i) + "&name=viewer" + str(i)
        connection = await websockets.connect(url)
        connections.append(connection)
        asyncio.create_task(listen(i, connection))
    await asyncio.sleep(0.5)

    controller = connections[0]
    await controller.send(json.dumps({"t": "set_video", "id": "bbb"}))
    await asyncio.sleep(0.3)

    # ---- part 1: how fast do controls reach everyone? ----
    delays = []
    failed_trials = 0
    actions = ["play", "pause", "seek"]

    for n in range(trials):
        action_id = int(time.time() * 1000) * 1000 + n      # unique number per action
        sent_at = time.time() * 1000
        message = {"t": actions[n % 3], "position": float(n % 60), "ts": action_id}
        await controller.send(json.dumps(message))

        # wait (up to 2 seconds) until every viewer has it
        give_up_at = time.time() + 2
        while len(received_at.get(action_id, {})) < client_count and time.time() < give_up_at:
            await asyncio.sleep(0.001)

        arrivals = received_at.pop(action_id, {})
        if len(arrivals) < client_count:
            failed_trials += 1
        for arrived in arrivals.values():
            delays.append(arrived - sent_at)

        await asyncio.sleep(0.02)

    # ---- part 2: everyone seeks at once ----
    for round_number in range(20):
        sends = []
        for i, connection in enumerate(connections):
            sends.append(connection.send(json.dumps({"t": "seek", "position": float(i)})))
        await asyncio.gather(*sends)
        await asyncio.sleep(0.2)

    # ---- part 3: one viewer drops and comes back ----
    await connections[1].close()
    url = "ws://" + host + "/ws/" + code + "?uid=u1&name=viewer1"
    returning = await websockets.connect(url)
    first_message = json.loads(await asyncio.wait_for(returning.recv(), 2))
    got_full_state = (first_message.get("t") == "state"
                      and first_message.get("video", {}).get("id") == "bbb")

    # ---- results ----
    p95 = percentile(delays, 95)
    print("viewers:", client_count, " trials:", trials, " deliveries:", len(delays),
          " trials with a missing delivery:", failed_trials)
    print("delay in ms  ->  median %.2f   p95 %.2f   p99 %.2f   worst %.2f"
          % (statistics.median(delays), p95, percentile(delays, 99), max(delays)))
    print("p95 under 250 ms:", "PASS" if p95 <= 250 else "FAIL")
    print("reconnect gets full state back:", "PASS" if got_full_state else "FAIL")

    for connection in connections:
        await connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1:8000")
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--trials", type=int, default=200)
    args = parser.parse_args()
    asyncio.run(run_test(args.host, args.clients, args.trials))
