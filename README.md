# Watch Party

Watch movies or YouTube videos together, in sync. The backend is FastAPI with WebSockets, and the frontend is one plain HTML/JavaScript file.

## Files
- `server.py` - the server (rooms, sync, chat, polls, queue)
- `index.html` - the whole website
- `movies.json` - the small built-in movie list
- `loadtest.py` - measures how fast controls reach everyone
- `requirements.txt` - Python packages

## Run it
Put all the files in one folder, open a terminal there, then:
```
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8000
```
Open http://localhost:8000 in your browser (don't double-click index.html, it has to come from the server).
To join from a phone or another laptop on the same Wi-Fi, use `http://<your-PC-IP>:8000`. On Windows, run `ipconfig` to find the IP.

## Using it
- **Movies tab:** search (typos are fine), filter by genre, then Play now or Add to queue.
- **YouTube tab:** paste any YouTube link. You get a preview with the thumbnail and title, then Play now or Add to queue. You can also paste a direct `.mp4` link.
- **Queue:** movies and YouTube links can be mixed. When a video ends, the host's browser starts the next one.
- Chat, emoji reactions, polls, and host transfer are in the other tabs.

## How the sync works
- The server keeps one copy of the room's playback state: which video, playing or paused, and the position.
- Browsers only ask for changes (play, pause, seek). The server applies them one at a time and tells everybody.
- Each browser works out how far its clock is from the server's, using ping round trips, so "where should the video be right now" is accurate.
- Joining late or reconnecting gets the full state right away.
- Every 3 seconds the server sends a heartbeat. If you drift a little, the video speeds up or slows down slightly. If you drift more than 0.5 s (0.4 s on YouTube), it jumps to the right spot.

## YouTube notes
- Some videos can't be embedded (the owner turned it off). The app tells you, so just try another one.
- Ads can push one person out of sync. The heartbeat pulls them back afterwards.
- YouTube doesn't allow tiny speed changes, so on YouTube the drift fix is always a jump.
- Video titles come from the free noembed.com service. If it's not reachable, the app uses "YouTube video".

## Measuring latency
With the server running, in a second terminal:
```
python loadtest.py --clients 10 --trials 200
```
It prints the median, p95 and p99 delay, checks that nothing got lost, and checks that a viewer who reconnects gets the full state back. Also test with two real devices on Wi-Fi, and write down the machine, network, number of viewers and trials in your report.
