"""
Watch Party - the server.

Start it with:
    uvicorn server:app --host 0.0.0.0 --port 8000

How it works, in short:
  * Every room keeps ONE copy of "what is playing and where we are in it".
  * Browsers don't change playback themselves. They tell the server what they
    want (play / pause / seek) and the server tells everybody.
  * The server also sends a small "heartbeat" every few seconds so each browser
    can check whether it has wandered away from the room's position.
"""
import asyncio
import hashlib
import json
import random
import re
import string
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from rapidfuzz import fuzz, process

HERE = Path(__file__).parent

# ---- settings you might want to tweak ----
HEARTBEAT_SECONDS = 3       # how often we tell clients where the video should be
EMPTY_ROOM_MINUTES = 2      # an empty room is deleted after this long
ALLOWED_EMOJIS = ["👍", "😂", "😮", "❤️", "🔥", "👏"]

# the little built-in movie list (see movies.json)
MOVIES = json.loads((HERE / "movies.json").read_text(encoding="utf-8"))
MOVIES_BY_ID = {movie["id"]: movie for movie in MOVIES}

# matches youtube.com/watch?v=..., youtu.be/..., /shorts/..., /embed/..., /live/...
YOUTUBE_PATTERN = re.compile(
    r"(?:youtu\.be/|youtube\.com/(?:watch\?(?:[^#]*&)?v=|embed/|shorts/|live/))([A-Za-z0-9_-]{11})"
)

# uvicorn can only do live connections (the whole point of this app) if one of these is installed.
# Without it the page loads but nothing works, so shout about it right at the start.
try:
    import websockets  # noqa: F401
except ImportError:
    try:
        import wsproto  # noqa: F401
    except ImportError:
        print("\n*** WARNING: no websocket library found. Run:  pip install websockets  ***\n")

app = FastAPI(title="Watch Party")
rooms = {}   # room code -> Room


def now_ms():
    return int(time.time() * 1000)


def random_code(length=6):
    # no 0/O or 1/I, they are too easy to mix up when reading a code out loud
    letters = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    while True:
        code = "".join(random.choice(letters) for _ in range(length))
        if code not in rooms:
            return code


def random_id():
    return "".join(random.choice(string.ascii_lowercase) for _ in range(6))


def get_youtube_id(link):
    match = YOUTUBE_PATTERN.search(link)
    if match:
        return match.group(1)
    return None


def make_video(message):
    """
    Turn what a browser sent us into a video we can play.
    It can be a movie from our list (by id), a YouTube link, or a direct .mp4 link.
    Returns None if we can't make sense of it.
    """
    movie_id = message.get("id")
    if movie_id in MOVIES_BY_ID:
        return MOVIES_BY_ID[movie_id]

    link = str(message.get("url", "")).strip()
    if not link.startswith(("http://", "https://")):
        return None

    youtube_id = get_youtube_id(link)
    if youtube_id:
        # the browser looks up the real title and sends it along, otherwise we use a plain label
        title = str(message.get("title", "")).strip()[:100] or "YouTube video"
        return {
            "id": "yt-" + youtube_id,
            "title": title,
            "yt": youtube_id,        # presence of this key is how the page knows it's YouTube
            "url": link,
            "year": "", "genres": ["YouTube"], "rating": 0, "minutes": 0,
        }

    # anything else is treated as a direct video file
    short_hash = hashlib.md5(link.encode()).hexdigest()[:8]
    file_name = link.split("?")[0].rsplit("/", 1)[-1][:60] or "Video"
    return {
        "id": "file-" + short_hash,
        "title": file_name,
        "url": link,
        "year": "", "genres": [], "rating": 0, "minutes": 0,
    }


class Room:
    def __init__(self, code):
        self.code = code
        self.lock = asyncio.Lock()     # only one change at a time, so nothing gets mixed up
        self.clients = {}              # user id -> {"ws", "name", "joined"}
        self.host = None

        # playback state
        self.video = None
        self.playing = False
        self.position = 0.0            # in seconds, true at the moment `updated` was set
        self.updated = now_ms()
        self.version = 0               # goes up every time playback changes

        # the rest of the room
        self.queue = []
        self.polls = {}
        self.chat = []

    def position_now(self):
        """Where the video is right now. If it's playing, time keeps moving."""
        if self.playing:
            return self.position + (now_ms() - self.updated) / 1000
        return self.position

    def mark_changed(self):
        self.position = max(0.0, self.position)
        self.updated = now_ms()
        self.version += 1

    def people(self):
        return [{"id": uid, "name": c["name"]} for uid, c in self.clients.items()]

    def playback(self, kind, **extra):
        """The playback info that gets sent to browsers."""
        message = {
            "t": kind,
            "video": self.video,
            "playing": self.playing,
            "position": self.position_now(),
            "server_time": now_ms(),
            "version": self.version,
        }
        message.update(extra)
        return message

    def snapshot(self):
        """Everything a browser needs when it joins (or comes back after a drop)."""
        return self.playback(
            "state",
            code=self.code,
            host=self.host,
            users=self.people(),
            queue=self.queue,
            polls=[poll_for_client(p) for p in self.polls.values()],
            chat=self.chat[-50:],
        )


def poll_for_client(poll):
    keys = ["id", "q", "options", "votes", "closed", "by"]
    return {key: poll[key] for key in keys}


async def send_to_all(room, message):
    text = json.dumps(message)
    gone = []
    for uid, client in list(room.clients.items()):
        try:
            await client["ws"].send_text(text)
        except Exception:
            gone.append(uid)
    for uid in gone:
        room.clients.pop(uid, None)


async def tell_who_is_here(room):
    await send_to_all(room, {"t": "presence", "users": room.people(), "host": room.host})


async def heartbeat_loop(room):
    while rooms.get(room.code) is room:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        if not room.clients:
            continue
        await send_to_all(room, {
            "t": "hb",
            "playing": room.playing,
            "position": room.position_now(),
            "server_time": now_ms(),
            "version": room.version,
            "vid": room.video["id"] if room.video else None,
        })


async def delete_room_later(room):
    await asyncio.sleep(EMPTY_ROOM_MINUTES * 60)
    if not room.clients and rooms.get(room.code) is room:
        del rooms[room.code]


# ------------------------------------------------------------------
# normal web pages / API
# ------------------------------------------------------------------
@app.post("/api/rooms")
async def create_room():
    room = Room(random_code())
    rooms[room.code] = room
    asyncio.create_task(heartbeat_loop(room))
    asyncio.create_task(delete_room_later(room))
    return {"code": room.code}


@app.get("/api/rooms/{code}")
async def check_room(code: str):
    room = rooms.get(code.upper())
    if room is None:
        raise HTTPException(status_code=404, detail="Room not found")
    return {"code": room.code, "people": len(room.clients)}


@app.get("/api/movies")
def search_movies(q: str = "", genre: str = "", year: int = 0, min_rating: float = 0):
    results = []
    for movie in MOVIES:
        if genre and genre not in movie["genres"]:
            continue
        if year and movie["year"] != year:
            continue
        if movie["rating"] < min_rating:
            continue
        results.append(movie)

    # fuzzy search on the title, so small typos still find the movie
    if q.strip():
        titles = {movie["id"]: movie["title"] for movie in results}
        matches = process.extract(q, titles, scorer=fuzz.WRatio, processor=str.lower,
                                  limit=20, score_cutoff=60)
        by_id = {movie["id"]: movie for movie in results}
        results = [by_id[movie_id] for _title, _score, movie_id in matches]

    all_genres = sorted({g for movie in MOVIES for g in movie["genres"]})
    return {"results": results, "genres": all_genres}


@app.get("/")
@app.get("/room/{code}")
def home_page(code: str = ""):
    return FileResponse(HERE / "index.html")


# ------------------------------------------------------------------
# the live connection
# ------------------------------------------------------------------
@app.websocket("/ws/{code}")
async def live_connection(ws: WebSocket, code: str, uid: str, name: str = "Guest"):
    await ws.accept()
    room = rooms.get(code.upper())
    if room is None:
        await ws.close(code=4404)
        return

    async with room.lock:
        # same person connecting again (refresh / wifi blip): drop the old socket
        old = room.clients.get(uid)
        if old:
            try:
                await old["ws"].close()
            except Exception:
                pass

        room.clients[uid] = {"ws": ws, "name": name.strip()[:24] or "Guest", "joined": now_ms()}
        if room.host not in room.clients:
            room.host = uid

        # new or returning person gets the full picture straight away
        await ws.send_text(json.dumps(room.snapshot()))
        await tell_who_is_here(room)

    try:
        while True:
            text = await ws.receive_text()
            try:
                message = json.loads(text)
            except ValueError:
                continue
            if isinstance(message, dict):
                await handle_message(room, uid, message, ws)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        async with room.lock:
            still_me = room.clients.get(uid, {}).get("ws") is ws
            if still_me:
                del room.clients[uid]
                if room.host == uid and room.clients:
                    # host left, so the person who has been here longest takes over
                    room.host = min(room.clients, key=lambda u: room.clients[u]["joined"])
                await tell_who_is_here(room)
        if not room.clients:
            asyncio.create_task(delete_room_later(room))


async def play_next_in_queue(room):
    if room.queue:
        next_item = room.queue.pop(0)
        room.video = next_item["video"]
        room.position = 0.0
        room.playing = True
        room.mark_changed()
        await send_to_all(room, room.playback("ctl", action="set_video", by=None, ts=None))
        await send_to_all(room, {"t": "queue", "queue": room.queue})
    else:
        room.playing = False
        room.mark_changed()
        await send_to_all(room, room.playback("ctl", action="ended", by=None, ts=None))


async def handle_message(room, uid, message, ws):
    kind = message.get("t")

    # clock check: the browser uses our reply to work out how far off its clock is
    if kind == "ping":
        reply = {"t": "pong", "c": message.get("c"), "s": now_ms()}
        await ws.send_text(json.dumps(reply))
        return

    async with room.lock:
        # ---------- playback ----------
        if kind in ("play", "pause", "seek"):
            if room.video is None:
                return
            given = message.get("position")
            if isinstance(given, (int, float)) and not isinstance(given, bool):
                room.position = float(given)
            else:
                room.position = room.position_now()

            if kind == "play":
                room.playing = True
            elif kind == "pause":
                room.playing = False
            # a seek keeps whatever play/pause state we already had

            room.mark_changed()
            # "ts" is the browser's own send time, we just hand it back (handy for measuring latency)
            await send_to_all(room, room.playback("ctl", action=kind, by=uid, ts=message.get("ts")))

        elif kind == "set_video":
            video = make_video(message)
            if video is None:
                return
            room.video = video
            room.position = 0.0
            room.playing = True         # starts right away for everybody
            room.mark_changed()
            await send_to_all(room, room.playback("ctl", action="set_video", by=uid, ts=message.get("ts")))

        elif kind == "sync":
            # a browser noticed it missed something, so we send everything again
            await ws.send_text(json.dumps(room.snapshot()))

        elif kind == "next":
            if uid == room.host:
                await play_next_in_queue(room)

        # ---------- queue ----------
        elif kind == "queue_add":
            video = make_video(message)
            if video and len(room.queue) < 50:
                room.queue.append({"pid": random_id(), "video": video})
                await send_to_all(room, {"t": "queue", "queue": room.queue})

        elif kind == "queue_remove":
            room.queue = [item for item in room.queue if item["pid"] != message.get("pid")]
            await send_to_all(room, {"t": "queue", "queue": room.queue})

        elif kind == "queue_move":
            direction = -1 if message.get("dir") == -1 else 1
            spot = None
            for i, item in enumerate(room.queue):
                if item["pid"] == message.get("pid"):
                    spot = i
            if spot is not None and 0 <= spot + direction < len(room.queue):
                q = room.queue
                q[spot], q[spot + direction] = q[spot + direction], q[spot]
                await send_to_all(room, {"t": "queue", "queue": q})

        # ---------- chat and reactions ----------
        elif kind == "chat":
            text = str(message.get("text", "")).strip()[:500]
            if text:
                entry = {"t": "chat", "uid": uid, "name": room.clients[uid]["name"],
                         "text": text, "at": now_ms()}
                room.chat.append(entry)
                room.chat = room.chat[-200:]
                await send_to_all(room, entry)

        elif kind == "reaction":
            if message.get("emoji") in ALLOWED_EMOJIS:
                await send_to_all(room, {"t": "reaction", "emoji": message["emoji"],
                                         "name": room.clients[uid]["name"]})

        # ---------- polls ----------
        elif kind == "poll_create":
            question = str(message.get("q", "")).strip()[:140]
            options = [str(o).strip()[:60] for o in message.get("options", []) if str(o).strip()]
            options = options[:6]
            if question and len(options) >= 2:
                poll_id = random_id()
                room.polls[poll_id] = {"id": poll_id, "q": question, "options": options,
                                       "votes": {}, "closed": False, "by": uid}
                await send_to_all(room, {"t": "poll", "poll": poll_for_client(room.polls[poll_id])})

        elif kind == "poll_vote":
            poll = room.polls.get(message.get("id"))
            choice = message.get("option")
            valid_choice = isinstance(choice, int) and poll and 0 <= choice < len(poll["options"])
            if poll and not poll["closed"] and valid_choice:
                poll["votes"][uid] = choice      # one vote each, voting again just changes it
                await send_to_all(room, {"t": "poll", "poll": poll_for_client(poll)})

        elif kind == "poll_close":
            poll = room.polls.get(message.get("id"))
            if poll and uid in (poll["by"], room.host):
                poll["closed"] = True
                await send_to_all(room, {"t": "poll", "poll": poll_for_client(poll)})

        # ---------- host ----------
        elif kind == "transfer_host":
            new_host = message.get("to")
            if uid == room.host and new_host in room.clients:
                room.host = new_host
                await tell_who_is_here(room)
