"""
╔══════════════════════════════════════════════════════╗
║         🎵 VoidX Music Bot  v3.0  — PRODUCTION       ║
║   Redis Queue  |  MongoDB Stats  |  Sponsor System   ║
╚══════════════════════════════════════════════════════╝
"""

import os, asyncio, logging, random, time
import aiohttp
from pyrogram import Client, filters
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioPiped, AudioParameters
import yt_dlp

from database.db import (
    init_db, RedisQueue, RedisState,
    log_play, get_top_songs, get_user_stats,
    get_group_history, get_top_users,
    get_group_settings, update_group_setting,
    add_sponsor, remove_sponsor, list_sponsors,
    check_and_send_sponsor,
)

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
API_ID         = int(os.environ.get("API_ID",         "0"))
API_HASH       =     os.environ.get("API_HASH",        "")
BOT_TOKEN      =     os.environ.get("BOT_TOKEN",       "")
STRING_SESSION =     os.environ.get("STRING_SESSION",  "")
OWNER_ID       = int(os.environ.get("OWNER_ID",        "0"))   # aapka Telegram ID
DOWNLOAD_DIR   = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────
# CLIENTS
# ──────────────────────────────────────────
app       = Client("MusicBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot   = Client("UserBot",  api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)
pytgcalls = PyTgCalls(userbot)

idle_tasks = {}   # {chat_id: asyncio.Task}

# ══════════════════════════════════════════
# UTILS
# ══════════════════════════════════════════
def make_track(info: dict, requester: str, user_id: int) -> dict:
    dur = info.get("duration", 0)
    m, s = divmod(dur, 60)
    return {
        "title":        info.get("title",    "Unknown"),
        "url":          info.get("url",       ""),
        "webpage_url":  info.get("webpage_url",""),
        "duration":     dur,
        "duration_fmt": f"{m}:{s:02d}",
        "thumbnail":    info.get("thumbnail", ""),
        "uploader":     info.get("uploader",  "Unknown"),
        "requester":    requester,
        "user_id":      user_id,
    }

def search_yt(query: str) -> dict:
    opts = {
        "format": "bestaudio/best", "quiet": True,
        "no_warnings": True, "default_search": "ytsearch1",
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(query, download=False)
        if "entries" in info: info = info["entries"][0]
        return info

def dl_audio(query: str) -> str:
    safe    = "".join(c for c in query[:40] if c.isalnum() or c in " _-").strip()
    outpath = os.path.join(DOWNLOAD_DIR, safe)
    opts = {
        "format": "bestaudio/best", "quiet": True,
        "outtmpl": outpath + ".%(ext)s",
        "postprocessors": [{"key":"FFmpegExtractAudio","preferredcodec":"mp3","preferredquality":"192"}],
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([query])
    return outpath + ".mp3"

async def fetch_lyrics(title: str) -> str | None:
    try:
        parts  = title.split(" - ", 1)
        artist = parts[0].strip() if len(parts)==2 else title
        song   = parts[1].strip() if len(parts)==2 else title
        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                f"https://api.lyrics.ovh/v1/{artist}/{song}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                if r.status == 200:
                    data   = await r.json()
                    lyrics = data.get("lyrics","")
                    if lyrics:
                        return lyrics[:3800] + ("\n_...aur bhi hai_" if len(lyrics)>3800 else "")
    except:
        pass
    return None

# ══════════════════════════════════════════
# PLAY CORE
# ══════════════════════════════════════════
async def play_track(chat_id: int, track: dict):
    try:
        await pytgcalls.join_group_call(
            chat_id, AudioPiped(track["url"], AudioParameters(bitrate=160))
        )
    except:
        await pytgcalls.change_stream(
            chat_id, AudioPiped(track["url"], AudioParameters(bitrate=160))
        )

def np_buttons(chat_id: int, loop: bool, shuffle: bool, m247: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⏸ Pause",  callback_data="pause"),
            InlineKeyboardButton("⏭ Skip",   callback_data="skip"),
            InlineKeyboardButton("⏹ Stop",   callback_data="stop"),
        ],
        [
            InlineKeyboardButton(f"🔁 {'✅' if loop    else '❌'}",     callback_data="loop"),
            InlineKeyboardButton(f"🔀 {'✅' if shuffle else '❌'}",     callback_data="shuffle"),
            InlineKeyboardButton(f"♾️ {'✅' if m247    else '❌'}",     callback_data="247"),
        ],
        [
            InlineKeyboardButton("📋 Queue",   callback_data="queue"),
            InlineKeyboardButton("🎤 Lyrics",  callback_data="lyrics"),
            InlineKeyboardButton("📊 Stats",   callback_data="stats"),
        ],
    ])

async def send_np(chat_id: int, track: dict):
    loop    = await RedisState.get(chat_id, "loop",    False)
    shuffle = await RedisState.get(chat_id, "shuffle", False)
    m247    = await RedisState.get(chat_id, "247",     False)
    caption = (
        f"🎵 **Ab Chal Raha Hai**\n\n"
        f"**{track['title']}**\n"
        f"🎤 `{track['uploader']}`  ·  ⏱ `{track['duration_fmt']}`\n"
        f"👤 {track['requester']}"
    )
    btns = np_buttons(chat_id, loop, shuffle, m247)
    try:
        if track.get("thumbnail"):
            await app.send_photo(chat_id, photo=track["thumbnail"], caption=caption, reply_markup=btns)
            return
    except:
        pass
    await app.send_message(chat_id, caption, reply_markup=btns)

# ══════════════════════════════════════════
# IDLE WATCHER  (Auto-Leave)
# ══════════════════════════════════════════
async def idle_watcher(chat_id: int):
    settings = await get_group_settings(chat_id)
    timeout  = settings.get("idle_timeout", 300)
    await asyncio.sleep(timeout)
    if await RedisState.get(chat_id, "247", False): return
    if await RedisQueue.length(chat_id) > 0: return
    try:
        await pytgcalls.leave_group_call(chat_id)
        await RedisQueue.clear(chat_id)
        await app.send_message(chat_id, f"💤 **{timeout//60} min idle — Bot nikal gaya!**\n`/play` se wapas bulao.")
    except: pass

def reset_idle(chat_id: int):
    if chat_id in idle_tasks and not idle_tasks[chat_id].done():
        idle_tasks[chat_id].cancel()
    idle_tasks[chat_id] = asyncio.create_task(idle_watcher(chat_id))

# ══════════════════════════════════════════════════════════════
# ░░░░░░░░░░░░░░░░  COMMANDS  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
# ══════════════════════════════════════════════════════════════

# ── /start ──────────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def start_cmd(_, msg: Message):
    me = await app.get_me()
    await msg.reply_text(
        "🎵 **VoidX Music Bot v3.0** — Production Ready!\n\n"
        "**🎶 Music:**\n"
        "`/play` · `/pause` · `/resume` · `/skip` · `/stop`\n"
        "`/queue` · `/np` · `/volume` · `/loop` · `/shuffle`\n\n"
        "**📊 Stats:**\n"
        "`/history` · `/topcharts` · `/mystats` · `/topusers`\n\n"
        "**⚙️ Settings:**\n"
        "`/settings` · `/247` · `/autoleave`\n\n"
        "**💰 Sponsor:**\n"
        "`/sponsor add` · `/sponsor list` · `/sponsor remove`\n\n"
        "**🎧 Extra:**\n"
        "`/download [song]` · `/lyrics [song]`",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Group Mein Add Karo", url=f"https://t.me/{me.username}?startgroup=true")
        ]])
    )

# ── /play ────────────────────────────────────────────────────
@app.on_message(filters.command("play") & filters.group)
async def play_cmd(_, msg: Message):
    if len(msg.command) < 2:
        await msg.reply_text("❌ `/play Kesariya`"); return

    query = " ".join(msg.command[1:])
    m     = await msg.reply_text(f"🔍 **{query}** dhoondh raha hoon...")

    try:
        info  = await asyncio.get_event_loop().run_in_executor(None, search_yt, query)
        track = make_track(info, msg.from_user.mention, msg.from_user.id)
        qlen  = await RedisQueue.length(msg.chat.id)

        await RedisQueue.push(msg.chat.id, track)
        reset_idle(msg.chat.id)

        # Log to DB + get play count for sponsor check
        play_count = await log_play(
            msg.chat.id, msg.from_user.id,
            msg.from_user.username or msg.from_user.first_name, track
        )

        if qlen == 0:
            await play_track(msg.chat.id, track)
            await m.delete()
            await send_np(msg.chat.id, track)
        else:
            await m.edit_text(
                f"📋 **Queue Mein Add!**\n\n"
                f"🎵 **{track['title']}**\n"
                f"📍 Position `#{qlen+1}` · ⏱ `{track['duration_fmt']}`\n"
                f"👤 {track['requester']}"
            )

        # Sponsor check — har N songs ke baad
        async def _send_sponsor(text):
            await app.send_message(msg.chat.id, f"📢 **Sponsored**\n\n{text}")

        await check_and_send_sponsor(msg.chat.id, play_count, _send_sponsor)

    except Exception as e:
        logger.error(f"Play error: {e}")
        await m.edit_text(f"❌ Song nahi mila!\n`{str(e)[:200]}`")

# ── Controls ─────────────────────────────────────────────────
@app.on_message(filters.command("pause") & filters.group)
async def pause_cmd(_, msg):
    try: await pytgcalls.pause_stream(msg.chat.id); await msg.reply_text("⏸ **Paused!**")
    except Exception as e: await msg.reply_text(f"❌ `{e}`")

@app.on_message(filters.command("resume") & filters.group)
async def resume_cmd(_, msg):
    try:
        await pytgcalls.resume_stream(msg.chat.id)
        reset_idle(msg.chat.id)
        await msg.reply_text("▶️ **Resumed!**")
    except Exception as e: await msg.reply_text(f"❌ `{e}`")

@app.on_message(filters.command("skip") & filters.group)
async def skip_cmd(_, msg):
    loop = await RedisState.get(msg.chat.id, "loop", False)
    if loop:
        await RedisQueue.rotate_loop(msg.chat.id)
    else:
        await RedisQueue.pop(msg.chat.id)

    nxt = await RedisQueue.peek(msg.chat.id)
    if nxt:
        await play_track(msg.chat.id, nxt); reset_idle(msg.chat.id)
        await send_np(msg.chat.id, nxt)
    else:
        try: await pytgcalls.leave_group_call(msg.chat.id)
        except: pass
        await msg.reply_text("⏭ Queue khaali — Bot nikal gaya!")

@app.on_message(filters.command("stop") & filters.group)
async def stop_cmd(_, msg):
    await RedisQueue.clear(msg.chat.id)
    await RedisState.set(msg.chat.id, "loop",    False)
    await RedisState.set(msg.chat.id, "shuffle", False)
    try: await pytgcalls.leave_group_call(msg.chat.id)
    except: pass
    await msg.reply_text("⏹ **Band! Queue saaf.**")

@app.on_message(filters.command("queue") & filters.group)
async def queue_cmd(_, msg):
    q = await RedisQueue.get_all(msg.chat.id)
    if not q: await msg.reply_text("📋 Queue khaali! `/play` se shuru karo."); return
    text = "📋 **Music Queue:**\n\n"
    for i, t in enumerate(q[:15]):
        text += f"{'🎵' if i==0 else f'`{i}.`'} **{t['title']}** `[{t['duration_fmt']}]`\n    👤 {t['requester']}\n\n"
    if len(q) > 15: text += f"_...aur {len(q)-15} songs_"
    loop = await RedisState.get(msg.chat.id, "loop", False)
    shuf = await RedisState.get(msg.chat.id, "shuffle", False)
    if loop or shuf:
        text += "\n" + ("🔁 Loop  " if loop else "") + ("🔀 Shuffle" if shuf else "")
    await msg.reply_text(text)

@app.on_message(filters.command("np") & filters.group)
async def np_cmd(_, msg):
    t = await RedisQueue.peek(msg.chat.id)
    if not t: await msg.reply_text("❌ Kuch nahi chal raha!"); return
    await send_np(msg.chat.id, t)

@app.on_message(filters.command("volume") & filters.group)
async def volume_cmd(_, msg):
    if len(msg.command) < 2: await msg.reply_text("❌ `/volume 80`"); return
    try:
        vol = int(msg.command[1])
        if not 1 <= vol <= 200: await msg.reply_text("❌ 1-200 ke beech!"); return
        await pytgcalls.change_volume_call(msg.chat.id, vol)
        await update_group_setting(msg.chat.id, "volume", vol)
        bar = "█"*(vol//20) + "░"*(10-vol//20)
        await msg.reply_text(f"🔊 **Volume: {vol}%**\n`{bar}`")
    except Exception as e: await msg.reply_text(f"❌ `{e}`")

@app.on_message(filters.command("loop") & filters.group)
async def loop_cmd(_, msg):
    val = await RedisState.toggle(msg.chat.id, "loop")
    await msg.reply_text(f"🔁 **Loop: {'ON ✅' if val else 'OFF ❌'}**")

@app.on_message(filters.command("shuffle") & filters.group)
async def shuffle_cmd(_, msg):
    val = await RedisState.toggle(msg.chat.id, "shuffle")
    if val: await RedisQueue.shuffle(msg.chat.id)
    await msg.reply_text(f"🔀 **Shuffle: {'ON ✅ — Queue shuffle ho gayi!' if val else 'OFF ❌'}**")

@app.on_message(filters.command("247") & filters.group)
async def mode247_cmd(_, msg):
    val = await RedisState.toggle(msg.chat.id, "247")
    await update_group_setting(msg.chat.id, "mode_247", val)
    await msg.reply_text(f"♾️ **24/7 Mode: {'ON ✅' if val else 'OFF ❌'}**")

# ══════════════════════════════════════════
# STATS COMMANDS
# ══════════════════════════════════════════

@app.on_message(filters.command("history") & filters.group)
async def history_cmd(_, msg):
    history = await get_group_history(msg.chat.id, limit=10)
    if not history: await msg.reply_text("📜 Koi history nahi abhi tak!"); return
    text = "📜 **Recent Play History:**\n\n"
    for i, h in enumerate(history, 1):
        t    = h["played_at"].strftime("%d %b %H:%M")
        text += f"`{i}.` **{h['title']}**\n    👤 @{h.get('username','?')}  ·  🕐 {t}\n\n"
    await msg.reply_text(text)

@app.on_message(filters.command("topcharts") & filters.group)
async def topcharts_cmd(_, msg):
    top = await get_top_songs(chat_id=msg.chat.id, limit=10)
    if not top: await msg.reply_text("📊 Abhi koi data nahi!"); return
    text = "🏆 **Top Songs (Is Group Mein):**\n\n"
    medals = ["🥇","🥈","🥉"] + ["🎵"]*7
    for i, s in enumerate(top):
        text += f"{medals[i]} **{s['_id']}** — `{s['count']} plays`\n"
    await msg.reply_text(text)

@app.on_message(filters.command("mystats"))
async def mystats_cmd(_, msg):
    stats = await get_user_stats(msg.from_user.id)
    if not stats:
        await msg.reply_text("📊 Tumhari koi stats nahi abhi tak! `/play` se shuru karo."); return
    total_mins = stats.get("total_duration", 0) // 60
    await msg.reply_text(
        f"📊 **Tumhari Stats:**\n\n"
        f"👤 {msg.from_user.mention}\n"
        f"🎵 Total Plays: `{stats.get('total_plays', 0)}`\n"
        f"⏱ Total Suna: `{total_mins} minutes`\n"
        f"📅 Join: `{stats.get('joined','?').strftime('%d %b %Y') if stats.get('joined') else '?'}`\n"
        f"🕐 Last Seen: `{stats.get('last_seen','?').strftime('%d %b %H:%M') if stats.get('last_seen') else '?'}`"
    )

@app.on_message(filters.command("topusers") & filters.group)
async def topusers_cmd(_, msg):
    users = await get_top_users(chat_id=msg.chat.id, limit=10)
    if not users: await msg.reply_text("👥 Abhi koi data nahi!"); return
    text  = "👑 **Top Music Listeners:**\n\n"
    medals = ["🥇","🥈","🥉"] + ["🎧"]*7
    for i, u in enumerate(users):
        name = u.get("username") or "Unknown"
        text += f"{medals[i]} @{name} — `{u.get('total_plays',0)} plays`\n"
    await msg.reply_text(text)

# ══════════════════════════════════════════
# SETTINGS COMMAND
# ══════════════════════════════════════════
@app.on_message(filters.command("settings") & filters.group)
async def settings_cmd(_, msg):
    s     = await get_group_settings(msg.chat.id)
    loop  = await RedisState.get(msg.chat.id, "loop",    False)
    shuf  = await RedisState.get(msg.chat.id, "shuffle", False)
    m247  = await RedisState.get(msg.chat.id, "247",     False)
    await msg.reply_text(
        f"⚙️ **Group Settings:**\n\n"
        f"🔊 Volume: `{s.get('volume', 100)}%`\n"
        f"⏱ Idle Timeout: `{s.get('idle_timeout', 300)//60} min`\n"
        f"🔁 Loop: `{'ON' if loop else 'OFF'}`\n"
        f"🔀 Shuffle: `{'ON' if shuf else 'OFF'}`\n"
        f"♾️ 24/7 Mode: `{'ON' if m247 else 'OFF'}`\n"
        f"🖼 Thumbnail: `{'ON' if s.get('thumbnail', True) else 'OFF'}`\n\n"
        f"_Commands: `/loop` `/shuffle` `/247` `/volume`_"
    )

# ══════════════════════════════════════════
# 💰 SPONSOR SYSTEM
# ══════════════════════════════════════════
@app.on_message(filters.command("sponsor") & filters.group)
async def sponsor_cmd(_, msg: Message):
    if len(msg.command) < 2:
        await msg.reply_text(
            "💰 **Sponsor System:**\n\n"
            "`/sponsor add [naam] | [message] | [interval]`\n"
            "_Example: `/sponsor add Nike | Buy Nike shoes! 👟 | 5`_\n"
            "_(Har 5 songs ke baad message aayega)_\n\n"
            "`/sponsor list` — Sab sponsors dekho\n"
            "`/sponsor remove [naam]` — Sponsor hatao"
        )
        return

    action = msg.command[1].lower()

    if action == "add":
        # Format: /sponsor add naam | message | interval
        try:
            rest  = msg.text.split("add", 1)[1].strip()
            parts = [p.strip() for p in rest.split("|")]
            name      = parts[0]
            sponsor_msg = parts[1] if len(parts) > 1 else "Sponsored message"
            interval  = int(parts[2]) if len(parts) > 2 else 10

            await add_sponsor(
                msg.chat.id, name, sponsor_msg,
                interval=interval, added_by=msg.from_user.id
            )
            await msg.reply_text(
                f"✅ **Sponsor Add Ho Gaya!**\n\n"
                f"📛 Naam: `{name}`\n"
                f"📢 Message: _{sponsor_msg}_\n"
                f"🔢 Interval: Har `{interval}` songs ke baad"
            )
        except Exception as e:
            await msg.reply_text(f"❌ Format galat!\n`/sponsor add naam | message | interval`\n\n`{e}`")

    elif action == "list":
        sponsors = await list_sponsors(msg.chat.id)
        if not sponsors:
            await msg.reply_text("📋 Koi sponsor nahi abhi tak!"); return
        text = "📋 **Active Sponsors:**\n\n"
        for s in sponsors:
            status = "✅ Active" if s.get("active") else "❌ Inactive"
            text  += (
                f"📛 **{s['name']}** — {status}\n"
                f"📢 _{s['message'][:60]}..._\n"
                f"🔢 Interval: `{s['interval']}` songs · Shown: `{s.get('plays_shown',0)}` times\n\n"
            )
        await msg.reply_text(text)

    elif action == "remove":
        if len(msg.command) < 3:
            await msg.reply_text("❌ `/sponsor remove [naam]`"); return
        name   = " ".join(msg.command[2:])
        result = await remove_sponsor(msg.chat.id, name)
        await msg.reply_text(
            f"✅ **`{name}` Sponsor Hata Diya!**" if result
            else f"❌ `{name}` naam ka sponsor nahi mila!"
        )

# ══════════════════════════════════════════
# DOWNLOAD & LYRICS
# ══════════════════════════════════════════
@app.on_message(filters.command("download") & filters.group)
async def download_cmd(_, msg: Message):
    if len(msg.command) < 2: await msg.reply_text("❌ `/download Tum Hi Ho`"); return
    query = " ".join(msg.command[1:])
    m     = await msg.reply_text(f"⬇️ **{query}** download ho raha hai...")
    try:
        fp = await asyncio.get_event_loop().run_in_executor(None, dl_audio, query)
        await msg.reply_audio(fp, caption=f"🎵 **{query}**\n_VoidX Music Bot_")
        os.remove(fp); await m.delete()
    except Exception as e: await m.edit_text(f"❌ Fail!\n`{str(e)[:200]}`")

@app.on_message(filters.command("lyrics") & filters.group)
async def lyrics_cmd(_, msg: Message):
    if len(msg.command) >= 2:
        query = " ".join(msg.command[1:])
    else:
        t = await RedisQueue.peek(msg.chat.id)
        if not t: await msg.reply_text("❌ `/lyrics Song Name`"); return
        query = t["title"]
    m      = await msg.reply_text(f"🎤 Lyrics dhoondh raha hoon...")
    lyrics = await fetch_lyrics(query)
    if lyrics: await m.edit_text(f"🎤 **{query}**\n\n{lyrics}")
    else:       await m.edit_text(f"😔 Lyrics nahi mile!\nGoogle: `{query} lyrics`")

# ══════════════════════════════════════════
# CALLBACKS
# ══════════════════════════════════════════
@app.on_callback_query()
async def cb(_, query: CallbackQuery):
    cid  = query.message.chat.id
    data = query.data
    try:
        if data == "pause":
            await pytgcalls.pause_stream(cid); await query.answer("⏸ Paused!")

        elif data == "resume":
            await pytgcalls.resume_stream(cid); reset_idle(cid); await query.answer("▶️ Resumed!")

        elif data == "skip":
            loop = await RedisState.get(cid, "loop", False)
            if loop: await RedisQueue.rotate_loop(cid)
            else:    await RedisQueue.pop(cid)
            nxt = await RedisQueue.peek(cid)
            if nxt:
                await play_track(cid, nxt); reset_idle(cid)
                await query.answer(f"⏭ {nxt['title'][:40]}"); await send_np(cid, nxt)
            else:
                try: await pytgcalls.leave_group_call(cid)
                except: pass
                await query.answer("Queue khaali!")

        elif data == "stop":
            await RedisQueue.clear(cid)
            try: await pytgcalls.leave_group_call(cid)
            except: pass
            await query.answer("⏹ Stopped!")

        elif data == "loop":
            val = await RedisState.toggle(cid, "loop")
            await query.answer(f"🔁 Loop {'ON ✅' if val else 'OFF ❌'}")
            t = await RedisQueue.peek(cid)
            if t:
                l,s,m = val, await RedisState.get(cid,"shuffle",False), await RedisState.get(cid,"247",False)
                try: await query.message.edit_reply_markup(np_buttons(cid,l,s,m))
                except: pass

        elif data == "shuffle":
            val = await RedisState.toggle(cid, "shuffle")
            if val: await RedisQueue.shuffle(cid)
            await query.answer(f"🔀 Shuffle {'ON ✅' if val else 'OFF ❌'}")

        elif data == "247":
            val = await RedisState.toggle(cid, "247")
            await update_group_setting(cid, "mode_247", val)
            await query.answer(f"♾️ 24/7 {'ON ✅' if val else 'OFF ❌'}")

        elif data == "queue":
            q = await RedisQueue.get_all(cid)
            if not q: await query.answer("Queue khaali!", show_alert=True)
            else:
                txt = "\n".join(f"{'▶' if i==0 else str(i)+'.'} {t['title'][:35]}" for i,t in enumerate(q[:8]))
                await query.answer(txt[:200], show_alert=True)

        elif data == "lyrics":
            t = await RedisQueue.peek(cid)
            if not t: await query.answer("Kuch nahi chal raha!", show_alert=True); return
            await query.answer("🎤 Lyrics fetch ho raha hai...")
            lyrics = await fetch_lyrics(t["title"])
            if lyrics: await app.send_message(cid, f"🎤 **{t['title']}**\n\n{lyrics[:3000]}")
            else:       await app.send_message(cid, f"😔 Lyrics nahi mile: `{t['title']}`")

        elif data == "stats":
            t = await RedisQueue.peek(cid)
            top = await get_top_songs(cid, limit=3)
            txt = "📊 **Quick Stats:**\n"
            if top:
                txt += "\n".join(f"🎵 {s['_id'][:35]} — `{s['count']}x`" for s in top)
            await query.answer(txt[:200] or "Abhi koi data nahi!", show_alert=True)

    except Exception as e:
        logger.error(f"CB error: {e}")
        await query.answer("❌ Error!", show_alert=True)

# ══════════════════════════════════════════
# STREAM ENDED
# ══════════════════════════════════════════
@pytgcalls.on_stream_end()
async def stream_ended(_, update):
    cid  = update.chat_id
    loop = await RedisState.get(cid, "loop", False)

    if loop:
        await RedisQueue.rotate_loop(cid)
    else:
        await RedisQueue.pop(cid)

    nxt = await RedisQueue.peek(cid)
    if nxt:
        await play_track(cid, nxt); reset_idle(cid)
        await send_np(cid, nxt)
    else:
        m247 = await RedisState.get(cid, "247", False)
        if m247:
            await app.send_message(cid, "♾️ Queue khaali. `/play` se add karo!")
        else:
            try: await pytgcalls.leave_group_call(cid)
            except: pass

# ══════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════
async def main():
    logger.info("🚀 VoidX Music Bot v3.0 start ho raha hai...")
    await init_db()
    await userbot.start()
    await pytgcalls.start()
    await app.start()
    me = await app.get_me()
    logger.info(f"✅ @{me.username} LIVE hai!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
