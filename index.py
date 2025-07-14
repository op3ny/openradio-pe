#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HSYST Radio Player - Premium Edition with Rate Limiting
------------------------------------------------------------
- Reliable track playback with full duration support
- Enhanced visual interface
- Comprehensive JSON logging system
- Robust error handling
- Intelligent rate limiting for SoundCloud API
"""

import os
import sys
import json
import time
import random
import logging
import argparse
import subprocess
import requests
import signal
import threading
import queue
from datetime import datetime, timedelta
from collections import defaultdict, deque, OrderedDict
from enum import Enum, auto
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Deque, Tuple, Set, Any
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import select
from pathlib import Path
import math

# Constants
VERSION = "8.1"  # Professional Edition with Rate Limiting
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
MAX_RETRIES = 3
RETRY_DELAY = 2
BUFFER_CHECK_INTERVAL = 0.5
CONFIG_RELOAD_INTERVAL = 5
TRANSITION_DURATION = 3
MIN_BUFFER_SIZE = 5
MAX_QUEUE_SIZE = 15
STREAM_TIMEOUT = 30
PRELOAD_WORKERS = 3
FFMPEG_MONITOR_INTERVAL = 0.1
STREAM_END_TIMEOUT = 10
FFMPEG_RESTART_DELAY = 1
PLAYBACK_END_MARGIN = 5  # Seconds to wait after expected track end

# Rate limiting constants
MIN_REQUEST_INTERVAL = 3.0  # Minimum seconds between requests
MAX_REQUEST_INTERVAL = 10.0  # Maximum seconds between requests
ADAPTIVE_DELAY_INCREMENT = 0.5  # How much to increase delay when rate limited
ADAPTIVE_DELAY_DECREMENT = 0.1  # How much to decrease delay when successful
INITIAL_REQUEST_DELAY = 5.0  # Starting delay between requests
MAX_CONSECUTIVE_FAILURES = 3  # Max failures before backing off significantly

# Setup logging
def setup_logging(base_dir: str) -> logging.Logger:
    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger("HSYSTRadio")
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Main log file
    main_log = os.path.join(log_dir, "hsyst_radio.log")
    file_handler = logging.FileHandler(main_log)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    
    # Console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# JSON logger setup
class JsonLogger:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.log_dir = os.path.join(base_dir, "logs", "json")
        os.makedirs(self.log_dir, exist_ok=True)
        
    def _write_json_log(self, filename: str, data: Dict) -> None:
        try:
            filepath = os.path.join(self.log_dir, filename)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logging.getLogger("HSYSTRadio").error(f"Error writing JSON log: {e}")

    def log_current_track(self, track: Dict) -> None:
        self._write_json_log("current_track.json", track)
        
    def log_queue(self, queue: List[Dict]) -> None:
        self._write_json_log("upcoming_tracks.json", {"tracks": queue})
        
    def log_history(self, history: List[Dict]) -> None:
        self._write_json_log("played_tracks.json", {"tracks": history})
        
    def log_artists(self, artists: Dict[str, int]) -> None:
        self._write_json_log("artist_stats.json", artists)

@dataclass
class Track:
    id: str
    title: str
    artist: str
    duration: float
    url: str
    source: str
    audio_path: str
    video_path: str
    ready: bool = False
    last_played: Optional[datetime] = None
    play_count: int = 0
    
    def to_dict(self) -> Dict:
        data = asdict(self)
        if self.last_played:
            data['last_played'] = self.last_played.isoformat()
        return data

class PlayerState(Enum):
    IDLE = auto()
    BUFFERING = auto()
    PLAYING = auto()
    TRANSITION = auto()
    ERROR = auto()
    SHUTDOWN = auto()

DEFAULT_CONFIG = {
    "system": {
        "log_file": "logs/hsyst_radio.log",
        "played_file": "data/played.json",
        "stats_file": "data/stats.json",
        "config_file": "config/config.json",
        "temp_dir": "temp",
        "cache_dir": "cache",
        "video_dir": "videos",
        "pid_file": "temp/hsyst_radio.pid",
        "max_history": 500,
        "buffer_size": MIN_BUFFER_SIZE,
        "max_cache_size": 20,
        "track_buffer_size": MAX_QUEUE_SIZE,
        "preload_ahead": 5,
        "cleanup_interval": 3600,
    },
    "stream": {
        "width": 1920,
        "height": 1080,
        "fps": 30,
        "bitrate": "5000k",
        "audio_bitrate": "256k",
        "crf": 18,
        "preset": "veryfast",
        "threads": 4,
        "keyframe_interval": 60,
        "rtmp_url": "rtmp://rtmp.livepub.hsyst.xyz/live",
        "stream_key": "open",
        "crossfade_duration": TRANSITION_DURATION,
    },
    "player": {
        "min_duration": 120,
        "max_duration": 3600,  # 1 hour max duration
        "max_consecutive_artists": 2,
        "artist_cooldown": 5,
        "track_cooldown": 30,
        "search_timeout": 10,
        "retry_delay": 2,
        "min_buffer_size": MIN_BUFFER_SIZE,
        "max_empty_retries": 3,
        "buffer_refill_threshold": 3,
        "ffmpeg_restart_delay": FFMPEG_RESTART_DELAY,
        "playback_end_margin": PLAYBACK_END_MARGIN,
    },
    "visuals": {
        "transition_duration": TRANSITION_DURATION,
        "font_size_title": 72,
        "font_size_artist": 54,
        "font_size_info": 36,
        "margin": 50,
        "text_color": "white",
        "highlight_color": "#00FFCC",
        "background_colors": [
            "#1E90FF", "#32CD32", "#FF4500", "#9932CC",
            "#FF1493", "#00CED1", "#FFD700"
        ],
        "font_file": "DejaVuSans-Bold.ttf",
        "logo_text": "HSYST RADIO",
        "credits_text": "Criado por op3n",
        "live_indicator": "• AO VIVO •",
        "box_opacity": 0.7,
        "box_border": 10,
    },
    "search": {
        "initial_results": 5,
        "max_results": 50,
        "backoff_factor": 2,
        "max_retries": 3,
        "user_agent": DEFAULT_USER_AGENT,
        "search_url": "https://soundcloud.com/search/sounds?q=",
        "timeout": 15,
        "download_timeout": 300,
        "min_request_interval": MIN_REQUEST_INTERVAL,
        "max_request_interval": MAX_REQUEST_INTERVAL,
        "adaptive_delay_increment": ADAPTIVE_DELAY_INCREMENT,
        "adaptive_delay_decrement": ADAPTIVE_DELAY_DECREMENT,
        "max_consecutive_failures": MAX_CONSECUTIVE_FAILURES,
    },
    "queries": [
        "Monte Zion",
        "Mama Quilla",
        "Mato Seco"
    ]
}

class ConfigManager:
    def __init__(self, base_dir: str, logger: logging.Logger):
        self.base_dir = base_dir
        self.logger = logger
        self.config = self._load_default_config()
        self.last_modified = 0
        self.watcher_thread = None
        self.running = False
        self.lock = threading.Lock()
        self._ensure_directories()
        
    def _ensure_directories(self) -> None:
        dirs = [
            os.path.join(self.base_dir, "logs"),
            os.path.join(self.base_dir, "logs", "json"),
            os.path.join(self.base_dir, "data"),
            os.path.join(self.base_dir, "config"),
            os.path.join(self.base_dir, "cache"),
            os.path.join(self.base_dir, "temp"),
            os.path.join(self.base_dir, "videos"),
        ]
        for dir_path in dirs:
            os.makedirs(dir_path, exist_ok=True)
    
    def _load_default_config(self) -> Dict:
        config = DEFAULT_CONFIG.copy()
        for section in ["system", "stream", "player", "visuals", "search"]:
            for key, value in config[section].items():
                if isinstance(value, str) and any(x in value for x in ["logs/", "data/", "config/", "cache/", "temp/", "videos/"]):
                    config[section][key] = os.path.join(self.base_dir, value)
        return config
    
    def _load_config_file(self) -> Optional[Dict]:
        config_file = self.config["system"]["config_file"]
        if not os.path.exists(config_file):
            self.logger.info("No config file found, using defaults")
            return None
        try:
            with open(config_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading config file: {e}")
            return None
    
    def _save_config_file(self, config: Dict) -> bool:
        config_file = self.config["system"]["config_file"]
        try:
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=4)
            return True
        except Exception as e:
            self.logger.error(f"Error saving config file: {e}")
            return False
    
    def _deep_merge(self, base: Dict, update: Dict) -> Dict:
        for key, value in update.items():
            if isinstance(value, dict) and key in base:
                base[key] = self._deep_merge(base.get(key, {}), value)
            else:
                base[key] = value
        return base
    
    def load(self) -> Dict:
        file_config = self._load_config_file()
        with self.lock:
            if file_config:
                self.config = self._deep_merge(self._load_default_config(), file_config)
            else:
                self._save_config_file(self.config)
            if os.path.exists(self.config["system"]["config_file"]):
                self.last_modified = os.path.getmtime(self.config["system"]["config_file"])
            return self.config.copy()
    
    def get(self, *keys) -> Any:
        with self.lock:
            result = self.config
            for key in keys:
                if isinstance(result, dict):
                    result = result.get(key)
                else:
                    return None
            return result
    
    def update(self, section: str, updates: Dict) -> bool:
        with self.lock:
            if section not in self.config:
                return False
            self.config[section].update(updates)
            return self._save_config_file(self.config)
    
    def start_watcher(self) -> None:
        if self.watcher_thread and self.watcher_thread.is_alive():
            return
        self.running = True
        self.watcher_thread = threading.Thread(
            target=self._watch_config,
            daemon=True,
            name="ConfigWatcher"
        )
        self.watcher_thread.start()
    
    def stop_watcher(self) -> None:
        self.running = False
        if self.watcher_thread:
            self.watcher_thread.join(timeout=2)
    
    def _watch_config(self) -> None:
        config_file = self.config["system"]["config_file"]
        while self.running:
            try:
                if os.path.exists(config_file):
                    current_modified = os.path.getmtime(config_file)
                    if current_modified > self.last_modified:
                        self.logger.info("Configuration file modified, reloading...")
                        self.load()
                        self.last_modified = current_modified
                time.sleep(CONFIG_RELOAD_INTERVAL)
            except Exception as e:
                self.logger.error(f"Config watcher error: {e}")
                time.sleep(CONFIG_RELOAD_INTERVAL * 2)

class TrackManager:
    def __init__(self, config: ConfigManager, logger: logging.Logger, json_logger: JsonLogger):
        self.config = config
        self.logger = logger
        self.json_logger = json_logger
        self.played_tracks: Deque[Track] = deque(maxlen=self.config.get("system", "max_history"))
        self.track_stats = defaultdict(int)
        self.artist_stats = defaultdict(int)
        self.removed_tracks = set()
        self.last_artists = deque(maxlen=self.config.get("player", "max_consecutive_artists"))
        self.lock = threading.Lock()
        self._load_data()
    
    def _load_data(self) -> None:
        try:
            played_file = self.config.get("system", "played_file")
            if os.path.exists(played_file):
                with open(played_file, 'r') as f:
                    history = json.load(f)
                    with self.lock:
                        self.played_tracks = deque(
                            [self._dict_to_track(track) for track in history if isinstance(track, dict)],
                            maxlen=self.config.get("system", "max_history")
                        )
            
            stats_file = self.config.get("system", "stats_file")
            if os.path.exists(stats_file):
                with open(stats_file, 'r') as f:
                    stats = json.load(f)
                    with self.lock:
                        self.track_stats = defaultdict(int, stats.get("tracks", {}))
                        self.artist_stats = defaultdict(int, stats.get("artists", {}))
                        self.json_logger.log_artists(self.artist_stats)
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
    
    def _dict_to_track(self, data: Dict) -> Track:
        return Track(
            id=data.get("id", ""),
            title=data.get("title", "Unknown Title"),
            artist=data.get("artist", "Unknown Artist"),
            duration=data.get("duration", 0),
            url=data.get("url", ""),
            source=data.get("source", ""),
            audio_path=data.get("audio_path", ""),
            video_path=data.get("video_path", ""),
            ready=data.get("ready", False),
            last_played=datetime.fromisoformat(data["last_played"]) if data.get("last_played") else None,
            play_count=data.get("play_count", 0)
        )
    
    def save_data(self) -> bool:
        try:
            with self.lock:
                played_copy = [self._track_to_dict(track) for track in self.played_tracks]
                track_stats_copy = dict(self.track_stats)
                artist_stats_copy = dict(self.artist_stats)
            
            with open(self.config.get("system", "played_file"), 'w') as f:
                json.dump(played_copy, f, indent=2)
            
            with open(self.config.get("system", "stats_file"), 'w') as f:
                json.dump({
                    "tracks": track_stats_copy,
                    "artists": artist_stats_copy,
                    "last_update": datetime.now().isoformat(),
                    "total_plays": sum(track_stats_copy.values())
                }, f, indent=2)
            
            self.json_logger.log_history(played_copy)
            self.json_logger.log_artists(artist_stats_copy)
            return True
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            return False
    
    def _track_to_dict(self, track: Track) -> Dict:
        return track.to_dict()
    
    def add_played_track(self, track: Track) -> None:
        if not track or not track.id or not track.artist:
            return
            
        with self.lock:
            track.last_played = datetime.now()
            track.play_count = self.track_stats.get(track.id, 0) + 1
            self.played_tracks.append(track)
            self.track_stats[track.id] += 1
            self.artist_stats[track.artist] += 1
            self.last_artists.append(track.artist)
            
            # Update JSON logs
            self.json_logger.log_current_track(track.to_dict())
            
            if len(self.played_tracks) % 10 == 0:
                self.save_data()
    
    def is_recent_artist(self, artist: str) -> bool:
        with self.lock:
            return artist in self.last_artists
    
    def is_recent_track(self, track_id: str) -> bool:
        with self.lock:
            for track in self.played_tracks:
                if track.id == track_id:
                    cooldown = self.config.get("player", "track_cooldown") * 60
                    return (datetime.now() - track.last_played).total_seconds() < cooldown
            return False
    
    def get_cached_tracks(self) -> List[Track]:
        cached = []
        cache_dir = self.config.get("system", "cache_dir")
        video_dir = self.config.get("system", "video_dir")
        
        try:
            with self.lock:
                cached_info = {track.id: track for track in self.played_tracks if track.id}
            
            for file in os.listdir(cache_dir):
                if file.endswith(".mp3"):
                    track_id = file[:-4]
                    if track_id in cached_info:
                        track = cached_info[track_id]
                        if os.path.exists(track.audio_path) and os.path.exists(track.video_path):
                            cached.append(track)
                    else:
                        audio_path = os.path.join(cache_dir, file)
                        video_path = os.path.join(video_dir, f"{track_id}.mp4")
                        if os.path.exists(audio_path) and os.path.exists(video_path):
                            track = Track(
                                id=track_id,
                                title=track_id,
                                artist="Unknown Artist",
                                duration=180,
                                url=f"file://{audio_path}",
                                source="cache",
                                audio_path=audio_path,
                                video_path=video_path,
                                ready=True
                            )
                            cached.append(track)
            
            cached.sort(key=lambda x: self.track_stats.get(x.id, 0))
            
            max_cache = self.config.get("system", "max_cache_size")
            if len(cached) > max_cache:
                for track in cached[max_cache:]:
                    try:
                        os.remove(track.audio_path)
                        os.remove(track.video_path)
                        with self.lock:
                            self.removed_tracks.add(track.id)
                        self.logger.info(f"Removed track from cache: {track.id}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning cache: {e}")
                cached = cached[:max_cache]
                
        except Exception as e:
            self.logger.error(f"Error reading cache: {e}")
        
        return cached
    
    def cleanup_cache(self) -> None:
        cache_dir = self.config.get("system", "cache_dir")
        video_dir = self.config.get("system", "video_dir")
        
        try:
            with self.lock:
                valid_files = {track.id for track in self.played_tracks if track.id}
            
            for file in os.listdir(cache_dir):
                if file.endswith(".mp3") and file[:-4] not in valid_files:
                    try:
                        os.remove(os.path.join(cache_dir, file))
                        video_file = os.path.join(video_dir, f"{file[:-4]}.mp4")
                        if os.path.exists(video_file):
                            os.remove(video_file)
                        self.logger.info(f"Removed orphaned cache file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error removing orphaned file {file}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error during cache cleanup: {e}")

class SoundCloudAPI:
    def __init__(self, config: ConfigManager, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.config.get("search", "user_agent"),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://soundcloud.com/",
            "DNT": "1"
        })
        self.current_query = ""
        self.search_lock = threading.Lock()
        self.search_limit = self.config.get("search", "initial_results")
        self.last_request_time = 0
        self.current_delay = INITIAL_REQUEST_DELAY
        self.consecutive_failures = 0
        self.request_history = OrderedDict()
        self.max_history = 50
        self.rate_limit_delay = 1
        self.user_agents = self._generate_user_agents()
        self.current_agent_index = 0
    
    def _generate_user_agents(self) -> List[str]:
        """Generate a list of user agents to rotate through"""
        base_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]
        
        # Generate variations by slightly modifying version numbers
        variations = []
        for agent in base_agents:
            for i in range(1, 4):
                modified = agent.replace("91.0", f"{91+i}.0").replace("89.0", f"{89+i}.0")
                variations.append(modified)
        
        return base_agents + variations
    
    def _rotate_user_agent(self) -> None:
        """Rotate to the next user agent in the list"""
        self.current_agent_index = (self.current_agent_index + 1) % len(self.user_agents)
        self.session.headers.update({
            "User-Agent": self.user_agents[self.current_agent_index]
        })
    
    def _record_request(self, url: str, success: bool) -> None:
        """Record the request in history for rate limiting analysis"""
        timestamp = time.time()
        self.request_history[timestamp] = {
            "url": url,
            "success": success,
            "delay": self.current_delay
        }
        
        # Trim old history
        while len(self.request_history) > self.max_history:
            self.request_history.popitem(last=False)
    
    def _calculate_request_delay(self) -> float:
        """Calculate the appropriate delay between requests based on recent history"""
        min_delay = self.config.get("search", "min_request_interval")
        max_delay = self.config.get("search", "max_request_interval")
        
        # If we've had consecutive failures, increase delay significantly
        if self.consecutive_failures >= self.config.get("search", "max_consecutive_failures"):
            self.current_delay = min(self.current_delay * 2, max_delay)
            self.logger.warning(f"Multiple consecutive failures, increasing delay to {self.current_delay:.1f}s")
            return self.current_delay
        
        # Calculate success rate in recent requests
        success_count = sum(1 for req in self.request_history.values() if req["success"])
        total_recent = len(self.request_history)
        
        if total_recent > 0:
            success_rate = success_count / total_recent
            
            # Adjust delay based on success rate
            if success_rate < 0.5:
                # More than half failed - increase delay
                increment = self.config.get("search", "adaptive_delay_increment")
                self.current_delay = min(self.current_delay + increment, max_delay)
                self.logger.debug(f"Low success rate ({success_rate:.0%}), increasing delay to {self.current_delay:.1f}s")
            elif success_rate > 0.8 and self.current_delay > min_delay:
                # High success rate - decrease delay
                decrement = self.config.get("search", "adaptive_delay_decrement")
                self.current_delay = max(self.current_delay - decrement, min_delay)
                self.logger.debug(f"High success rate ({success_rate:.0%}), decreasing delay to {self.current_delay:.1f}s")
        
        return self.current_delay
    
    def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting by waiting the appropriate amount of time"""
        # Calculate time since last request
        time_since_last = time.time() - self.last_request_time
        required_delay = self._calculate_request_delay()
        
        # If we need to wait, do so
        if time_since_last < required_delay:
            wait_time = required_delay - time_since_last
            self.logger.debug(f"Rate limiting: waiting {wait_time:.1f}s before next request")
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def search_tracks(self, query: str, limit: int = None) -> List[str]:
        if not limit:
            limit = self.search_limit
            
        self.logger.debug(f"Searching SoundCloud for: {query} (limit: {limit})")
        self.current_query = query
        
        # Enforce rate limiting
        self._enforce_rate_limit()
        self._rotate_user_agent()
        
        try:
            url = self.config.get("search", "search_url") + query.replace(' ', '+')
            response = self.session.get(
                url,
                timeout=self.config.get("search", "timeout"),
                headers={
                    "Referer": "https://soundcloud.com/",
                    "X-Requested-With": "XMLHttpRequest" if random.random() > 0.7 else None
                }
            )
            
            # Check for rate limiting or blocking
            if response.status_code == 429:
                self.logger.warning("Rate limited by SoundCloud, increasing delay")
                self.consecutive_failures += 1
                self._record_request(url, False)
                return []
            
            response.raise_for_status()
            
            self.consecutive_failures = 0
            self._record_request(url, True)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            links = []
            
            # Find track links in the search results
            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.count('/') == 2 and not href.startswith(('/you/', '/search', '/discover')):
                    full_url = f"https://soundcloud.com{href}"
                    if full_url not in links:
                        links.append(full_url)
                        if len(links) >= limit:
                            break
                            
            self.logger.debug(f"Found {len(links)} tracks for query: {query}")
            return links
            
        except requests.RequestException as e:
            self.consecutive_failures += 1
            self._record_request(url, False)
            self.logger.error(f"SoundCloud search error for '{query}': {e}")
            return []
        except Exception as e:
            self.consecutive_failures += 1
            self._record_request(url, False)
            self.logger.error(f"Unexpected error during search: {e}")
            return []
    
    def get_track_info(self, track_url: str) -> Optional[Track]:
        for attempt in range(MAX_RETRIES):
            try:
                # Enforce rate limiting
                self._enforce_rate_limit()
                self._rotate_user_agent()
                
                cmd = [
                    'yt-dlp',
                    '--dump-json',
                    '--no-playlist',
                    '--no-warnings',
                    '--referer', 'https://soundcloud.com/',
                    track_url
                ]
                
                self.logger.debug(f"Fetching track info for {track_url}")
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.config.get("search", "timeout"),
                    check=True
                )
                
                info = json.loads(result.stdout)
                track = Track(
                    id=str(info.get('id', '')),
                    title=info.get('title', 'Unknown Title'),
                    artist=info.get('uploader', 'Unknown Artist'),
                    duration=info.get('duration', 0),
                    url=track_url,
                    source=self.current_query,
                    audio_path="",
                    video_path="",
                    ready=False
                )
                
                self.consecutive_failures = 0
                self._record_request(track_url, True)
                self.logger.debug(f"Track info retrieved: {track.id}")
                return track
                
            except subprocess.TimeoutExpired:
                self.consecutive_failures += 1
                self._record_request(track_url, False)
                self.logger.warning(f"Timeout getting track info from {track_url}, attempt {attempt + 1}/{MAX_RETRIES}")
            except subprocess.CalledProcessError as e:
                self.consecutive_failures += 1
                self._record_request(track_url, False)
                self.logger.error(f"yt-dlp error for {track_url}: {e.stderr}")
            except Exception as e:
                self.consecutive_failures += 1
                self._record_request(track_url, False)
                self.logger.error(f"Error getting track info from {track_url}: {e}")
                
            time.sleep(RETRY_DELAY * (attempt + 1))
            
        return None
    
    def download_track(self, track: Track) -> Optional[Track]:
        if not track or not track.url:
            self.logger.error("Invalid track or URL")
            return None
        
        cache_dir = self.config.get("system", "cache_dir")
        video_dir = self.config.get("system", "video_dir")
        audio_path = os.path.join(cache_dir, f"{track.id}.mp3")
        video_path = os.path.join(video_dir, f"{track.id}.mp4")
        
        if os.path.exists(audio_path) and os.path.exists(video_path):
            track.audio_path = audio_path
            track.video_path = video_path
            track.ready = True
            self.logger.info(f"Using cached track: {track.id}")
            return track
        
        audio_downloaded = False
        for attempt in range(MAX_RETRIES):
            try:
                # Enforce rate limiting for downloads too
                self._enforce_rate_limit()
                self._rotate_user_agent()
                
                cmd = [
                    'yt-dlp',
                    '-x', '--audio-format', 'mp3',
                    '--audio-quality', '192K',
                    '--no-playlist',
                    '--no-warnings',
                    '--referer', 'https://soundcloud.com/',
                    '--throttled-rate', '100K',  # Limit download speed
                    '-o', audio_path,
                    track.url
                ]
                
                self.logger.debug(f"Downloading track {track.id}: {' '.join(cmd)}")
                result = subprocess.run(
                    cmd,
                    check=True,
                    timeout=self.config.get("search", "download_timeout"),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                if os.path.exists(audio_path):
                    audio_downloaded = True
                    self.consecutive_failures = 0
                    self._record_request(track.url, True)
                    break
                else:
                    self.logger.error(f"Download failed for {track.id}, no file created")
                    self.consecutive_failures += 1
                    self._record_request(track.url, False)
                    
            except subprocess.TimeoutExpired:
                self.consecutive_failures += 1
                self._record_request(track.url, False)
                self.logger.warning(f"Timeout downloading track {track.id}, attempt {attempt + 1}/{MAX_RETRIES}")
            except subprocess.CalledProcessError as e:
                self.consecutive_failures += 1
                self._record_request(track.url, False)
                self.logger.error(f"Download failed for {track.id}: {e.stderr}")
            except Exception as e:
                self.consecutive_failures += 1
                self._record_request(track.url, False)
                self.logger.error(f"Unexpected error downloading {track.id}: {e}")
                
            time.sleep(RETRY_DELAY * (attempt + 1))
        
        if not audio_downloaded:
            return None
        
        if not self._create_video(track, audio_path, video_path):
            try:
                os.remove(audio_path)
            except:
                pass
            return None
        
        track.audio_path = audio_path
        track.video_path = video_path
        track.ready = True
        return track
    
    def _create_video(self, track: Track, audio_path: str, video_path: str) -> bool:
        try:
            width = self.config.get("stream", "width")
            height = self.config.get("stream", "height")
            fps = self.config.get("stream", "fps")
            
            filters = self._generate_track_filters(track)
            
            cmd = [
                'ffmpeg', '-y',
                '-i', audio_path,
                '-f', 'lavfi',
                '-i', f'color=c=black:s={width}x{height}:r={fps}',
                '-filter_complex', filters,
                '-map', '[v]',
                '-map', '0:a',
                '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '18',
                '-c:a', 'aac', '-b:a', '192k',
                '-pix_fmt', 'yuv420p',
                '-t', str(track.duration),  # Explicit duration
                video_path
            ]
            
            self._run_ffmpeg_command(cmd, "Creating track video")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating video for {track.id}: {str(e)}")
            return False
    
    def _run_ffmpeg_command(self, cmd: List[str], description: str) -> bool:
        self.logger.debug(f"{description}: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"FFmpeg command failed: {e.stderr}")
            raise
        except Exception as e:
            self.logger.error(f"Error running FFmpeg command: {str(e)}")
            raise
    
    def _get_font_path(self) -> str:
        font_file = self.config.get("visuals", "font_file")
        
        if os.path.isabs(font_file) and os.path.exists(font_file):
            return font_file
            
        system_fonts = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
        ]
        
        for font in system_fonts:
            if os.path.exists(font):
                return font
                
        try:
            result = subprocess.run(
                ['fc-match', '-f', '%{file}', 'sans:bold'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and os.path.exists(result.stdout.strip()):
                return result.stdout.strip()
        except:
            pass
            
        return "Arial"
    
    def _generate_track_filters(self, track: Track) -> str:
        width = self.config.get("stream", "width")
        height = self.config.get("stream", "height")
        margin = self.config.get("visuals", "margin")
        bg_color = random.choice(self.config.get("visuals", "background_colors"))
        font_file = self._get_font_path()
        transition_duration = self.config.get("visuals", "transition_duration")
        box_opacity = self.config.get("visuals", "box_opacity")
        box_border = self.config.get("visuals", "box_border")
        
        def escape_text(text: str) -> str:
            return text.replace(":", "\\:").replace("'", "\\'").replace("[", "\\[").replace("]", "\\]")
        
        title = escape_text(track.title)
        artist = escape_text(track.artist)
        
        # Fade in/out effects
        fade_in = f"if(lt(t,{transition_duration}),t/{transition_duration},1)"
        fade_out = f"if(gt(t,{track.duration}-{transition_duration}),({track.duration}-t)/{transition_duration},1)"
        
        # Base video source
        base = f"color=c={bg_color}:s={width}x{height}:r={self.config.get('stream', 'fps')}"
        
        # Build all visual elements
        filters = [
            # Track title (bottom left, large)
            f"drawtext=text='{title}':"
            f"fontfile='{font_file}':"
            f"fontcolor={self.config.get('visuals', 'text_color')}:"
            f"fontsize={self.config.get('visuals', 'font_size_title')}:"
            f"x={margin}:y=h-text_h-{margin}:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{fade_in}*{fade_out}'",
            
            # Artist name (above title, slightly smaller)
            f"drawtext=text='{artist}':"
            f"fontfile='{font_file}':"
            f"fontcolor={self.config.get('visuals', 'highlight_color')}:"
            f"fontsize={self.config.get('visuals', 'font_size_artist')}:"
            f"x={margin}:y=h-text_h-{margin}-{self.config.get('visuals', 'font_size_title')}-30:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{fade_in}*{fade_out}'",
            
            # Station logo (bottom right)
            f"drawtext=text='{self.config.get('visuals', 'logo_text')}':"
            f"fontfile='{font_file}':"
            f"fontcolor={self.config.get('visuals', 'highlight_color')}:"
            f"fontsize={self.config.get('visuals', 'font_size_info')}:"
            f"x=w-text_w-{margin}:y=h-text_h-{margin}:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{fade_in}*{fade_out}'",
            
            # Credits (top left)
            f"drawtext=text='{self.config.get('visuals', 'credits_text')}':"
            f"fontfile='{font_file}':"
            f"fontcolor=white:"
            f"fontsize={self.config.get('visuals', 'font_size_info')}:"
            f"x={margin}:y={margin}:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{fade_in}*{fade_out}'",
            
            # Live indicator (top right)
            f"drawtext=text='{self.config.get('visuals', 'live_indicator')}':"
            f"fontfile='{font_file}':"
            f"fontcolor=red:fontsize=36:"
            f"x=w-text_w-{margin}:y={margin}:"
            f"box=1:boxcolor=black@{box_opacity}:boxborderw={box_border}:"
            f"alpha='{fade_in}*{fade_out}'"
        ]
        
        # Combine all filters
        filter_chain = base
        for f in filters:
            filter_chain += f",{f}"
        
        return f"{filter_chain}[v]"
    
    def _adjust_search_limit(self, success: bool) -> None:
        max_limit = self.config.get("search", "max_results")
        initial_limit = self.config.get("search", "initial_results")
        backoff = self.config.get("search", "backoff_factor")
        
        with self.search_lock:
            if success and self.search_limit < max_limit:
                self.search_limit = min(self.search_limit * backoff, max_limit)
                self.logger.debug(f"Increased search limit to {self.search_limit}")
            elif not success:
                self.search_limit = initial_limit
                self.logger.debug(f"Reset search limit to initial value")

class AudioStreamer:
    def __init__(self, config: ConfigManager, track_manager: TrackManager, logger: logging.Logger, json_logger: JsonLogger):
        self.config = config
        self.track_manager = track_manager
        self.logger = logger
        self.json_logger = json_logger
        self.stream_process = None
        self.running = False
        self.current_track = None
        self.next_track = None
        self.ffmpeg_lock = threading.Lock()
        self.track_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        self.stream_start_time = 0
        self.current_track_duration = 0
        self.state = PlayerState.IDLE
        self.state_lock = threading.Lock()
        self.empty_buffer_retries = 0
        self.stream_thread = None
        self.stream_active = threading.Event()
        self.stream_ended = threading.Event()
        self.monitor_thread = None
        self.stream_restart_pending = False
    
    def set_state(self, state: PlayerState) -> None:
        with self.state_lock:
            old_state = self.state
            self.state = state
            if old_state != state:
                self.logger.info(f"Player state changed: {old_state.name} -> {state.name}")
                if state == PlayerState.PLAYING:
                    self.stream_active.set()
                else:
                    self.stream_active.clear()
    
    def get_state(self) -> PlayerState:
        with self.state_lock:
            return self.state
    
    def start(self) -> None:
        if self.running:
            return
            
        self.running = True
        self.stream_thread = threading.Thread(
            target=self._stream_loop,
            daemon=True,
            name="StreamThread"
        )
        self.stream_thread.start()
    
    def stop(self) -> None:
        self.running = False
        self._stop_stream()
        
        if self.stream_thread:
            self.stream_thread.join(timeout=2)
            
        while not self.track_queue.empty():
            try:
                self.track_queue.get_nowait()
            except queue.Empty:
                break
    
    def enqueue_track(self, track: Track) -> bool:
        if not track or not track.ready or not os.path.exists(track.video_path):
            self.logger.error(f"Cannot enqueue invalid or unready track: {track}")
            return False
            
        try:
            with self.state_lock:
                queued_ids = set()
                temp_queue = queue.Queue()
                
                while not self.track_queue.empty():
                    t = self.track_queue.get_nowait()
                    queued_ids.add(t.id)
                    temp_queue.put(t)
                
                while not temp_queue.empty():
                    self.track_queue.put(temp_queue.get_nowait())
                
                if track.id in queued_ids:
                    self.logger.debug(f"Track {track.id} already in queue, skipping enqueue")
                    return False
                    
            self.track_queue.put(track, timeout=1)
            self.logger.info(f"Enqueued track: {track.id}")
            
            # Update queue in JSON log
            self._update_queue_log()
            return True
            
        except queue.Full:
            self.logger.warning("Track queue is full, removing oldest track")
            try:
                self.track_queue.get_nowait()
                self.track_queue.put(track, timeout=1)
                self.logger.info(f"Enqueued track after clearing: {track.id}")
                self._update_queue_log()
                return True
            except Exception as e:
                self.logger.error(f"Failed to enqueue track after clearing: {e}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error enqueueing track: {e}")
            return False
    
    def _update_queue_log(self) -> None:
        """Update the JSON log with current queue state"""
        try:
            queue_list = []
            temp_queue = queue.Queue()
            
            # Copy current queue
            while not self.track_queue.empty():
                track = self.track_queue.get_nowait()
                queue_list.append(track.to_dict())
                temp_queue.put(track)
            
            # Restore queue
            while not temp_queue.empty():
                self.track_queue.put(temp_queue.get_nowait())
            
            self.json_logger.log_queue(queue_list)
        except Exception as e:
            self.logger.error(f"Error updating queue log: {e}")
    
    def _get_rtmp_url(self) -> str:
        rtmp_url = self.config.get("stream", "rtmp_url")
        stream_key = self.config.get("stream", "stream_key")
        
        if not rtmp_url.endswith('/'):
            rtmp_url += '/'
            
        return f"{rtmp_url}{stream_key}"
    
    def _start_stream(self, input_file: str) -> bool:
        try:
            cmd = [
                'ffmpeg',
                '-re',
                '-i', input_file,
                '-c:v', 'libx264',
                '-preset', self.config.get("stream", "preset"),
                '-b:v', self.config.get("stream", "bitrate"),
                '-g', str(self.config.get("stream", "keyframe_interval")),
                '-c:a', 'aac',
                '-b:a', self.config.get("stream", "audio_bitrate"),
                '-f', 'flv',
                self._get_rtmp_url()
            ]
            
            with self.ffmpeg_lock:
                if self.stream_process:
                    self._stop_stream()
                
                self.logger.debug(f"Starting FFmpeg with command: {' '.join(cmd)}")
                
                # Use Popen with pipes for better process management
                self.stream_process = subprocess.Popen(
                    cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    bufsize=1
                )
                
                # Start monitoring thread
                self.monitor_thread = threading.Thread(
                    target=self._monitor_ffmpeg_process,
                    daemon=True
                )
                self.monitor_thread.start()
                
                # Wait briefly to check if process started
                time.sleep(0.5)
                if self.stream_process.poll() is not None:
                    stderr = self.stream_process.stderr.read() if self.stream_process.stderr else "Unknown error"
                    self.logger.error(f"FFmpeg failed to start: {stderr}")
                    return False
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to start stream: {e}")
            return False
    
    def _monitor_ffmpeg_process(self) -> None:
        """Monitor the FFmpeg process and handle unexpected termination"""
        while self.running and self.stream_process:
            retcode = self.stream_process.poll()
            if retcode is not None:
                # Process has terminated
                stderr = self.stream_process.stderr.read() if self.stream_process.stderr else ""
                self.logger.warning(f"FFmpeg process terminated with code {retcode}")
                if stderr:
                    self.logger.error(f"FFmpeg error output: {stderr}")
                
                if not self.stream_ended.is_set():
                    self.logger.warning("FFmpeg terminated unexpectedly during playback")
                    self.set_state(PlayerState.ERROR)
                    self.stream_ended.set()
                break
            
            # Check for error messages in stderr
            try:
                ready, _, _ = select.select([self.stream_process.stderr], [], [], 0.1)
                if ready:
                    line = self.stream_process.stderr.readline()
                    if line:
                        if "error" in line.lower() or "failed" in line.lower():
                            self.logger.error(f"FFmpeg error: {line.strip()}")
            except (ValueError, OSError):
                pass
            
            time.sleep(FFMPEG_MONITOR_INTERVAL)
    
    def _stop_stream(self) -> None:
        with self.ffmpeg_lock:
            if self.stream_process:
                try:
                    self.logger.debug("Stopping FFmpeg process")
                    self.stream_process.terminate()
                    
                    # Wait for process to terminate
                    try:
                        self.stream_process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        self.logger.warning("FFmpeg didn't terminate, killing it")
                        self.stream_process.kill()
                        self.stream_process.wait()
                except Exception as e:
                    self.logger.error(f"Error stopping stream: {e}")
                finally:
                    self.stream_process = None
    
    def _stream_track(self, track: Track) -> bool:
        if not track or not track.video_path or not os.path.exists(track.video_path):
            self.logger.error(f"Invalid track or missing video file: {track}")
            return False
        
        self.set_state(PlayerState.PLAYING)
        self.stream_start_time = time.time()
        self.current_track_duration = track.duration
        self.stream_ended.clear()
        
        if not self._start_stream(track.video_path):
            self.set_state(PlayerState.ERROR)
            return False
        
        # Calculate expected end time with buffer
        expected_end_time = self.stream_start_time + track.duration + self.config.get("player", "playback_end_margin")
        
        # Wait for the track to finish or be interrupted
        while self.running and not self.stream_ended.is_set():
            elapsed = time.time() - self.stream_start_time
            
            # Check if we've exceeded expected duration
            if time.time() > expected_end_time:
                self.logger.info(f"Track playback completed after {elapsed:.1f}s (duration: {track.duration}s)")
                break
            
            # Check if FFmpeg is still running
            if self.stream_process and self.stream_process.poll() is not None:
                self.logger.warning("FFmpeg process terminated during playback")
                self.set_state(PlayerState.ERROR)
                break
            
            time.sleep(0.1)
        
        # Ensure stream is stopped
        self._stop_stream()
        
        # Small delay before next track to allow system to recover
        time.sleep(self.config.get("player", "ffmpeg_restart_delay"))
        
        return True
    
    def _handle_empty_buffer(self) -> bool:
        max_retries = self.config.get("player", "max_empty_retries")
        
        if self.empty_buffer_retries >= max_retries:
            self.logger.error("Max empty buffer retries reached")
            self.empty_buffer_retries = 0
            return False
        
        self.empty_buffer_retries += 1
        self.logger.warning(f"Empty buffer, retrying ({self.empty_buffer_retries}/{max_retries})")
        return True
    
    def _stream_loop(self) -> None:
        min_buffer = self.config.get("player", "min_buffer_size")
        self.logger.info(f"Waiting for initial buffer to fill (needed: {min_buffer})")
        
        # Wait for initial buffer to fill
        while self.running and self.track_queue.qsize() < min_buffer:
            self.logger.debug(f"Current buffer size: {self.track_queue.qsize()}")
            time.sleep(BUFFER_CHECK_INTERVAL)
        
        self.set_state(PlayerState.PLAYING)
        self.empty_buffer_retries = 0
        
        while self.running:
            try:
                # Get current track
                if self.current_track is None:
                    try:
                        self.current_track = self.track_queue.get(timeout=1)
                        self.track_manager.add_played_track(self.current_track)
                        self.empty_buffer_retries = 0
                        self.logger.info(f"Playing track: {self.current_track.id}")
                    except queue.Empty:
                        if not self._handle_empty_buffer():
                            continue
                        continue
                
                # Try to get next track without waiting
                try:
                    self.next_track = self.track_queue.get_nowait()
                    self.track_manager.add_played_track(self.next_track)
                    self._update_queue_log()
                except queue.Empty:
                    self.next_track = None
                
                # Stream the current track
                success = self._stream_track(self.current_track)
                
                if not success:
                    self.logger.error("Failed to stream track, retrying...")
                    self.set_state(PlayerState.ERROR)
                    time.sleep(self.config.get("player", "retry_delay"))
                    continue
                
                # Move to next track
                self.current_track = self.next_track
                self.next_track = None
                
            except Exception as e:
                self.logger.error(f"Stream loop error: {e}")
                self.set_state(PlayerState.ERROR)
                time.sleep(self.config.get("player", "retry_delay"))
        
        self.set_state(PlayerState.SHUTDOWN)
        self._stop_stream()

class TrackBufferManager:
    def __init__(self, config: ConfigManager, track_manager: TrackManager, 
                 soundcloud: SoundCloudAPI, streamer: AudioStreamer, logger: logging.Logger):
        self.config = config
        self.track_manager = track_manager
        self.soundcloud = soundcloud
        self.streamer = streamer
        self.logger = logger
        self.running = False
        self.buffer_thread = None
        self.preload_thread = None
        self.cleanup_thread = None
        self.active_downloads = set()
        self.download_lock = threading.Lock()
        self.buffer_refill_threshold = self.config.get("player", "buffer_refill_threshold")
        self.queued_track_ids = set()
        self.executor = ThreadPoolExecutor(max_workers=PRELOAD_WORKERS)
    
    def start(self) -> None:
        if self.running:
            return
            
        self.running = True
        
        self.buffer_thread = threading.Thread(
            target=self._buffer_loop,
            daemon=True,
            name="BufferThread"
        )
        self.buffer_thread.start()
        
        self.preload_thread = threading.Thread(
            target=self._preload_loop,
            daemon=True,
            name="PreloadThread"
        )
        self.preload_thread.start()
        
        self.cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="CleanupThread"
        )
        self.cleanup_thread.start()
    
    def stop(self) -> None:
        self.running = False
        self.executor.shutdown(wait=False)
        
        if self.buffer_thread:
            self.buffer_thread.join(timeout=2)
        if self.preload_thread:
            self.preload_thread.join(timeout=2)
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=2)
    
    def _find_next_track(self) -> Optional[Track]:
        max_attempts = 10
        attempts = 0
        
        while attempts < max_attempts and self.running:
            attempts += 1
            
            cached_tracks = self.track_manager.get_cached_tracks()
            valid_cached = [
                t for t in cached_tracks 
                if not self.track_manager.is_recent_track(t.id) and 
                not self.track_manager.is_recent_artist(t.artist) and 
                t.ready and t.id not in self.queued_track_ids
            ]
            
            if valid_cached:
                track = random.choice(valid_cached)
                self.logger.info(f"Selected cached track: {track.id}")
                return track
            
            query = self._get_next_query()
            track_urls = self.soundcloud.search_tracks(query)
            
            if not track_urls:
                self.logger.warning(f"No results for query: {query}")
                time.sleep(1)
                continue
            
            futures = []
            for url in track_urls:
                with self.download_lock:
                    if url in self.active_downloads:
                        continue
                    self.active_downloads.add(url)
                futures.append(self.executor.submit(self._process_track_url, url))
            
            valid_tracks = []
            for future in as_completed(futures):
                try:
                    track = future.result()
                    if track:
                        valid_tracks.append(track)
                except Exception as e:
                    self.logger.error(f"Error processing track: {e}")
            
            if valid_tracks:
                track = random.choice(valid_tracks)
                self.logger.info(f"Selected new track: {track.id}")
                return track
            
            time.sleep(1)
        
        self.logger.error("Failed to find a valid track after multiple attempts")
        return None
    
    def _process_track_url(self, url: str) -> Optional[Track]:
        try:
            track_info = self.soundcloud.get_track_info(url)
            if not track_info:
                return None
            
            min_duration = self.config.get("player", "min_duration")
            max_duration = self.config.get("player", "max_duration")
            if track_info.duration < min_duration or track_info.duration > max_duration:
                self.logger.debug(f"Skipping track due to duration: {track_info.duration}s")
                return None
            
            if self.track_manager.is_recent_track(track_info.id):
                self.logger.debug(f"Skipping recently played track: {track_info.id}")
                return None
            
            if self.track_manager.is_recent_artist(track_info.artist):
                self.logger.debug(f"Skipping recent artist: {track_info.artist}")
                return None
            
            if track_info.id in self.queued_track_ids:
                self.logger.debug(f"Skipping already queued track: {track_info.id}")
                return None
            
            downloaded_track = self.soundcloud.download_track(track_info)
            return downloaded_track if (downloaded_track and downloaded_track.ready) else None
            
        finally:
            with self.download_lock:
                self.active_downloads.discard(url)
    
    def _get_next_query(self) -> str:
        queries = self.config.get("queries")
        return random.choice(queries)
    
    def _buffer_loop(self) -> None:
        buffer_size = self.config.get("system", "track_buffer_size")
        min_buffer = self.config.get("player", "min_buffer_size")
        
        while self.running:
            try:
                current_size = self.streamer.track_queue.qsize()
                
                if current_size >= buffer_size:
                    time.sleep(BUFFER_CHECK_INTERVAL)
                    continue
                
                if current_size < min_buffer:
                    tracks_needed = min_buffer - current_size
                    self.logger.warning(f"Buffer below minimum, urgently adding {tracks_needed} tracks")
                    
                    for _ in range(tracks_needed):
                        track = self._find_next_track()
                        if track and self.streamer.enqueue_track(track):
                            with self.download_lock:
                                self.queued_track_ids.add(track.id)
                            self.logger.info(f"Added track to buffer: {track.id}")
                        else:
                            self.logger.error("Couldn't add track for urgent buffer fill")
                            time.sleep(BUFFER_CHECK_INTERVAL)
                            break
                    continue
                
                if current_size <= self.buffer_refill_threshold:
                    track = self._find_next_track()
                    if track and self.streamer.enqueue_track(track):
                        with self.download_lock:
                            self.queued_track_ids.add(track.id)
                        self.logger.info(f"Added track to buffer: {track.id}")
                    else:
                        self.logger.error("Failed to add track to buffer")
                        time.sleep(BUFFER_CHECK_INTERVAL)
                
                time.sleep(BUFFER_CHECK_INTERVAL)
                
            except Exception as e:
                self.logger.error(f"Buffer loop error: {e}")
                time.sleep(BUFFER_CHECK_INTERVAL * 2)
    
    def _preload_loop(self) -> None:
        preload_ahead = self.config.get("system", "preload_ahead")
        
        while self.running:
            try:
                with self.download_lock:
                    active_count = len(self.active_downloads)
                
                if active_count < preload_ahead:
                    query = self._get_next_query()
                    track_urls = self.soundcloud.search_tracks(query, limit=preload_ahead)
                    
                    for url in track_urls:
                        with self.download_lock:
                            if url in self.active_downloads:
                                continue
                            self.active_downloads.add(url)
                        
                        self.executor.submit(self._preload_track, url)
                
                time.sleep(BUFFER_CHECK_INTERVAL * 2)
                
            except Exception as e:
                self.logger.error(f"Preload loop error: {e}")
                time.sleep(BUFFER_CHECK_INTERVAL * 4)
    
    def _preload_track(self, url: str) -> None:
        try:
            track_info = self.soundcloud.get_track_info(url)
            if track_info:
                min_duration = self.config.get("player", "min_duration")
                max_duration = self.config.get("player", "max_duration")
                if track_info.duration < min_duration or track_info.duration > max_duration:
                    return
                
                if (self.track_manager.is_recent_track(track_info.id) or 
                    self.track_manager.is_recent_artist(track_info.artist)):
                    return
                
                if track_info.id in self.queued_track_ids:
                    return
                
                downloaded_track = self.soundcloud.download_track(track_info)
                if downloaded_track and downloaded_track.ready:
                    if self.streamer.enqueue_track(downloaded_track):
                        with self.download_lock:
                            self.queued_track_ids.add(downloaded_track.id)
                            
        except Exception as e:
            self.logger.error(f"Preload track error for {url}: {e}")
        finally:
            with self.download_lock:
                self.active_downloads.discard(url)
    
    def _cleanup_loop(self) -> None:
        cleanup_interval = self.config.get("system", "cleanup_interval")
        
        while self.running:
            try:
                self.track_manager.cleanup_cache()
                
                with self.download_lock:
                    current_queue_ids = set()
                    temp_queue = queue.Queue()
                    
                    while not self.streamer.track_queue.empty():
                        track = self.streamer.track_queue.get_nowait()
                        current_queue_ids.add(track.id)
                        temp_queue.put(track)
                    
                    while not temp_queue.empty():
                        self.streamer.track_queue.put(temp_queue.get_nowait())
                    
                    self.queued_track_ids = current_queue_ids
                
                time.sleep(cleanup_interval)
                
            except Exception as e:
                self.logger.error(f"Cleanup loop error: {e}")
                time.sleep(cleanup_interval / 2)

class HsystRadioPlayer:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.logger = setup_logging(self.base_dir)
        self.json_logger = JsonLogger(self.base_dir)
        self.config = ConfigManager(self.base_dir, self.logger)
        self.track_manager = TrackManager(self.config, self.logger, self.json_logger)
        self.soundcloud = SoundCloudAPI(self.config, self.logger)
        self.streamer = AudioStreamer(self.config, self.track_manager, self.logger, self.json_logger)
        self.buffer_manager = TrackBufferManager(
            self.config, self.track_manager, 
            self.soundcloud, self.streamer, self.logger
        )
        self.running = False
        
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
    
    def _handle_signal(self, signum, frame) -> None:
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def _write_pid_file(self) -> bool:
        pid_file = self.config.get("system", "pid_file")
        try:
            with open(pid_file, 'w') as f:
                f.write(str(os.getpid()))
            return True
        except Exception as e:
            self.logger.error(f"Error writing PID file: {e}")
            return False
    
    def _remove_pid_file(self) -> None:
        pid_file = self.config.get("system", "pid_file")
        try:
            if os.path.exists(pid_file):
                os.remove(pid_file)
        except Exception as e:
            self.logger.error(f"Error removing PID file: {e}")
    
    def start(self) -> None:
        if self.running:
            return
            
        self.logger.info(f"Starting HSYST Radio Player v{VERSION}")
        self.running = True
        
        if not self._write_pid_file():
            self.logger.error("Failed to write PID file, continuing anyway")
        
        self.config.load()
        self.config.start_watcher()
        self.buffer_manager.start()
        self.streamer.start()
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received, shutting down...")
            self.stop()
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")
            self.stop()
    
    def stop(self) -> None:
        if not self.running:
            return
            
        self.logger.info("Stopping HSYST Radio Player...")
        self.running = False
        
        self.streamer.stop()
        self.buffer_manager.stop()
        self.config.stop_watcher()
        
        self.track_manager.save_data()
        self._remove_pid_file()
        
        self.logger.info("HSYST Radio Player stopped")

def main():
    parser = argparse.ArgumentParser(
        description=f"HSYST RADIO - Professional Streamer v{VERSION}",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--daemon', action='store_true', help="Run as daemon")
    parser.add_argument('--stop', action='store_true', help="Stop running instance")
    parser.add_argument('--verbose', action='store_true', help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.stop:
        pid_file = DEFAULT_CONFIG["system"]["pid_file"]
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                os.kill(pid, signal.SIGTERM)
                print(f"Sent termination signal to process {pid}")
                return
            except Exception as e:
                print(f"Error stopping process: {e}")
        else:
            print("No running instance found")
        return
    
    player = HsystRadioPlayer()
    
    if args.verbose:
        for handler in player.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.DEBUG)
    
    if args.daemon:
        try:
            from daemonize import Daemonize
        except ImportError:
            print("Error: daemonize module not found. Install with: pip3 install daemonize")
            sys.exit(1)
            
        daemon = Daemonize(
            app="hsyst_radio",
            pid=player.config.get("system", "pid_file"),
            action=player.start,
            chdir=player.base_dir,
            logger=player.logger,
            keep_fds=[handler.stream.fileno() for handler in player.logger.handlers 
                     if hasattr(handler, 'stream')]
        )
        daemon.start()
    else:
        player.start()

if __name__ == "__main__":
    main()
