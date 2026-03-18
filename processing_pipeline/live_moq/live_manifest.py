"""
live_manifest.py – Generates and updates a dynamic DASH manifest (type="dynamic")
as segments arrive.

Called by LiveIngestWorker._flush_segment() after each S3 upload.

DASH-IF Live spec:
  - type="dynamic"          → player re-fetches the manifest periodically
  - availabilityStartTime   → epoch of the first segment
  - minimumUpdatePeriod     → how often the player re-fetches the manifest
  - timeShiftBufferDepth    → sliding window of available segments (DVR)
  - $Number$                → player computes segment URLs by number
  - suggestedPresentationDelay → recommended playback delay (live latency)
"""

import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from xml.dom import minidom
from xml.etree import ElementTree as ET

log = logging.getLogger(__name__)

# Namespaces DASH
_NS_MPD   = "urn:mpeg:dash:schema:mpd:2011"
_NS_XSI   = "http://www.w3.org/2001/XMLSchema-instance"
_NS_XLINK = "http://www.w3.org/1999/xlink"

ET.register_namespace("",      _NS_MPD)
ET.register_namespace("xsi",   _NS_XSI)
ET.register_namespace("xlink", _NS_XLINK)


@dataclass
class TrackInfo:
    """Metadata for a track (video or audio)."""
    track_id:    str                    # "video" | "audio"
    mime_type:   str                    # "video/mp4" | "audio/mp4"
    codecs:      str                    # "avc1.42c01f" | "mp4a.40.2"
    bandwidth:   int                    # estimated bits/s
    # Video only
    width:       Optional[int] = None
    height:      Optional[int] = None
    frame_rate:  Optional[str] = None   # "30000/1001" or "30"
    # Audio only
    audio_sampling_rate: Optional[int] = None
    num_channels:        Optional[int] = None


@dataclass
class SegmentEntry:
    """A segment uploaded to S3."""
    track_id:    str
    number:      int                    # absolute index since stream start
    s3_key:      str                    # e.g. live/stream-abc/video/000042.m4s
    duration_s:  float                  # actual segment duration
    byte_size:   int
    wall_time:   float = field(default_factory=time.time)


class LiveManifestWriter:
    """
    Builds and updates a dynamic MPD.

    Usage:
        writer = LiveManifestWriter(
            stream_id        = "stream-abc123",
            s3_base_url      = "https://cdn.example.com/live/stream-abc123",
            segment_duration = 4,
        )
        # After each segment flush:
        writer.add_segment(SegmentEntry(...))
        mpd_xml = writer.render()
    """

    def __init__(
        self,
        stream_id:            str,
        s3_base_url:          str,          # public URL root for S3 segments
        segment_duration:     float = 4.0,  # target segment duration (seconds)
        time_shift_buffer:    float = 60.0, # DVR window (seconds)
        min_update_period:    float = 2.0,  # manifest re-fetch frequency (seconds)
        suggested_delay:      float = 10.0, # recommended live latency (seconds)
        tracks: Optional[list[TrackInfo]] = None,
    ):
        self.stream_id         = stream_id
        self.s3_base_url       = s3_base_url.rstrip("/")
        self.segment_duration  = segment_duration
        self.time_shift_buffer = time_shift_buffer
        self.min_update_period = min_update_period
        self.suggested_delay   = suggested_delay

        # Default tracks if none are provided
        self.tracks: list[TrackInfo] = tracks or [
            TrackInfo(
                track_id   = "video",
                mime_type  = "video/mp4",
                codecs     = "avc1.42c01f",   # H.264 Baseline
                bandwidth  = 1_500_000,
                width      = 1280,
                height     = 720,
                frame_rate = "30",
            ),
            TrackInfo(
                track_id            = "audio",
                mime_type           = "audio/mp4",
                codecs              = "mp4a.40.2",  # AAC-LC
                bandwidth           = 128_000,
                audio_sampling_rate = 44100,
                num_channels        = 2,
            ),
        ]

        # Internal state
        self._start_time: float = time.time()
        self._segments:   dict[str, list[SegmentEntry]] = {
            t.track_id: [] for t in self.tracks
        }

    # ── API publique ──────────────────────────────────────────────────────────

    def add_segment(self, entry: SegmentEntry) -> None:
        """Registers a new segment. Thread-safe when called from asyncio."""
        if entry.track_id not in self._segments:
            self._segments[entry.track_id] = []
        self._segments[entry.track_id].append(entry)
        log.debug(
            "[manifest/%s] track=%s seg#%d  %.1f KB",
            self.stream_id, entry.track_id, entry.number, entry.byte_size / 1024,
        )

    def render(self) -> str:
        """Generates the full MPD XML. Called after each add_segment()."""
        now_utc  = datetime.now(timezone.utc)
        start_dt = datetime.fromtimestamp(self._start_time, tz=timezone.utc)

        # ── MPD root ────────────────────────────────────────────────────────
        mpd = ET.Element(f"{{{_NS_MPD}}}MPD", {
            f"{{{_NS_XSI}}}schemaLocation": (
                f"{_NS_MPD} "
                "http://standards.iso.org/ittf/PubliclyAvailableStandards/"
                "MPEG-DASH_schema_files/DASH-MPD.xsd"
            ),
            "type":                    "dynamic",
            "availabilityStartTime":   start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "publishTime":             now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "minimumUpdatePeriod":     self._iso_duration(self.min_update_period),
            "minBufferTime":           self._iso_duration(self.segment_duration * 2),
            "timeShiftBufferDepth":    self._iso_duration(self.time_shift_buffer),
            "suggestedPresentationDelay": self._iso_duration(self.suggested_delay),
            # Profil DASH-IF live
            "profiles":                "urn:mpeg:dash:profile:isoff-live:2011",
        })

        # ── Period ────────────────────────────────────────────────────────────
        period = ET.SubElement(mpd, f"{{{_NS_MPD}}}Period", {
            "id":    "P0",
            "start": "PT0S",
        })

        # ── AdaptationSet per track ───────────────────────────────────────────
        for adapt_id, track in enumerate(self.tracks):
            self._build_adaptation_set(period, track, adapt_id)

        # ── Pretty-printed XML output ─────────────────────────────────────────
        raw = ET.tostring(mpd, encoding="unicode", xml_declaration=False)
        pretty = minidom.parseString(raw).toprettyxml(indent="  ")
        # minidom adds its own XML declaration, so we prefix ours
        lines = pretty.splitlines()
        xml_lines = [l for l in lines if l.strip()]  # remove empty lines
        xml_header = '<?xml version="1.0" encoding="UTF-8"?>'
        body = "\n".join(xml_lines[1:])               # drop minidom header
        return f"{xml_header}\n{body}\n"

    def get_manifest_s3_key(self) -> str:
        return f"live/{self.stream_id}/manifest.mpd"

    def segment_count(self, track_id: str) -> int:
        return len(self._segments.get(track_id, []))

    # ── Helpers XML ──────────────────────────────────────────────────────────

    def _build_adaptation_set(
        self,
        period:   ET.Element,
        track:    TrackInfo,
        adapt_id: int,
    ) -> None:
        segs = self._segments.get(track.track_id, [])

        attrs: dict[str, str] = {
            "id":          str(adapt_id),
            "mimeType":    track.mime_type,
            "codecs":      track.codecs,
            "startWithSAP": "1",
        }
        if track.frame_rate:
            attrs["frameRate"] = track.frame_rate
        if track.audio_sampling_rate:
            attrs["audioSamplingRate"] = str(track.audio_sampling_rate)

        adapt = ET.SubElement(period, f"{{{_NS_MPD}}}AdaptationSet", attrs)

        # AudioChannelConfiguration
        if track.num_channels:
            ET.SubElement(adapt, f"{{{_NS_MPD}}}AudioChannelConfiguration", {
                "schemeIdUri": "urn:mpeg:dash:23003:3:audio_channel_configuration:2011",
                "value":       str(track.num_channels),
            })

        # ── Representation ────────────────────────────────────────────────────
        repr_attrs: dict[str, str] = {
            "id":        f"r_{track.track_id}",
            "bandwidth": str(self._estimate_bandwidth(track, segs)),
        }
        if track.width and track.height:
            repr_attrs["width"]  = str(track.width)
            repr_attrs["height"] = str(track.height)

        representation = ET.SubElement(adapt, f"{{{_NS_MPD}}}Representation", repr_attrs)

        # ── SegmentTemplate ───────────────────────────────────────────────────
        # We use $Number$ + duration (in timescale units) for a compact manifest without
        # an exhaustive list of segments.
        #
        # The player computes:
        #   URL = baseURL + media.replace("$Number$", floor(now - availStart) / segDur)
        #
        timescale   = 1000                                # milliseconds
        duration_tu = int(self.segment_duration * timescale)
        start_num   = 1

        # DVR window: number of the first segment still available
        if segs:
            total_segs   = segs[-1].number + 1
            window_segs  = math.ceil(self.time_shift_buffer / self.segment_duration)
            start_num    = max(1, total_segs - window_segs)

        # Track S3 root (without filename)
        # e.g. https://cdn.example.com/live/stream-abc/video
        base_url = f"{self.s3_base_url}/{track.track_id}"

        ET.SubElement(representation, f"{{{_NS_MPD}}}BaseURL").text = base_url + "/"

        ET.SubElement(representation, f"{{{_NS_MPD}}}SegmentTemplate", {
            "timescale":      str(timescale),
            "duration":       str(duration_tu),
            "startNumber":    str(start_num),
            "initialization": "init.mp4",        # ftyp + moov (1 par track)
            "media":          "$Number%06d$.m4s", # moof + mdat  ex: 000042.m4s
        })

    @staticmethod
    def _estimate_bandwidth(track: TrackInfo, segs: list[SegmentEntry]) -> int:
        """Estimate average bitrate over the last 10 segments when available."""
        if not segs:
            return track.bandwidth
        recent = segs[-min(10, len(segs)):]
        if not recent:
            return track.bandwidth
        total_bytes    = sum(s.byte_size for s in recent)
        total_duration = sum(s.duration_s for s in recent)
        if total_duration <= 0:
            return track.bandwidth
        return int((total_bytes * 8) / total_duration)

    @staticmethod
    def _iso_duration(seconds: float) -> str:
        """Convert seconds to ISO 8601 duration (PT#.#S)."""
        if seconds == int(seconds):
            return f"PT{int(seconds)}S"
        return f"PT{seconds:.1f}S"