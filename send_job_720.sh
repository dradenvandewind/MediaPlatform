curl -X 'POST' \
  'http://localhost:8000/jobs/submit' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "video_url": "https://download.blender.org/peach/bigbuckbunny_movies/BigBuckBunny_320x180.mp4",
  "title": "BigBuckBunny_320x180",
  "profiles": [
    "720p"
  ],
  "audio_tracks": [
    "en"
  ],
  "subtitles": [],
  "watermark_config": {},
  "drm_config": {}
}'
