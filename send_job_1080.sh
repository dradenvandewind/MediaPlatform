curl -X 'POST' \
  'http://localhost:8000/jobs/submit' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "video_url": "https://download.blender.org/peach/bigbuckbunny_movies/big_buck_bunny_1080p_h264.mov",
  "title": "BigBuckBunny_1080p",
  "profiles": [
    "1080p",
  ],
  "audio_tracks": [
    "en"
  ],
  "subtitles": [],
  "watermark_config": {},
  "drm_config": {}
}'
