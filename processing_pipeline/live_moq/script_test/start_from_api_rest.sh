curl -X POST http://localhost:8085/live/start \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "live/my-channel",
    "tracks":    ["video", "audio"],
    "duration":  3600
  }'  -c:v libx264 -c:a aac \
  -f moq \
  moqt://localhost:4443/live/my-channel