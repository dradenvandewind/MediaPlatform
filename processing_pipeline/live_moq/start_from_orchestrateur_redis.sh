curl -X POST http://localhost:8000/jobs/live/submit \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "live/my-channel",
    "tracks":    ["video", "audio"],
    "duration":  3600
  }'


#   {
#   "job_id":  "abc123",
#   "input": {
#     "namespace": "live/my-channel",
#     "tracks":    ["video", "audio"],
#     "duration":  3600,
#     "stream_id": "stream-abc123"
#   }
# }