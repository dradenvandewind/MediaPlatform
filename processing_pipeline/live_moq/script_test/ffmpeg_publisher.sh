ffmpeg -re -i input.mp4 \
  -c:v libx264 -c:a aac \
  -f moq \
  moqt://localhost:4443/live/my-channel