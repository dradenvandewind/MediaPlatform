gst-launch-1.0 \
  tcpclientsrc host=127.0.0.1 port=5565 ! queue ! qtdemux name=demux \
  demux.video_0 ! queue ! h264parse ! mp4mux streamable=true fragment-duration=500 ! dashsink name=dash \
  tcpclientsrc host=127.0.0.1 port=5566 ! queue ! qtdemux \
  ! queue ! aacparse ! mp4mux streamable=true fragment-duration=500 \
  dash.location=./stream.mpd \
  dash.min-buffer-time=1000 \
  dash.target-duration=2 \
  dash.max-files=10 \
  dash.low-latency=true \
  dash.chunk-duration=500



#   gst-launch-1.0 \
#   tcpclientsrc port=5565 ! queue ! decodebin ! x264enc tune=zerolatency key-int-max=48 ! h264parse ! queue ! mux. \
#   tcpclientsrc port=5566 ! queue ! decodebin ! voaacenc ! aacparse ! queue ! mux. \
#   mp4mux name=mux streamable=true fragment-duration=500 \
#   ! dashsink \
#     location=./stream.mpd \
#     target-duration=2 \
#     min-buffer-time=1000 \
#     max-files=10


# Bonus : GStreamer LL-DASH ultra clean

# Avec chunking réel :

# dashsink \
#   low-latency=true \
#   chunk-duration=500 \


gst-launch-1.0 \
  tcpclientsrc host=127.0.0.1 port=5565 ! queue ! qtdemux name=demux \
  demux.video_0 ! queue ! h264parse ! mp4mux streamable=true fragment-duration=500 ! dashsink name=dash \
  min-buffer-time=1000 \
  target-duration=2 \
  dynamic=true \
  muxer=2 \
  use-segment-list=true \
  mpd-filename=./stream.mpd \
  mpd-root-path=/opt/sandbox \
  tcpclientsrc host=127.0.0.1 port=5566 ! queue ! qtdemux \
  ! queue ! aacparse ! mp4mux streamable=true fragment-duration=500 


gst-launch-1.0 \
  tcpclientsrc host=127.0.0.1 port=5565 ! queue ! qtdemux name=demux \
  demux.video_0 ! queue ! h264parse ! mp4mux streamable=true fragment-duration=500 ! dashsink name=dash \
  min-buffer-time=1000 \
  target-duration=2 \
  dynamic=true \
  muxer=2 \
  use-segment-list=true \
  mpd-filename=./stream.mpd \
  mpd-root-path=/opt/sandbox \
  tcpclientsrc host=127.0.0.1 port=5566 ! queue ! qtdemux \
  ! queue ! aacparse ! mp4mux streamable=true fragment-duration=500 




  gst-launch-1.0 \
  tcpclientsrc host=127.0.0.1 port=5565 ! queue ! qtdemux name=demux \
  demux.video_0 ! queue ! h264parse ! mp4mux streamable=true fragment-duration=500 ! dashsink name=dash \
    min-buffer-time=1000 \
  target-duration=2 \
  dynamic=true \
  muxer=2 \
  use-segment-list=true \
  mpd-filename=./stream.mpd \
  mpd-root-path=/opt/sandbox \
  tcpclientsrc host=127.0.0.1 port=5566 ! queue ! qtdemux \
  ! queue ! aacparse ! mp4mux streamable=true fragment-duration=500 