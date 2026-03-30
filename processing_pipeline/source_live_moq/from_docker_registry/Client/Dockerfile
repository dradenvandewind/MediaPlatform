FROM ubuntu:25.04
ENV DEBIAN_FRONTEND=noninteractive
ENV GST_VERSION=1.26

RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    pkg-config \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-wheel \
    # ← bindings Python GObject (pour gi.repository)
    python3-gi \
    python3-gi-cairo \
    gir1.2-glib-2.0 \
    libgirepository1.0-dev \
    flex bison \
    liborc-0.4-dev \
    libglib2.0-dev \
    libssl-dev \
    libxml2-dev \
    libdrm-dev \
    libx11-dev \
    libxext-dev \
    libxv-dev \
    libasound2-dev \
    libpulse-dev \
    libvpx-dev \
    libx264-dev \
    libx265-dev \
    libaom-dev \
    libdav1d-dev \
    libde265-dev \
    wget curl \
    ca-certificates libnice-dev vim cargo rustc pkg-config \
    libgl-dev \
    libegl-dev \
    libgles2-mesa-dev \
    libwayland-dev \
    libx11-dev \
    && pip3 install --break-system-packages meson==1.4.0 ninja \
    && rm -rf /var/lib/apt/lists/*


RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install cargo-c

WORKDIR /opt
RUN git clone --depth=1 --branch ${GST_VERSION} \
    https://gitlab.freedesktop.org/gstreamer/gstreamer.git

WORKDIR /opt/gstreamer
RUN meson setup build \
    --prefix=/usr \
    --buildtype=release \
    -Dtests=disabled \
    -Ddoc=disabled \
    -Dintrospection=enabled \
    -Dgpl=enabled \
    -Dgood=enabled \
    -Dbad=enabled \
    -Drs=enabled 

RUN ninja -C build -j$(nproc)
RUN ninja -C build install
RUN ldconfig



RUN apt-get update && apt-get install -y graphviz ffmpeg

ENV LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
ENV GST_PLUGIN_PATH=/usr/lib/x86_64-linux-gnu/gstreamer-1.0
# Chemin des typelibs générés par introspection
ENV GI_TYPELIB_PATH=/usr/lib/x86_64-linux-gnu/girepository-1.0
ENV GST_DEBUG_DUMP_DOT_DIR=/tmp/
ENV GST_DEBUG=1
COPY ./test_client_gst.sh /opt/test_client_gst.sh

COPY ./moq_to_dash_hls_opti.py /opt/moq_to_dash_hls_opti.py
RUN apt-get update && apt-get install -y tzdata \
    && ln -sf /usr/share/zoneinfo/Europe/Paris /etc/localtime \
    && echo "Europe/Paris" > /etc/timezone \
    && apt-get clean
ENV TZ=Europe/Paris

#COPY ./test_gst_client.py /opt/test_gst_client.py
CMD ["gst-launch-1.0", "--version"]
