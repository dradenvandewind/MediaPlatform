Publisher (OBS/ffmpeg) ──MOQT/QUIC──► moq-relay:4443
                                            │
                                      moq-sub (1 par track)
                                            │ stdout binaire
                                      _read_and_segment()
                                            │ flush toutes SEGMENT_DURATION s
                                           S3  live/{stream_id}/video/000001.ts
                                            │
                               orchestrateur /jobs/{id}/next