# LakeDucktor

LakeDucktor keeps [DuckLake](https://ducklake.select/) healthy.

Point it at a lake and it continuously performs safe, bounded maintenance using
the lake's own settings and native DuckLake operations. It keeps no
authoritative state and exposes health metrics.

LakeDucktor maintains physical files. It does not provision lakes, proxy SQL,
manage access, or accept application writes.

[Vision and scope](docs/VISION.md)

[Running LakeDucktor](docs/RUNNING.md) · [Scaling](docs/SCALING.md)
