# Backend Create-to-Ready Benchmark

Date: 2026-08-11, 21:35-21:36 CST

## Environment

- Host: macOS 26.5.2 (`darwin/arm64`), Go 1.26.5.
- Docker: 29.4.0, API 1.54.
- OrbStack: 2.2.2; source VM was a fresh `ubuntu-2404` arm64 VM with 2 CPU and 2 GiB memory.
- Apple Container: CLI 1.1.0 with the recommended Kata 3.28.0 arm64 kernel; image `docker.io/library/alpine:3.20`, 2 CPU and 2 GiB memory.
- SBX: `sbx` 0.38.0, authenticated `sandboxd`, image `e2b-local/sbx-envd:dev`.
- `envd`: version 0.6.13, SHA-256 `2107036d0f0c11b80b764a1edfd3a4cc018e696f2657b21fbb3680303ce5ff48`, compiled from public `e2b-dev/infra` revision `04fa7b52b35ba1e6eacbaa03c7d1f78533e2359c`. The SBX image was rebuilt with `--pull --no-cache`; Docker, OrbStack, and Apple Container used the same extracted static binary.

## Method

Each backend received one unrecorded warm-up and three recorded runs. Each run
created a fresh sandbox and deleted it before the next run. Sampling occurred
every 15 ms through the backend's existing `InspectSandbox` interface:

- **Create**: elapsed time until the runtime resource first existed.
- **Start**: elapsed time until it first reported `running`.
- **Ready**: elapsed time until `CreateSandbox` returned after `envd /health` succeeded.

All three values are cumulative from the start of `CreateSandbox`; `Ready` is
the create-to-ready value. Polling makes the first two values accurate to about
one sampling interval. Docker ran with FUSE disabled for this benchmark. The
Docker image was the fresh SBX image with the same freshly built `envd` bind-mounted.

## Results

Values are milliseconds. `p50` is the median of the three recorded runs;
`mean` is included to show variability.

| Backend | Create p50 (mean) | Start p50 (mean) | Ready p50 (mean) |
| --- | ---: | ---: | ---: |
| Docker | 284.805 (306.058) | 284.805 (306.058) | 1294.454 (1312.302) |
| OrbStack | 17.284 (17.131) | 17.738 (22.721) | 1735.584 (1631.645) |
| Apple Container | 1015.382 (686.776) | 1149.747 (1127.655) | 4266.020 (4231.849) |
| SBX | 2117.625 (2077.558) | 2207.366 (2225.576) | 3026.089 (3050.051) |

Raw recorded runs (`create / start / ready`, ms):

| Backend | Run 1 | Run 2 | Run 3 |
| --- | --- | --- | --- |
| Docker | 284.805 / 284.805 / 1294.454 | 279.943 / 279.943 / 1283.574 | 353.425 / 353.425 / 1358.877 |
| OrbStack | 17.738 / 17.738 / 1735.584 | 17.284 / 17.284 / 1257.141 | 16.371 / 33.141 / 1902.210 |
| Apple Container | 1025.111 / 1025.111 / 4137.457 | 19.835 / 1149.747 / 4292.070 | 1015.382 / 1208.107 / 4266.020 |
| SBX | 2117.625 / 2343.340 / 3183.477 | 2126.022 / 2126.022 / 2940.586 | 1989.027 / 2207.366 / 3026.089 |

## Interpretation

Docker was fastest on this warm local image path. OrbStack reached a running
clone quickly because its source VM was already warm, while its `envd` bootstrap
dominates readiness. SBX adds authenticated `sandboxd` creation and the guest
reverse tunnel before health can succeed. Apple Container 1.1.0 reset the
published localhost port on this host even though `envd` was healthy inside the
guest; the runtime now falls back to the reachable guest IPv4, and that
three-second failed local-proxy probe is included in the measured result.
