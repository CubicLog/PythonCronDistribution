To scale workers, use `--scale worker=3`
ex: `docker compose up --scale worker=3`

Workers use Redis to automatically distribute tasks and "lease" them based on a weight factor for each task. It will try to eventually balance load based on this weight. The `REDIS_URL` env var can be changed to a central Redis instance for a multi-machine setup, allowing for horizontal scaling.
