# Queue & Job Management API

Base path: `/api/queue`

Endpoints

- `GET /api/queue/stats`
  - Description: Get queue statistics (counts per queue, processing rates)
  - Response: `{ queues: { backtest: { queued: 10, started: 2, failed: 1 }, optimization: {...} }, redis_info: {...} }`

- `GET /api/queue/jobs`
  - Description: List jobs for the authenticated user
  - Query params: `?status=queued&limit=20&page=1`

- `GET /api/queue/jobs/{job_id}`
  - Description: Get job details, logs, and progress

- `POST /api/queue/jobs/{job_id}/cancel`
  - Description: Cancel a queued or running job

- `DELETE /api/queue/jobs/{job_id}`
  - Description: Remove a job from storage (user action)

Notes

- Queue backend: Redis + RQ workers. Worker processes consume job queues and update status/progress in job storage.
- Result TTL: job results may be kept for a configured TTL (default 7 days) before automatic cleanup.
