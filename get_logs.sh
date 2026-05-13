gh run view 25737552794 --json status,conclusion,headSha,displayTitle,jobs --jq '.status, .conclusion, .headSha, .displayTitle, (.jobs[] | "\(.name): \(.conclusion)")' | cat
gh run view 25737552794 --log | grep -E "Pulling image tag|container /app/app.py checksum|Staging deploy successful" | cat
