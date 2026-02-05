# Authentication API

Base path: `/api/auth`

Authentication is JWT-based. Most protected endpoints require an `Authorization: Bearer <access_token>` header.

Endpoints

- `POST /api/auth/register`
  - Description: Register a new user
  - Body (application/json): `{ "username": string, "email": string, "password": string }`
  - Response: 201 Created, user profile (id, username, email)

- `POST /api/auth/login`
  - Description: Login and obtain access + refresh tokens
  - Body: `{ "username": string, "password": string }`
  - Response: `{ "access_token": string, "refresh_token": string, "token_type": "bearer" }`

- `POST /api/auth/refresh`
  - Description: Exchange refresh token for a new access token
  - Body: `{ "refresh_token": string }`
  - Response: `{ "access_token": string, "refresh_token": string }`

- `GET /api/auth/me`
  - Description: Get current authenticated user profile
  - Auth: Bearer token required
  - Response: `{ "id": number, "username": string, "email": string }`

Notes

- Tokens: `access_token` short-lived (~30 minutes), `refresh_token` longer (days). Configure via environment variables.
- Use HTTPS in production to protect credentials and tokens.
