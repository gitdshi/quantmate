# Strategies API

Base path: `/api/strategies`

Endpoints

- `GET /api/strategies`
  - Description: List user strategies
  - Query params: `?limit=&page=&sort=`
  - Response: Paginated list of strategies

- `POST /api/strategies`
  - Description: Create a new strategy
  - Auth: Bearer token required
  - Body: `{ "name": string, "description": string, "code": string, "params": object }`
  - Response: Created strategy object with `id`

- `GET /api/strategies/{id}`
  - Description: Get strategy details
  - Response: Strategy object including source code and parameter schema

- `PUT /api/strategies/{id}`
  - Description: Update strategy
  - Auth: Bearer token required
  - Body: Partial strategy fields
  - Response: Updated strategy

- `DELETE /api/strategies/{id}`
  - Description: Delete user strategy
  - Auth: Bearer token required
  - Response: 204 No Content

- `GET /api/strategies/builtin`
  - Description: List built-in strategies available for quick import (e.g., Turtle Trading, Triple MA)

Notes

- Strategy code validation: server-side AST validation and safe execution checks are performed before saving or running.
- Parameter schema: strategies expose parameter names, types, min/max/default for UI builders and optimization.
