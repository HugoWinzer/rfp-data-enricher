### Batch endpoints
- `GET /?limit=N` → run a batch
- `GET /?limit=N&dry=1` → count candidates only
- `GET /stats` → field coverage stats


### Required secrets
- `openai-api-key`, `ticketmaster-key`, `google-places-key`, `eventbrite-token`


### Env
`PROJECT_ID, DATASET_ID=rfpdata, TABLE=culture_merged, BQ_LOCATION=europe-southwest1, OPENAI_MODEL=gpt-4o-mini, STOP_ON_GPT_QUOTA=1`
