#!/bin/bash
export PROJECT=kolban-dlp-tests
gcloud run deploy deidentify \
  --source=. \
  --platform=managed \
  --region=us-central1 \
  --no-allow-unauthenticated \
  --description="Invoke SDP De-identify for BigQuery" \
  --labels=app=deidentify \
  --max-instances=2 \
  --min-instances=0 \
  --project ${PROJECT}