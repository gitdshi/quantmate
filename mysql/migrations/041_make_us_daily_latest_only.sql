UPDATE data_source_items
SET sync_mode = 'latest_only'
WHERE source = 'tushare'
  AND item_key = 'us_daily'
  AND COALESCE(sync_mode, '') <> 'latest_only';