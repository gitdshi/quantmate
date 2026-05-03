-- Disable Tushare catalog interfaces that are permanently unavailable with
-- the current token permission level.  These consistently return "请指定正确的
-- 接口名" (wrong interface name / no access) or "必填参数" (missing mandatory
-- parameter that the code does not yet supply).  Leaving them enabled wastes
-- API calls and backfill processing time for every pending date.
--
-- Interfaces disabled:
--   bo_weekly, bo_monthly   — movie box-office (require higher Tushare points)
--   realtime_list, realtime_quote, realtime_tick — real-time crawler APIs
--   film_record             — film script filing (requires higher points)
--   stk_weekly_monthly, stk_week_month_adj, fut_weekly_monthly — need 'freq'
--     param not yet implemented
--   tmt_twincome, tmt_twincomedetail — need 'item' param not yet implemented

UPDATE data_source_items
SET enabled = 0
WHERE source = 'tushare'
  AND item_key IN (
    'bo_weekly', 'bo_monthly',
    'realtime_list', 'realtime_quote', 'realtime_tick',
    'film_record',
    'stk_weekly_monthly', 'stk_week_month_adj', 'fut_weekly_monthly',
    'tmt_twincome', 'tmt_twincomedetail'
  )
  AND enabled = 1;
