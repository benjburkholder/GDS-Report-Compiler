:: RUN GDS REPORT
:: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:: SET ENV VARIABLES
SETLOCAL
CD C:\Repositories\clients
SET venvpath=%cd%\venv\Scripts\python.exe
SET refresh_indicator_run=run
SET refresh_indicator_skip=skip
:: SCRIPT EXECUTION
:: =======================================================
:: AUTOMATION: Scripts for pulling data
%venvpath% %cd%\google_analytics_events.py %refresh_indicator_run%
timeout 10
%venvpath% %cd%\google_analytics_traffic.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\google_analytics_goals.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\google_my_business_insights.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\google_my_business_reviews.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\moz_local_sync.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\moz_local_visibility.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\moz_pro_rankings.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\moz_pro_serp.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\dialogtech_call_detail.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\google_ads_campaign.py %refresh_indicator_skip%
timeout 10
%venvpath% %cd%\account_cost.py %refresh_indicator_skip%
ENDLOCAL