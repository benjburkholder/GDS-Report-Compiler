:: RUN GDS REPORT
:: ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:: SET ENV VARIABLES
SETLOCAL
CD C:\Repositories\GDS-Report-Compiler
SET venvpath=%cd%\venv\Scripts\python.exe
:: SCRIPT EXECUTION
:: =======================================================
:: AUTOMATION: Scripts for pulling data
%venvpath% %cd%\google_analytics_events.py
timeout 10
%venvpath% %cd%\google_analytics_traffic.py
timeout 10
%venvpath% %cd%\google_analytics_goals.py
timeout 10
%venvpath% %cd%\google_my_business_insights.py
timeout 10
%venvpath% %cd%\google_my_business_reviews.py
timeout 10
%venvpath% %cd%\moz_local_sync.py
timeout 10
%venvpath% %cd%\moz_local_visibility.py
timeout 10
%venvpath% %cd%\moz_pro_rankings.py
timeout 10
%venvpath% %cd%\moz_pro_serp.py
ENDLOCAL