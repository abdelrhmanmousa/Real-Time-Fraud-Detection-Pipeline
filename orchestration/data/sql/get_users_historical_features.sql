-- This is a TEMPLATED SQL file.
-- The {{ params.project_id }} and {{ params.dataset }} placeholders will be
-- replaced by Airflow at runtime with values from Airflow Variables.

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset }}.For_Firestore_Users_Recalculated`
OPTIONS(
  description="Denormalized user data with all historical metrics recalculated from transaction history, prepared for Firestore."
) AS

WITH
UserLastTransaction AS (
  SELECT User_SK_FK, MAX(transaction_timestamp) AS last_tx_timestamp
  FROM `{{ params.project_id }}.{{ params.dataset }}.Fact_Transactions`
  GROUP BY User_SK_FK
),
CalculatedUserMetrics AS (
  SELECT
    ft.User_SK_FK,
    AVG(IF(ft.transaction_timestamp >= TIMESTAMP_SUB(ult.last_tx_timestamp, INTERVAL 30 DAY), ft.amount, NULL)) AS user_avg_tx_amount_30d,
    MAX(IF(ft.transaction_timestamp >= TIMESTAMP_SUB(ult.last_tx_timestamp, INTERVAL 90 DAY), ft.distance_from_home_km, NULL)) AS user_max_distance_from_home_90d,
    COUNT(DISTINCT IF(ft.transaction_timestamp >= TIMESTAMP_SUB(ult.last_tx_timestamp, INTERVAL 180 DAY), dc.card_country, NULL)) AS user_num_distinct_countries_6m,
    COUNTIF(ft.transaction_timestamp >= TIMESTAMP_SUB(ult.last_tx_timestamp, INTERVAL 24 HOUR)) AS user_tx_count_24h,
    COUNTIF(ft.transaction_timestamp >= TIMESTAMP_SUB(ult.last_tx_timestamp, INTERVAL 1 HOUR) AND dtf.was_3ds_successful = FALSE) AS user_failed_tx_count_1h,
    COUNT(DISTINCT IF(ft.transaction_timestamp >= TIMESTAMP_SUB(ult.last_tx_timestamp, INTERVAL 24 HOUR), dm.merchant_category, NULL)) AS user_num_distinct_mcc_24h
  FROM `{{ params.project_id }}.{{ params.dataset }}.Fact_Transactions` AS ft
  JOIN UserLastTransaction AS ult ON ft.User_SK_FK = ult.User_SK_FK
  JOIN `{{ params.project_id }}.{{ params.dataset }}.Dim_Merchants` AS dm ON ft.Merchant_SK_FK = dm.Merchant_SK
  JOIN `{{ params.project_id }}.{{ params.dataset }}.Dim_Cards` AS dc ON ft.Card_SK_FK = dc.Card_SK
  JOIN `{{ params.project_id }}.{{ params.dataset }}.Dim_Transaction_Flags` AS dtf ON ft.Transaction_Flags_SK_FK = dtf.Transaction_Flags_SK
  WHERE ft.transaction_timestamp >= TIMESTAMP_SUB(ult.last_tx_timestamp, INTERVAL 180 DAY)
  GROUP BY ft.User_SK_FK
),
UserCards AS (
  SELECT ft.User_SK_FK, ARRAY_AGG(STRUCT(dc.card_id, dc.card_type, dc.card_brand, dc.issuing_bank, dc.card_country)) AS cards
  FROM (SELECT DISTINCT User_SK_FK, Card_SK_FK FROM `{{ params.project_id }}.{{ params.dataset }}.Fact_Transactions`) AS ft
  JOIN `{{ params.project_id }}.{{ params.dataset }}.Dim_Cards` AS dc ON ft.Card_SK_FK = dc.Card_SK
  GROUP BY ft.User_SK_FK
),
UserDevices AS (
  SELECT ft.User_SK_FK, ARRAY_AGG(STRUCT(dd.device_id)) AS devices
  FROM (SELECT DISTINCT User_SK_FK, Device_SK_FK FROM `{{ params.project_id }}.{{ params.dataset }}.Fact_Transactions`) AS ft
  JOIN `{{ params.project_id }}.{{ params.dataset }}.Dim_Devices` AS dd ON ft.Device_SK_FK = dd.Device_SK
  GROUP BY ft.User_SK_FK
)
SELECT
  du.user_id,
  DATE_DIFF(CURRENT_DATE(), DATE(du.account_created_at_timestamp), DAY) AS user_account_age_days,
  du.country_code AS user_home_country,
  du.home_lat AS user_home_lat,
  du.home_lon AS user_home_long,
  cum.* EXCEPT(User_SK_FK),
  uc.cards,
  ud.devices
FROM `{{ params.project_id }}.{{ params.dataset }}.Dim_Users` AS du
LEFT JOIN CalculatedUserMetrics AS cum ON du.User_SK = cum.User_SK_FK
LEFT JOIN UserCards AS uc ON du.User_SK = uc.User_SK_FK
LEFT JOIN UserDevices AS ud ON du.User_SK = ud.User_SK_FK
WHERE cum.User_SK_FK IS NOT NULL;