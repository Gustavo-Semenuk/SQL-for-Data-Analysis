-- Databricks notebook source
-- DBTITLE 1,Transformação logaritimica

select
(*),
round(log(1+TransactionAmount),2) as Transf_log
from bank_transactions_data_2_1_csv

-- COMMAND ----------

-- DBTITLE 1,z-score
WITH Estatisticas AS (
    SELECT 
        AVG(LOG(1 + TransactionAmount)) AS media_log,
        STDDEV(LOG(1 + TransactionAmount)) AS desvio_log
    FROM bank_transactions_data_2_1_csv
),
Z_Score AS (
    SELECT 
        t.TransactionID,
        t.TransactionAmount,
        LOG(1 + t.TransactionAmount) AS valor_log,
        ROUND((LOG(1 + t.TransactionAmount) - e.media_log) / e.desvio_log,2) AS z_score
    FROM bank_transactions_data_2_1_csv t, Estatisticas e
)
SELECT * 
FROM Z_Score
WHERE ABS(z_score) > 3;  -- Outliers são valores com Z-Score acima de 3


-- COMMAND ----------

-- DBTITLE 1,Intervalo interquartil
WITH Quartis AS (
    SELECT 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY LOG(1 + TransactionAmount)) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY LOG(1 + TransactionAmount)) AS q3
    FROM bank_transactions_data_2_1_csv
)
SELECT t.*
FROM bank_transactions_data_2_1_csv t, Quartis q
WHERE LOG(1 + t.TransactionAmount) < q.q1 - 1.5 * (q.q3 - q.q1) 
   OR LOG(1 + t.TransactionAmount) > q.q3 + 1.5 * (q.q3 - q.q1);
