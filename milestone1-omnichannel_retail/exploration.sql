-- cek kategori di bronze
SELECT 
    source_category,
    COUNT(id)
FROM bronze.raw_records 
GROUP BY source_category
LIMIT 15;

-- cek bronze
WITH RankedData AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER(PARTITION BY source_category ORDER BY (SELECT NULL)) as rn
    FROM bronze.raw_records
)
SELECT *
FROM RankedData
WHERE rn <= 10;

-- cek rejected records silver

select * from silver.rejected_records;

-- cek gold
SELECT *
FROM gold.executive_kpis_daily 
ORDER BY metric_date DESC LIMIT 5;
