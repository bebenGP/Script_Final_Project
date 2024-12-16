WITH ses_summary AS (
    SELECT 
        kecamatan,
        SUM(total_ses_a) AS total_ses_a
    FROM 
        demografi
    GROUP BY 
        kecamatan
),
poi_summary AS (
    SELECT 
        upper(district) AS kecamatan,
        COUNT(poi_name) AS total_poi
    FROM 
        data_services
    GROUP BY 
        district
)
SELECT 
    COALESCE(ses.kecamatan, poi.kecamatan) AS kecamatan,
    COALESCE(ses.total_ses_a, 0) AS total_ses_a,
    COALESCE(poi.total_poi, 0) AS total_poi
FROM 
    ses_summary AS ses
FULL OUTER JOIN 
    poi_summary AS poi
ON 
    ses.kecamatan = poi.kecamatan
ORDER BY 
    kecamatan;