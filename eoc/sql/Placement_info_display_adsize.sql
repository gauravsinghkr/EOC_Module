SELECT 
        substr(OVERALL.PLACEMENT_DESC,1,INSTR(OVERALL.PLACEMENT_DESC, '.', 1)-1) as Placement#       
        ,OVERALL.MEDIA_SIZE_DESC as Adsize
        ,SUM(OVERALL.VIEWS) Delivered_Impression
        ,SUM(OVERALL.CLICKS) Clicks
        ,SUM(OVERALL.CONVERSIONS) Conversion
FROM     
(
    SELECT * FROM TFR_REP.EOC_SALES_VIEW WHERE IO_ID = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}'     
) OVERALL
LEFT JOIN
(
    SELECT PLACEMENT_ID, DAY_DESC, SUM(VIEWS) FROM TFR_REP.EOC_SALES_VIEW A
    WHERE IO_ID = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}'
    GROUP BY PLACEMENT_ID, DAY_DESC    
    HAVING SUM(VIEWS) <= 0
) ZEROS
    ON OVERALL.PLACEMENT_ID = ZEROS.PLACEMENT_ID AND OVERALL.DAY_DESC = ZEROS.DAY_DESC
    WHERE ZEROS.PLACEMENT_ID IS NULL AND ZEROS.DAY_DESC IS NULL
GROUP BY OVERALL.PLACEMENT_DESC, OVERALL.MEDIA_SIZE_DESC order by OVERALL.PLACEMENT_DESC