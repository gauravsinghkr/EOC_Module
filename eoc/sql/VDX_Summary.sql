SELECT * FROM (SELECT IO_ID, SUBSTR(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) AS PLACEMENT#, TO_CHAR(SDATE, 'YYYY-MM-DD') AS START_DATE, 
TO_CHAR(EDATE, 'YYYY-MM-DD') AS END_DATE, INITCAP(CREATIVE_DESC)  AS PLACEMENT_NAME, COST_TYPE_DESC AS COST_TYPE, 
NET_UNIT_COST as NET_UNIT_COST, GROSS_UNIT_COST as GROSS_UNIT_COST, NET_BUDGET as NET_PLANNED_COST, GROSS_BUDGET as GROSS_PLANNED_COST, BOOKED_QTY AS BOOKED_IMP#BOOKED_ENG 
FROM TFR_REP.EOC_SUMMARY_VIEW
WHERE (IO_ID = {0})
	 AND (DATA_SOURCE = 'KM')
	 AND TO_CHAR(SDATE, 'YYYY-MM-DD') <= '{2}'
	 AND (TO_CHAR(SDATE, 'YYYY-MM-DD') BETWEEN
	 (CASE WHEN '{1}' BETWEEN TO_CHAR(SDATE, 'YYYY-MM-DD') AND TO_CHAR(EDATE, 'YYYY-MM-DD') THEN TO_CHAR(SDATE, 'YYYY-MM-DD')
	  ELSE (CASE WHEN '{1}' < TO_CHAR(SDATE, 'YYYY-MM-DD') AND '{1}' <= TO_CHAR(EDATE, 'YYYY-MM-DD') THEN TO_CHAR(SDATE, 'YYYY-MM-DD')
			ELSE TO_CHAR(SDATE, 'YYYY-MM-DD') END)
	  END)
	 AND
	 (CASE WHEN '{2}' BETWEEN TO_CHAR(SDATE, 'YYYY-MM-DD') AND TO_CHAR(EDATE, 'YYYY-MM-DD') THEN '{2}'
	  ELSE (CASE WHEN '{2}' > TO_CHAR(EDATE, 'YYYY-MM-DD') AND '{1}' <= TO_CHAR(SDATE, 'YYYY-MM-DD') THEN TO_CHAR(EDATE, 'YYYY-MM-DD')
			ELSE TO_CHAR(EDATE, 'YYYY-MM-DD') END)
	  END))
	 AND
	 (TO_CHAR(EDATE, 'YYYY-MM-DD') BETWEEN
	 (CASE WHEN '{1}' BETWEEN TO_CHAR(SDATE, 'YYYY-MM-DD') AND TO_CHAR(EDATE, 'YYYY-MM-DD') THEN '{1}'
	  ELSE (CASE WHEN '{1}' < TO_CHAR(SDATE, 'YYYY-MM-DD') AND '{1}' <= TO_CHAR(EDATE, 'YYYY-MM-DD') THEN TO_CHAR(SDATE, 'YYYY-MM-DD')
			ELSE TO_CHAR(SDATE, 'YYYY-MM-DD') END)
	  END)
	 AND
	 (CASE WHEN '{2}' BETWEEN TO_CHAR(SDATE, 'YYYY-MM-DD') AND TO_CHAR(EDATE, 'YYYY-MM-DD') THEN TO_CHAR(EDATE, 'YYYY-MM-DD')
	  ELSE (CASE WHEN '{2}' > TO_CHAR(EDATE, 'YYYY-MM-DD') AND '{1}' <= TO_CHAR(SDATE, 'YYYY-MM-DD') THEN TO_CHAR(EDATE, 'YYYY-MM-DD')
			ELSE TO_CHAR(EDATE, 'YYYY-MM-DD') END)
	  END))
	 AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC
						  FROM TFR_REP.EOC_SUMMARY_VIEW)ORDER BY PLACEMENT_ID)
WHERE Placement_Name Not LIKE '%Pre-Roll%' and  Placement_Name Not LIKE '%Pre–Roll%'