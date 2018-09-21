select IO_ID, substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, 
TO_CHAR(SDATE, 'YYYY-MM-DD') as Start_Date, TO_CHAR(EDATE, 'YYYY-MM-DD') as End_Date, 
CREATIVE_DESC  as Placement_Name, COST_TYPE_DESC as Cost_type,NET_UNIT_COST as NET_UNIT_COST, GROSS_UNIT_COST as GROSS_UNIT_COST,
NET_BUDGET as NET_PLANNED_COST, GROSS_BUDGET as GROSS_PLANNED_COST, BOOKED_QTY as Booked_Imp#Booked_Eng FROM TFR_REP.EOC_SUMMARY_VIEW 
where IO_ID = {0} AND DATA_SOURCE = 'SalesFile' 
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
END)) ORDER BY PLACEMENT_ID