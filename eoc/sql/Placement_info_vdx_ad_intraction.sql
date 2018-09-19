select * from(select product, SUBSTR(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) AS PLACEMENT#, blaze_action_type_desc, blaze_tag_name_desc,sum(decode(product, 
'InStream', vwr_interaction, eng_interaction)) as interaction from EOC_INTERACTION_DETAIL_VIEW where io_id = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}'  
group by product, PLACEMENT_DESC, blaze_action_type_desc,blaze_tag_name_desc order by product, 
blaze_action_type_desc, blaze_tag_name_desc) pivot (sum(interaction) for blaze_action_type_desc in('Click-thru' Clickthru, 'Interaction' Interaction)) 
order by product,blaze_tag_name_desc