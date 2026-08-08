
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "dataflow_hub"."gold"."daily_stock_summary"
where date is null



  
  
      
    ) dbt_internal_test