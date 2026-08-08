
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select close
from "dataflow_hub"."gold"."daily_stock_summary"
where close is null



  
  
      
    ) dbt_internal_test