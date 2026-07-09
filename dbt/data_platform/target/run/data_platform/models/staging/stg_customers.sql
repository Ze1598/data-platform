-- back compat for old kwarg name
  
  
        
            
            
        

        

        merge into "iceberg"."staging"."customers" as DBT_INTERNAL_DEST
            using "iceberg"."staging"."customers__dbt_tmp" as DBT_INTERNAL_SOURCE
            on (
                DBT_INTERNAL_SOURCE._key_hash = DBT_INTERNAL_DEST._key_hash
            )

        
        when matched then update set
            "customer_id" = DBT_INTERNAL_SOURCE."customer_id","name" = DBT_INTERNAL_SOURCE."name","email" = DBT_INTERNAL_SOURCE."email","updated_at" = DBT_INTERNAL_SOURCE."updated_at","_key_hash" = DBT_INTERNAL_SOURCE."_key_hash","_attr_hash" = DBT_INTERNAL_SOURCE."_attr_hash","_loaded_at" = DBT_INTERNAL_SOURCE."_loaded_at"
        

        when not matched then insert
            ("customer_id", "name", "email", "updated_at", "_key_hash", "_attr_hash", "_loaded_at")
        values
            (DBT_INTERNAL_SOURCE."customer_id", DBT_INTERNAL_SOURCE."name", DBT_INTERNAL_SOURCE."email", DBT_INTERNAL_SOURCE."updated_at", DBT_INTERNAL_SOURCE."_key_hash", DBT_INTERNAL_SOURCE."_attr_hash", DBT_INTERNAL_SOURCE."_loaded_at")

    
