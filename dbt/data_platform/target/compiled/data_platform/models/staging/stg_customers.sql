



with source_raw as (

    select
        customer_id,
        name,
        email,
        updated_at,
        
to_hex(md5(to_utf8(
    coalesce(cast(customer_id as varchar), '~NULL~')
))) as _key_hash,
        
to_hex(md5(to_utf8(
    coalesce(cast(name as varchar), '~NULL~') || '|~|' || 
    coalesce(cast(email as varchar), '~NULL~') || '|~|' || 
    coalesce(cast(updated_at as varchar), '~NULL~')
))) as _attr_hash
    from "iceberg"."clean"."customers"

)



, source as (

    select source_raw.*
    from source_raw
    left join "iceberg"."staging"."customers" as target
        on source_raw._key_hash = target._key_hash
    where target._key_hash is null                       -- new business key
       or target._attr_hash != source_raw._attr_hash      -- changed attributes
)



select
    *,
    
current_timestamp(6) as _loaded_at
from source