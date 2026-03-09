-- stg_dim_product — passthrough staging for product dimension
select * from {{ source('ebi_semantic_poc', 'sem_poc_dim_product') }}
