-- stg_dim_store — passthrough staging for store dimension
select * from {{ source('ebi_semantic_poc', 'sem_poc_dim_store') }}
