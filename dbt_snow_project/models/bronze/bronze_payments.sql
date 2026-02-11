SELECT 
    * 
    FROM {{ source('source', 'payments') }}