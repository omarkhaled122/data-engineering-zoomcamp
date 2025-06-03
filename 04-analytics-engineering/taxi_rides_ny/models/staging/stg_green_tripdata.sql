with 

source as (

    select * from {{ source('staging', 'green_tripdata') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as trip_id,
        vendorid,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        store_and_fwd_flag,
        ratecodeid,
        pulocationid,
        dolocationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        {{get_payment_type_description('payment_type')}} AS payment_type_description,
        trip_type,
        congestion_surcharge,
        ehail_fee

    from source

)

select * from renamed
