USE practicesql;
-- cleaning the tables
with clean_shipments as(
	select distinct 
		s.shipment_id,
        cast(s.ship_date as date)as ship_date,
        cast(s.expected_delivery as date)as expected_delivery,
        cast(s.actual_delivery as date)as actual_delivery,
		s.warehouse_id,
        s.driver_id,
        s.vehicle_id,
        s.weight,
        case
			when lower(trim(s.status)) in ('delivered','delvered') then 'Delivered'
            when lower(trim(s.status)) in ('pending') then 'Pending'
            when lower(trim(s.status)) in ('in transit') then 'In Transit'
            else 'Unknown'
		end as status,
        cast(s.shipping_cost as decimal(10,2))as shipping_cost
        from shipments s
),
clean_routes as(
	select 
		r.route_id,
        r.shipment_id,
        case
			when r.distance like '%miles%' then cast(r.distance as decimal(10,2))*1.609
            else cast(r.distance as decimal(10,2))
		end as distance_km,
        r.expected_duration,
        r.actual_duration
	from routes r
),
clean_vehicles as(
	select
		case
			when lower(trim(v.vehicle_type))in('Trcuk') then 'Truck'
            when lower(trim(v.vehicle_type))in('Lorrie') then 'Lorry'
            when lower(trim(v.vehicle_type))in('Vann') then 'Van'
            else v.vehicle_type
		end as vehicle_type,
        v.vehicle_id,
        v.license_plate,
        v.cost_per_unit
	from vehicles v
),

-- join the above cleaned tables
joined_data as(	
    select
		s.shipment_id,
        w.warehouse_name,
        w.country,
        d.driver_id,
        concat(coalesce(d.first_name, ''), ' ', coalesce(d.last_name, '')) as driver_name,
        v.vehicle_id,
        v.license_plate,
        v.vehicle_type,
        s.status,
        s.ship_date,
        s.expected_delivery,
        s.actual_delivery,
        r.distance_km,
        s.weight,
        s.shipping_cost,
        r.expected_duration,
        r.actual_duration,
        case
			when r.actual_duration is not null and r.actual_duration > r.expected_duration then 1
            else 0
		end as late_flag
	from clean_shipments s
    join clean_routes r on s.shipment_id = r.shipment_id
    join clean_vehicles v on s.vehicle_id = v.vehicle_id
    join warehouses w on s.warehouse_id = w.warehouse_id
    join drivers d on s.driver_id = d.driver_id
),

final_metrics AS (
    -- Calculate KPIs
    SELECT
        country,
        warehouse_name,
        COUNT(*) AS total_shipments,
        SUM(CASE WHEN status = 'Delivered' THEN 1 ELSE 0 END) AS delivered_shipments,
        ROUND(100.0 * SUM(CASE WHEN late_flag = 0 AND status = 'Delivered' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),2) AS on_time_percent,
        ROUND(AVG(distance_km),2) AS avg_distance_km,
        ROUND(AVG(shipping_cost),2) AS avg_shipping_cost,
        SUM(shipping_cost) AS total_revenue
    FROM joined_data
    GROUP BY country, warehouse_name
)
select * from final_metrics AS Final
order by on_time_percent desc;