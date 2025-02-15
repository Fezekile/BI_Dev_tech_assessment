SELECT 
	   fp.[Pickup_Point_ID]
	  ,dp.[Regional]
	  ,dp.[Province]
	  ,dp.[Suburb]
	  ,fp.[Waybill]
      ,fp.[Customer_ID]
      ,fp.[Order_Date]
      ,SUM(fp.[Parcel_KG]) AS total_weight
      ,SUM(fp.[Courier_Charge]) AS total_courier_charge
      ,SUM(fp.[Sales_amount]) AS total_Sales_amount
	  ,COUNT(fp.[Customer_ID]) AS number_of_customers
	  ,COUNT(fp.[Waybill]) AS number_of_orders
  FROM [pargo_dm].[dbo].[fact_parcels] AS fp
  INNER JOIN [pargo_dm].[dbo].[dim_pickup_points] AS dp
  ON fp.[Pickup_Point_ID] = dp.[Pickup_Point_ID]
  GROUP BY  fp.[Pickup_Point_ID]
	  ,dp.[Regional]
	  ,dp.[Province]
	  ,dp.[Suburb]
	  ,fp.[Waybill]
      ,fp.[Customer_ID]
      ,fp.[Order_Date]
