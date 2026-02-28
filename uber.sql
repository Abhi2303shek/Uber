create database uber_data;
use uber_data;

CREATE TABLE uber (
	dates date,
    timing time,
    booking_id varchar(20),
    booking_status varchar(50),
    customer_id varchar(20),
    vehicle_type varchar(25),
    pickup varchar(50),
    drop_at varchar(50),
    avg_vtat decimal(5,1),
    avg_ctat decimal(5,1),
    cancel_by_cust bigint,
    reason_cbc varchar(100),
    cancel_by_driver bigint,
    reason_driver varchar(100),
    incomplete float,
    reason varchar(25),
    booking_value float,
    dist float,
    driver_rating decimal(5,2),
    cust_rating decimal(5,2),
    pay_method varchar(20)
);


LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/uber.csv'
INTO TABLE uber
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
@dates,
timing,
booking_id,
booking_status,
customer_id,
vehicle_type,
pickup,
drop_at,
avg_vtat,
avg_ctat,
cancel_by_cust,
reason_cbc,
cancel_by_driver,
reason_driver,
incomplete,
reason,
booking_value,
dist,
driver_rating,
cust_rating,
pay_method
)
SET
dates = STR_TO_DATE(@dates, '%d-%m-%Y');



-- / -- 🔥 1️⃣ Revenue & Financial Performance  --/
## 1. What is the total revenue generated (Completed rides only)?
select sum(booking_value) as revenue, booking_status from uber where booking_status='Completed' order by revenue;

## 2. What is the average booking value?
select avg(booking_value) as avg_revenue, booking_status from uber where booking_status='Completed' order by avg_revenue;

## 3. Revenue distribution by vehicle type?
select sum(booking_value) as revenue, vehicle_type from uber group by vehicle_type order by revenue desc;

## 5. Revenue contribution % by vehicle type?
select vehicle_type, round(sum(booking_value) * 100 /(select sum(booking_value) from uber),2) as revenue_pct from uber group by vehicle_type order by revenue_pct desc;

## 6. Revenue by payment method?
select sum(booking_value) as revenue, pay_method from uber group by pay_method order by revenue desc;

## 7. Revenue by pickup location?
select sum(booking_value) as revenue, pickup from uber group by pickup order by revenue desc;

## 8. Revenue by day of week?
select sum(booking_value) as revenue, DAYNAME(dates) days from uber where booking_status='Completed' group by dayname(dates) order by revenue desc;

## 9. Revenue by hour of day?
select sum(booking_value) as revenue, hour(timing) hours from uber where booking_status='Completed' group by hour(timing) order by revenue desc;

## 10. Revenue per kilometer (overall)?
select (sum(booking_value) / sum(dist)) as rpk from uber where booking_status='Completed';

## 11. Revenue per kilometer by vehicle type?
select round(sum(booking_value) / sum(dist),2) as rpk, vehicle_type from uber where booking_status='Completed' group by vehicle_type;

##12. Booking value distribution (Low / Medium / High buckets)?
select case when booking_value>3000 then 'High' when booking_value>1000 then 'Medium' else 'Low' end as price_buckets, count(*) as total from uber group by price_buckets;



-- /-- 🚗 2️⃣ Ride Demand & Behavior  --/
## 13. Total number of bookings?
select count(*) from uber;

## 14. Completion vs cancellation %?
select booking_status, count(*) as total, round(count(*)*100/(select count(*) from uber)) as pct from uber group by booking_status;

## 15. Overall cancellation rate?
select round(sum(cancel_by_cust + cancel_by_driver)/count(*)*100,2) as cancel_rate from uber;

## 16. Cancellation rate by vehicle type?
select vehicle_type, round(sum(cancel_by_cust + cancel_by_driver)/count(*)*100,2) as cancel_rate from uber group by vehicle_type;

## 17. Cancellation rate by payment method?
select pay_method, round(sum(cancel_by_cust + cancel_by_driver)/count(*)*100,2) as cancel_rate from uber group by pay_method;

## 18. Cancellation rate by pickup location?
select pickup, round(sum(cancel_by_cust + cancel_by_driver)/count(*)*100,2) as cancel_rate from uber group by pickup;

## 19. Peak booking hour?
select hour(timing) as hours, count(*) as nett from uber group by hours order by nett desc;

## 20. Peak booking day of week?
select count(*) as total, DAYNAME(dates) days from uber group by dayname(dates) order by total desc;

## 21. Top 10 pickup locations by demand?
select count(*) as nett, pickup from uber group by pickup order by nett desc limit 10;

## 22. Top 10 drop locations by demand?
select count(*) as nett, drop_at from uber group by drop_at order by nett desc limit 10;

## 23. Average ride distance?
select avg(dist) from uber where booking_status='Completed';

## 24. Distance distribution buckets (Short / Medium / Long)?
select case when dist>40 then 'Long' when dist>20 then 'Medium' else 'Short' end as dist_buckets, count(*) as total from uber group by dist_buckets;

## 25. Do long-distance rides have higher cancellation rates?
select case when dist>20 then 'Long' when dist>5 then 'Medium' else 'Short' end as dist_buckets,
round(sum(cancel_by_cust + cancel_by_driver)/count(*)*100,2) as cancel_rate from uber group by dist_buckets;



-- /-- 👥 3️⃣ Customer Insights  --/
## 26. Total unique customers?
select count(distinct(customer_id)) from uber;

##27. Repeat customer rate (% customers with >1 booking)?
select round(count(distinct case when c>1 then customer_id end) / count(distinct customer_id)*100,2) as repeat_rate from
(select customer_id, count(*) c from uber group by customer_id) t;

## 28. Average bookings per customer?
select avg(c) avg_booking_per_cust from (select customer_id, count(*) c from uber group by customer_id) t; 

## 29. Top 10 high-value customers by total spend?
select customer_id, sum(booking_value) as total from uber group by customer_id order by total desc limit 10;

## 30. Average booking value per customer?
select round(sum(booking_value)/count(distinct(customer_id)),2) as avg_bvpc from uber;

## 31. Most preferred payment method?
select pay_method, count(pay_method) as nett from uber where booking_status='Completed' group by pay_method order by nett desc;

## 32. Cash vs Online booking percentage?
select pay_method, round(count(*)*100/(select count(*) from uber),2) as pct from uber where booking_status='Completed' group by pay_method order by pct desc;



-- /--  🚘 4️⃣ Ratings & Experience Analysis  -- /
## 33. Average driver rating?
select avg(driver_rating) from uber where booking_status='Completed';

## 34. Average customer rating?
select avg(cust_rating) from uber where booking_status='Completed';

## 35. Rating distribution (1–5 breakdown)?
select driver_rating, count(*) as total from uber where booking_status='Completed' group by driver_rating order by driver_rating;

## 36. Average rating by vehicle type?
select vehicle_type, avg(driver_rating) from uber where booking_status='Completed' group by vehicle_type;

## 37. Do longer rides affect ratings?
select avg(driver_rating) from uber where booking_status='Completed' and dist>15;
select avg(driver_rating), dist from uber group by dist;

## 38. Pickup locations with lowest average ratings?
select avg(driver_rating) as avg_rated, pickup from uber group by pickup order by avg_rated asc limit 10;



-- / -- ⏱ 5️⃣ Operational Efficiency  -- /
## 39. Average vehicle arrival time (avg_vtat)?
select round(avg(avg_vtat),2) from uber;

## 40. Average customer trip acceptance time (avg_ctat)?
select round(avg(avg_ctat),2) from uber;

## 41. Which vehicle type has lowest arrival time?
select round(avg(avg_vtat),2) as arrival, vehicle_type from uber group by vehicle_type order by arrival asc;

## 42. Which pickup location has highest waiting time?
select pickup, round(avg(avg_vtat),2) as arrival from uber group by pickup order by arrival desc;

## 43. Percentage of incomplete rides?
select round(sum(incomplete)/count(*)*100,2) from uber;

## 44. Main reasons for incomplete rides?
select reason, count(*) as total from uber where incomplete=1 group by reason;

## 45. Top customer cancellation reasons?
select reason_cbc, count(*) as total from uber where cancel_by_cust=1 group by reason_cbc;

## 46. Top driver cancellation reasons?
select reason_driver, count(*) as total from uber where cancel_by_driver=1 group by reason_driver;

## 47. Which hour has highest cancellations?
select hour(timing), sum(cancel_by_cust + cancel_by_driver) as cancels from uber group by hour(timing) order by cancels desc;

## 48. Which pickup locations face most driver cancellations?
select pickup, sum(cancel_by_driver) as cancels from uber group by pickup order by cancels desc;



-- / -- 📊 6️⃣ Advanced Analytical Insights (Recruiter-Level)  --/
## 49. Correlation between distance & booking value?
select (count(*) * sum(dist * booking_value) - sum(dist) * sum(booking_value)) / 
sqrt(
(count(*) * sum(dist*dist) - pow(sum(dist),2)) * (count(*) * sum(booking_value*booking_value) - pow(sum(booking_value),2))
) as corr from uber;

## 50. Correlation between arrival time & cancellation?
select (count(*) * sum(avg_vtat * cancel_by_driver) - sum(avg_vtat) * sum(cancel_by_driver)) / 
sqrt(
(count(*) * sum(avg_vtat*avg_vtat) - pow(sum(cancel_by_driver),2)) * (count(*) * sum(cancel_by_driver*cancel_by_driver) - pow(sum(cancel_by_driver),2))
) as corr from uber;

## 51. Correlation between ratings & booking value?
select (count(*) * sum(driver_rating * booking_value) - sum(driver_rating) * sum(booking_value)) / 
sqrt(
(count(*) * sum(driver_rating*driver_rating) - pow(sum(driver_rating),2)) * (count(*) * sum(booking_value*booking_value) - pow(sum(booking_value),2))
) as corr from uber;

## 52. Does peak hour increase cancellation probability?
select hour(timing), round(sum(cancel_by_cust + cancel_by_driver)/count(*)*100,2) as cancels from uber group by hour(timing);

## 53. Revenue volatility by day?
select dayname(dates) as days, round(stddev(booking_value),2) as revenue_volatility from uber group by days;

## 54. Revenue volatility by hour?
select hour(timing) as hours, round(stddev(booking_value),2) as revenue_volatility from uber group by hours;

## 55. Identify high-risk cancellation zones (pickup-based)?
select pickup, round(sum(cancel_by_cust + cancel_by_driver)/count(*)*100,2) as risk from uber group by pickup order by risk desc;

## 56. Identify high-performing vehicle types (high revenue + low cancellation)?
select vehicle_type, sum(booking_value) as revenue, round(sum(cancel_by_cust + cancel_by_driver)/count(*)*100,2) as risk from uber group by vehicle_type order by revenue desc, risk asc;

## 57. Customer lifetime value approximation (total spend per customer)?
select customer_id, sum(booking_value) as lifetime from uber group by customer_id order by lifetime desc limit 10;

## 58. Seasonal pattern in bookings (if multi-month data exists)?
select month(dates) as months, count(*) as total from uber group by months order by months;

select month(dates) as months, sum(booking_value) as revenue, count(*) as total from uber where booking_status='Completed' group by months order by months;

select case when month(dates) in (11,12,1,2) then 'Winter' when month(dates) in (3,4,5,6) then 'Summer' else 'Monsoon' end as season, count(*) as total,
sum(booking_value) as revenue from uber where booking_status='Completed' group by season;




