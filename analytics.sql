-- Top 5 Stadium by Capacity
SELECT TOP 5 rank, stadium, capacity
FROM stadium
ORDER BY capacity DESC;

-- Average Capacity of the stadiums by region
SELECT region, AVG(capacity) AS Average_Capacity 
FROM stadium
GROUP BY region
ORDER BY Average_Capacity DESC;

-- Count the number of stadiums in each country
SELECT country, COUNT(*) AS Number_of_Stadiums
FROM stadium
GROUP BY country
ORDER BY Number_of_Stadiums DESC, country ASC;

-- stadium ranking with each region
SELECT rank , region, stadium, capacity,
    RANK() OVER(PARTITION BY region ORDER BY capacity DESC) AS region_rank
FROM stadium

-- Top 3 stadium ranking with capacity
SELECT rank , region, stadium, capacity, region_rank FROM (
    SELECT rank , region, stadium, capacity,
        RANK() OVER(PARTITION BY region ORDER BY capacity DESC) AS region_rank
    FROM stadium) as ranked_stadiums
WHERE region_rank <= 3;

-- Stadium with capacity above the average
SELECT stadium, t2.region, capacity, avg_capacity
FROM stadium, (SELECT region, AVG(capacity) FROM stadium GROUP BY region) t2
WHERE stadium.region = t2.region
and capacity > avg_capacity;

-- Stadiums with closes capacity to regional median
WITH MedianCTE AS (
    SELECT DISTINCT region, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY capacity) OVER (PARTITION BY region) as median_capacity
    FROM stadium
)

SELECT rank, stadium, region, capacity, median_capacity, ranked_stadiums.median_rank
FROM (
    SELECT s.rank, s.stadium, s.region, s.capacity, m.median_capacity,
    ROW_NUMBER() OVER (PARTITION BY s.region ORDER BY ABS(s.capacity - m.median_capacity)) AS median_rank
    FROM stadium s JOIN MedianCTE m ON s.region = m.region
) as ranked_stadiums
WHERE median_rank = 1;