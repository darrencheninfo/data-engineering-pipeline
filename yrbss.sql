DELIMITER $$  
CREATE PROCEDURE Get_BingeDrinking_Trends()  
BEGIN  
    SELECT year, AVG(binge_drinking_rate) AS avg_binge_drinking  
    FROM YRBSS  
    GROUP BY year  
    ORDER BY year;  
END $$  
DELIMITER ;  
