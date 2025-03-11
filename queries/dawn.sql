USE Dawn;

SHOW INDEXES FROM dawn.er_data;

-- Get_Annual_Drug_Cases_Per_DrugCode 
DELIMITER $$  
CREATE PROCEDURE Get_Annual_Drug_Cases_Per_DrugCode()  
BEGIN  
    SELECT MIN(caseid) AS sample_caseid, 
           MIN(metro) AS metro_area, 
           MIN(agecat) AS age_category, 
           MIN(sex) AS gender, 
           sdled_3_1 AS drug_code, 
           COUNT(*) AS total_cases  
    FROM dawn.er_data 
    GROUP BY sdled_3_1
    ORDER BY total_cases DESC;  
END $$  
DELIMITER ;  

CALL Get_Annual_Drug_Cases_Per_DrugCode();

-- MISSING VALUES 
DELIMITER $$  
CREATE PROCEDURE Convert_Neg7_To_Null()  
BEGIN  
    UPDATE dawn.er_data  
    SET 
        caseid = NULLIF(caseid, -7),  
        metro = NULLIF(metro, -7),  
        agecat = NULLIF(agecat, -7),  
        sex = NULLIF(sex, -7),  
        sdled_3_1 = NULLIF(sdled_3_1, -7)  ;
END $$  
DELIMITER ;

CALL Convert_Neg7_To_Null();





