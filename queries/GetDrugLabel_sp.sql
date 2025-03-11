CREATE PROCEDURE GetDrugLabel(
    IN sdled_3_1 INT,
    OUT Drug_Label VARCHAR(255)
)
BEGIN
    SELECT Drug_Label INTO Drug_Label
    FROM DrugLookupTable
    WHERE Value = sdled_3_1 
    ;
END;
