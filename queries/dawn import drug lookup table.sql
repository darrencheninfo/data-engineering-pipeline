-- import drug lookup table

GRANT FILE ON *.* TO 'dchenAdmin'@'%';
FLUSH PRIVILEGES;

LOAD DATA LOCAL INFILE 'C:/Users/darre/OneDrive/Documents/ADS/ADS 507 Data Engineering/data/Drug Abuse Warning Network (DAWN) ICPSR_34565/drug_lookup.csv'
INTO TABLE DrugLookupTable
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Value, Drug_Label);

CREATE PROCEDURE GetDrugLabel(
    IN sdled_3_1 INT,
    OUT Drug_Label VARCHAR(255)
)
BEGIN
    SELECT Drug_Label INTO Drug_Label
    FROM DrugLookupTable
    WHERE Value = sdled_3_1;
END;
