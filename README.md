# data-engineering-pipeline
Final Team Project - Practical Data Engineering (ADS-507-01) This repository contains a fully reproducible ETL/ELT data pipeline

## Installation

Azure Database MySQL server Host:  mysqldchen.mysql.database.azure.com
User: dchenAdmin
Password: 507password!
DATABASEs: 
'dawn'
'cdcyrbss'
'icpsr_03088'

Data Download Links:
CDC Youth Risk Behavior Study:  https://www.cdc.gov/yrbs/data/index.html#cdc_data_surveillance_section_6-dataset-file-formats
Drug Abuse Warning Network (Dawn): https://www.icpsr.umich.edu/web/NAHDAP/studies/34565/versions/V3
Alcohol and Drug Services Study (ADSS): https://www.icpsr.umich.edu/web/NAHDAP/studies/3088#

Uplooad datafiles to azure database

To run STORED PROCEDURES on Azure, Install SQL Server extension for VS Code https://marketplace.visualstudio.com/items?itemName=ms-mssql.mssql

## How to Monitor Pipeline

Azure SQL server Metrics link: 
https://portal.azure.com/#@darrenchenoutlook.onmicrosoft.com/blade/Microsoft_Azure_MonitoringMetrics/Metrics.ReactView/Referer/MetricsExplorer/ResourceId/%2Fsubscriptions%2F464a0109-8f64-4735-bee3-522cbdb76f9b%2FresourceGroups%2FADS507%2Fproviders%2FMicrosoft.DBforMySQL%2FflexibleServers%2Fmysqldchen/TimeContext/%7B%22relative%22%3A%7B%22duration%22%3A86400000%7D%2C%22showUTCTime%22%3Afalse%2C%22grain%22%3A1%7D/ChartDefinition/%7B%22v2charts%22%3A%5B%7B%22metrics%22%3A%5B%7B%22resourceMetadata%22%3A%7B%22id%22%3A%22%2Fsubscriptions%2F464a0109-8f64-4735-bee3-522cbdb76f9b%2FresourceGroups%2FADS507%2Fproviders%2FMicrosoft.DBforMySQL%2FflexibleServers%2Fmysqldchen%22%7D%2C%22name%22%3A%22Com_select%22%2C%22aggregationType%22%3A1%2C%22namespace%22%3A%22microsoft.dbformysql%2Fflexibleservers%22%2C%22metricVisualization%22%3A%7B%22displayName%22%3A%22Com%20Select%22%7D%7D%2C%7B%22resourceMetadata%22%3A%7B%22id%22%3A%22%2Fsubscriptions%2F464a0109-8f64-4735-bee3-522cbdb76f9b%2FresourceGroups%2FADS507%2Fproviders%2FMicrosoft.DBforMySQL%2FflexibleServers%2Fmysqldchen%22%7D%2C%22name%22%3A%22Queries%22%2C%22aggregationType%22%3A1%2C%22namespace%22%3A%22microsoft.dbformysql%2Fflexibleservers%22%2C%22metricVisualization%22%3A%7B%22displayName%22%3A%22Queries%22%7D%7D%5D%2C%22title%22%3A%22Sum%20Com%20Select%20and%20Sum%20Queries%20for%20mysqldchen%22%2C%22titleKind%22%3A1%2C%22visualization%22%3A%7B%22chartType%22%3A2%2C%22legendVisualization%22%3A%7B%22isVisible%22%3Atrue%2C%22position%22%3A2%2C%22hideHoverCard%22%3Afalse%2C%22hideLabelNames%22%3Atrue%7D%2C%22axisVisualization%22%3A%7B%22x%22%3A%7B%22isVisible%22%3Atrue%2C%22axisType%22%3A2%7D%2C%22y%22%3A%7B%22isVisible%22%3Atrue%2C%22axisType%22%3A1%7D%7D%7D%7D%5D%7D

https://portal.azure.com/#blade/AppInsightsExtension/UsageNotebookBlade/ComponentId/%2Fsubscriptions%2F464a0109-8f64-4735-bee3-522cbdb76f9b%2Fresourcegroups%2Fads507%2Fproviders%2Fmicrosoft.dbformysql%2Fflexibleservers%2Fmysqldchen/ConfigurationId/%2Fsubscriptions%2F464a0109-8f64-4735-bee3-522cbdb76f9b%2FresourceGroups%2FADS507%2Fproviders%2Fmicrosoft.insights%2Fworkbooks%2F6F0A4B46-6FF6-4A75-BF0E-522B6A8314C5/Type/workbook/WorkbookTemplateName/Location%20Workbook

statement analysis:  
select * from sys.`x$statement_analysis`

statements in top 5 percent by runtime. 
select * from sys.`x$statements_with_runtimes_in_95th_percentile`

## License

[MIT](https://choosealicense.com/licenses/mit/)    

# GITIGNORE
# Additional data files
additional_files = [
    "data/Drug Abuse Warning Network (DAWN) ICPSR_34565/DS0001/34565-0001-Data.csv",
    "data/National Comorbidity Survey ICPSR_28581/DS0006/28581-0006-Documentation-KBIT_Manual_and_Easel.zip"
]