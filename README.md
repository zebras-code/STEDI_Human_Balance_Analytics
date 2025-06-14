# STEDI Human Balance Analytics

This project simulates a real-world data engineering workflow where data is processed from raw JSON sources in an S3 landing zone, transformed through a Trusted and Curated zone using AWS Glue jobs, and validated through SQL queries in Amazon Athena.

## 🔧 Tools Used
- **AWS S3**: Stores the raw and processed JSON data
- **AWS Glue Studio**: Used to create ETL jobs for sanitizing and joining data
- **AWS Glue Data Catalog**: Organizes metadata and enables querying through Athena
- **Amazon Athena**: Used to validate data outputs at each stage via SQL queries
- **GitHub**: Hosts code, SQL scripts, and screenshots
  #Project Structure
  Created tables using crawlers for:
- `customer_landing` — 956 rows
- `accelerometer_landing` — 81273 rows
- `step_trainer_landing` — 28680 rows  
*→ Athena screenshots available.*

### Trusted Zone
Created Glue jobs to filter data:
- `customer_trusted.py` → filters customers who agreed to share data
- `accelerometer_trusted.py` → joins with `customer_trusted` by email
- `step_trainer_trusted.py` → joins with curated customers by serialNumber  
*→ Athena screenshots and row counts verified.*

### Curated Zone
- `customers_curated_job.py` → inner join of `customer_trusted` + `accelerometer_trusted`
- `machine_learning_curated.py` → joins `step_trainer_trusted` + `accelerometer_trusted` on timestamp  
*→ Final curated tables and counts validated.*

---

##  Screenshots Included
All Athena query results are provided in the `/screenshots/` folder and match Udacity rubric expectations, including:
- customer_landing
- accelerometer_trusted
- customers_curated
- machine_learning_curated

---
