import pandas as pd
import pyodbc
from ydata_profiling import ProfileReport

# 1. CONNECT TO SQL (Single Source of Truth)
conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=MSI\SQLEXPRESS;"
    r"Database=HemoData_Telco_DB;"
    r"Trusted_Connection=yes;"
)

query = """
    SELECT 
        -- From Fact Table (f)
        f.churn, 
        f.tenure, 
        f.contract, 
        f.total_charges, 
        f.payment_method,   -- <--- This was the culprit! Now strictly pulling from Fact.
        f.paperless_billing,

        -- From Customer Dimension (c)
        c.gender, 
        c.senior_citizen, 
        c.partner, 
        c.dependents,

        -- From Service Dimension (s)
        s.internet_service, 
        s.monthly_charges,
        s.phone_service,
        s.multiple_lines,
        s.online_security,
        s.tech_support
    FROM telco.fact_subscription f
    JOIN telco.dim_customer c ON f.customer_dim_id = c.customer_dim_id
    JOIN telco.dim_service s  ON f.service_dim_id = s.service_dim_id
"""

# 2. LOAD DATA
conn = pyodbc.connect(conn_str)
df = pd.read_sql(query, conn)
conn.close()

# 3. GENERATE ENTERPRISE REPORT
# 'explorative=True' enables deeper correlation checks and text analysis
profile = ProfileReport(df, title="Telco Churn Data Profile", explorative=True)

# 4. SAVE AS HTML
profile.to_file("Telco_Data_Profile_Report.html")

print("✅ Report generated! Open 'Telco_Data_Profile_Report.html' in your browser.")