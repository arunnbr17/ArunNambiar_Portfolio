import pandas as pd
import pyodbc
import Config
from ErrorHandler import handle_error


class TelcoETL:

    def __init__(self, csv_path, conn_string):
        self.csv_path = csv_path
        self.conn_string = conn_string
        self.conn = None
        self.cursor = None
        self.df = None
        self.customer_lookup = {}
        self.service_lookup = {}

    # ------------------------------------------------
    # CONNECT TO SQL SERVER
    # ------------------------------------------------
    def connect(self):
        try:
            self.conn = pyodbc.connect(self.conn_string)
            self.cursor = self.conn.cursor()
        except Exception as e:
            handle_error("Failed to connect to SQL Server", e)

    # ------------------------------------------------
    # LOAD AND CLEAN CSV
    # ------------------------------------------------
    def load_and_clean(self):
        try:
            df = pd.read_csv(self.csv_path)

            # Strip whitespace
            df = df.apply(lambda col: col.str.strip() if col.dtype == 'object' else col)

            # Convert Yes/No → 1/0
            yes_no_cols = ["Partner", "Dependents", "PhoneService", "PaperlessBilling", "Churn"]
            for c in yes_no_cols:
                df[c] = df[c].map({"Yes": 1, "No": 0}).fillna(0).astype(int)

            # Numeric conversions
            df["MonthlyCharges"] = pd.to_numeric(df["MonthlyCharges"], errors="coerce").fillna(0)
            df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce").fillna(0)

            self.df = df

        except Exception as e:
            handle_error("Failed during CSV load and clean", e)

    # ------------------------------------------------
    # LOAD DIM CUSTOMER
    # ------------------------------------------------
    def load_dim_customer(self):
        try:
            dim_customer = self.df[[
                "customerID", "gender", "SeniorCitizen", "Partner", "Dependents"
            ]].drop_duplicates()

            for _, row in dim_customer.iterrows():
                self.cursor.execute("""
                    INSERT INTO telco.dim_customer (
                        customerID, gender, senior_citizen, partner, dependents
                    ) VALUES (?,?,?,?,?)
                """,
                row["customerID"], row["gender"], row["SeniorCitizen"],
                row["Partner"], row["Dependents"])

            self.conn.commit()

            # Build lookup
            self.cursor.execute("SELECT customer_dim_id, customerID FROM telco.dim_customer")
            self.customer_lookup = {row.customerID: row.customer_dim_id for row in self.cursor.fetchall()}

        except Exception as e:
            handle_error("Failed loading dim_customer", e)

    # ------------------------------------------------
    # LOAD DIM SERVICE
    # ------------------------------------------------
    def load_dim_service(self):
        try:
            dim_service = self.df[[
                "PhoneService", "MultipleLines", "InternetService",
                "OnlineSecurity", "OnlineBackup", "DeviceProtection",
                "TechSupport", "StreamingTV", "StreamingMovies",
                "MonthlyCharges"
            ]].drop_duplicates()

            for _, row in dim_service.iterrows():
                self.cursor.execute("""
                    INSERT INTO telco.dim_service (
                        phone_service, multiple_lines, internet_service,
                        online_security, online_backup, device_protection,
                        tech_support, streaming_tv, streaming_movies,
                        monthly_charges
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                row["PhoneService"], row["MultipleLines"], row["InternetService"],
                row["OnlineSecurity"], row["OnlineBackup"], row["DeviceProtection"],
                row["TechSupport"], row["StreamingTV"], row["StreamingMovies"],
                row["MonthlyCharges"])

            self.conn.commit()

            # Build lookup
            self.cursor.execute("""
                SELECT service_dim_id,
                       phone_service, multiple_lines, internet_service,
                       online_security, online_backup, device_protection,
                       tech_support, streaming_tv, streaming_movies,
                       monthly_charges
                FROM telco.dim_service
            """)

            for row in self.cursor.fetchall():
                key = (
                    row.phone_service, row.multiple_lines, row.internet_service,
                    row.online_security, row.online_backup, row.device_protection,
                    row.tech_support, row.streaming_tv, row.streaming_movies,
                    float(row.monthly_charges)
                )

                self.service_lookup[key] = row.service_dim_id

        except Exception as e:
            handle_error("Failed loading dim_service", e)

    # ------------------------------------------------
    # LOAD FACT SUBSCRIPTION
    # ------------------------------------------------
    def load_fact_subscription(self):
        try:
            for _, row in self.df.iterrows():

                customer_dim_id = self.customer_lookup[row["customerID"]]

                service_key = (
                    row["PhoneService"], row["MultipleLines"], row["InternetService"],
                    row["OnlineSecurity"], row["OnlineBackup"], row["DeviceProtection"],
                    row["TechSupport"], row["StreamingTV"], row["StreamingMovies"],
                    float(row["MonthlyCharges"])
                )

                service_dim_id = self.service_lookup[service_key]

                self.cursor.execute("""
                    INSERT INTO telco.fact_subscription (
                        customer_dim_id, service_dim_id,
                        tenure, contract, paperless_billing,
                        payment_method, total_charges, churn
                    ) VALUES (?,?,?,?,?,?,?,?)
                """,
                customer_dim_id, service_dim_id,
                row["tenure"], row["Contract"], row["PaperlessBilling"],
                row["PaymentMethod"], row["TotalCharges"], row["Churn"])

            self.conn.commit()

        except Exception as e:
            handle_error("Failed loading fact_subscription", e)

    # ------------------------------------------------
    # RUN ALL STEPS
    # ------------------------------------------------
    def run(self):
        try:
            self.connect()
            self.load_and_clean()
            self.load_dim_customer()
            self.load_dim_service()
            self.load_fact_subscription()

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()


# ------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------
if __name__ == "__main__":
    CSV_PATH = Config.path
    CONN_STRING = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=MSI\SQLEXPRESS;"
        r"Database=HemoData_Telco_DB;"
        r"Trusted_Connection=yes;"
    )

    etl = TelcoETL(CSV_PATH, CONN_STRING)
    etl.run()

    print("ETL completed successfully.")
