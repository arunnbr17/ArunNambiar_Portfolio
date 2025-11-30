import logging
import traceback
import smtplib
from email.mime.text import MIMEText
import Config

# ---------------------------------------------
# LOGGING SETUP
# ---------------------------------------------
logging.basicConfig(
    filename="etl_log.txt",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------------------------------------
# EMAIL CONFIG (EDIT THESE)
# ---------------------------------------------
SENDER_EMAIL = Config.SENDER_EMAIL
SENDER_PASSWORD = Config.SENDER_PASSWORD
RECIPIENT_EMAIL = Config.RECIPIENT_EMAIL
SMTP_SERVER = Config.SMTP_SERVER
SMTP_PORT = Config.SMTP_PORT


# ---------------------------------------------
# SEND EMAIL (CALLED BY handle_error)
# ---------------------------------------------
def send_error_email(subject, body):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = SENDER_EMAIL
        msg["To"] = RECIPIENT_EMAIL

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)

    except Exception as e:
        logging.error("Failed to send error email: " + str(e))


# ---------------------------------------------
# MAIN ERROR HANDLER (CALLED FROM ETL)
# ---------------------------------------------
def handle_error(message, exception_obj):
    full_error = f"{message}\n\n{traceback.format_exc()}"
    
    logging.error(full_error)
    send_error_email("ETL Failure", full_error)
    
    raise exception_obj
