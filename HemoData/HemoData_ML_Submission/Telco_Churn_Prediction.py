import pandas as pd
import joblib
import os
import glob
import sys

# ==========================================
# 1. AUTO-LOCATE MODEL
# ==========================================
def load_best_model():
    # Find any file starting with "model_" and ending with ".pkl"
    pkl_files = glob.glob("model_*.pkl")
    
    if not pkl_files:
        print("Error: No model (.pkl) file found in this folder.")
        print("Please run '1_compare_models.py' first to generate the best model.")
        sys.exit()
    
    # If multiple exist, pick the most recent
    latest_model = max(pkl_files, key=os.path.getctime)
    
    print(f"Model found: {latest_model}")
    print(f"Loading model pipeline...")
    
    try:
        pipeline = joblib.load(latest_model)
        return pipeline, latest_model
    except Exception as e:
        print(f"Error loading model: {e}")
        sys.exit()

# ==========================================
# 2. INPUT FORM
# ==========================================
def get_user_input():
    print("\n" + "="*50)
    print("ENTER NEW CUSTOMER DETAILS")
    print("="*50)
    
    data = {}

    def ask_bool(prompt):
        while True:
            val = input(f"{prompt} (Yes/No or 1/0): ").strip().lower()
            if val in ['yes', 'y', '1', 'true']: return 1
            if val in ['no', 'n', '0', 'false']: return 0
            print("Please type 'Yes' or 'No'.")

    # --- NUMERIC INPUTS ---
    try:
        data['tenure'] = int(input("Tenure (Months): "))
        data['monthly_charges'] = float(input("Monthly Charges ($): "))
        data['total_charges'] = float(input("Total Charges ($): "))
    except ValueError:
        print("Error: Please enter valid numbers for charges/tenure.")
        return None

    # --- DROPDOWN INPUTS ---
    print("\n--- Select Options (Type the name) ---")
    
    print("Payment Methods: Electronic check, Mailed check, Bank transfer (automatic), Credit card (automatic)")
    data['payment_method'] = input("Payment Method: ").strip()
    
    print("Contract Types: Month-to-month, One year, Two year")
    data['contract'] = input("Contract: ").strip()
    
    print("Internet Service: DSL, Fiber optic, No")
    data['internet_service'] = input("Internet Service: ").strip()
    
    data['gender'] = input("Gender (Male/Female): ").strip()

    # --- BOOLEAN INPUTS ---
    print("\n--- Quick Yes/No Questions ---")
    data['senior_citizen'] = ask_bool("Senior Citizen?")
    data['partner']        = ask_bool("Has Partner?")
    data['dependents']     = ask_bool("Has Dependents?")
    data['phone_service']  = ask_bool("Has Phone Service?")
    data['paperless_billing'] = ask_bool("Paperless Billing?")
    
    # Optional Services (Defaults to No)
    data['multiple_lines']      = "No"
    data['online_security']     = "No"
    data['online_backup']       = "No"
    data['device_protection']   = "No"
    data['tech_support']        = "No"
    data['streaming_tv']        = "No"
    data['streaming_movies']    = "No"

    return pd.DataFrame([data])

# ==========================================
# 3. PREDICTION ENGINE
# ==========================================
if __name__ == "__main__":
    # 1. Load Model
    pipeline, model_name = load_best_model()
    
    while True:
        # 2. Get Data
        user_df = get_user_input()
        
        if user_df is not None:
            # 3. Predict
            print(f"\nRunning prediction using {model_name}...")
            
            prediction = pipeline.predict(user_df)[0]
            
            try:
                probability = pipeline.predict_proba(user_df)[0][1]
                conf_str = f"{probability:.1%}"
            except:
                probability = 0
                conf_str = "N/A"

            # 4. Show Result
            print("\n" + "="*50)
            print("FINAL PREDICTION RESULT")
            print("="*50)
            
            if prediction == 1:
                print(f"STATUS: HIGH RISK (CHURN)")
                print(f"Probability of Leaving: {conf_str}")
                print("Recommendation: Contact customer immediately.")
            else:
                print(f"STATUS: SAFE (ACTIVE)")
                print(f"Probability of Leaving: {conf_str}")
                print("Recommendation: Customer is stable.")
            print("="*50)
        
        # 5. Loop
        again = input("\nCheck another customer? (y/n): ").lower()
        if again != 'y':
            break
