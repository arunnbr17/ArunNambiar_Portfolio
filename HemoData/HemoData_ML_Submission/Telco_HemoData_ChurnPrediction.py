import pandas as pd
import pyodbc
import joblib
import warnings
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import f1_score, accuracy_score, roc_auc_score

# ==========================================
# IMPORT ALL THE ALGORITHMS
# ==========================================
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, AdaBoostClassifier, HistGradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.neural_network import MLPClassifier

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# ==========================================
# 1. CONFIG & DATA FETCH
# ==========================================
DB_CONN_STR = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=MSI\SQLEXPRESS;"
    r"Database=HemoData_Telco_DB;"
    r"Trusted_Connection=yes;"
)

def fetch_data():
    print("Fetching data from SQL Server...")
    query = """
        SELECT 
            f.churn, f.tenure, f.contract, f.paperless_billing, f.payment_method, 
            CAST(f.total_charges AS FLOAT) as total_charges,
            c.gender, c.senior_citizen, c.partner, c.dependents,
            s.phone_service, s.multiple_lines, s.internet_service,
            s.online_security, s.online_backup, s.device_protection,
            s.tech_support, s.streaming_tv, s.streaming_movies,
            CAST(s.monthly_charges AS FLOAT) as monthly_charges
        FROM telco.fact_subscription f
        JOIN telco.dim_customer c ON f.customer_dim_id = c.customer_dim_id
        JOIN telco.dim_service s  ON f.service_dim_id = s.service_dim_id
    """
    conn = pyodbc.connect(DB_CONN_STR)
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure boolean flags are integers
    bool_cols = df.select_dtypes(include=['bool']).columns
    df[bool_cols] = df[bool_cols].astype(int)
    return df

# ==========================================
# 2. DEFINE THE PREPROCESSING PIPELINE
# ==========================================
NUM_FEAT = ["tenure", "monthly_charges", "total_charges", "senior_citizen", "partner", "dependents", "phone_service", "paperless_billing"]
CAT_FEAT = ["gender", "multiple_lines", "internet_service", "online_security", "online_backup", "device_protection", "tech_support", "streaming_tv", "streaming_movies", "contract", "payment_method"]

preprocessor = ColumnTransformer(transformers=[
    ('num', Pipeline(steps=[('imputer', SimpleImputer(strategy='median')), ('scaler', StandardScaler())]), NUM_FEAT),
    ('cat', Pipeline(steps=[('imputer', SimpleImputer(strategy='constant', fill_value='Missing')), ('encoder', OneHotEncoder(handle_unknown='ignore', sparse_output=False))]), CAT_FEAT)
])

# ==========================================
# 3. RUN THE ULTIMATE TOURNAMENT
# ==========================================
if __name__ == "__main__":
    df = fetch_data()
    X = df.drop(columns=['churn'])
    y = df['churn']
    
    # Stratified split is crucial for Churn data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
    
    # --- THE CONTENDERS ---
    models = {
        "LogisticRegression": LogisticRegression(max_iter=1000, class_weight='balanced'),
        "DecisionTree":       DecisionTreeClassifier(max_depth=5, class_weight='balanced'),
        "RandomForest":       RandomForestClassifier(n_estimators=100, class_weight='balanced', random_state=42),
        "GradientBoosting":   GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42),
        "HistGradientBoost":  HistGradientBoostingClassifier(random_state=42), 
        "AdaBoost":           AdaBoostClassifier(random_state=42),
        "SVM (Linear)":       SVC(kernel='linear', probability=True, class_weight='balanced', random_state=42),
        "SVM (RBF)":          SVC(kernel='rbf', probability=True, class_weight='balanced', random_state=42),
        "KNN":                KNeighborsClassifier(n_neighbors=5),
        "NaiveBayes":         GaussianNB(),
        "NeuralNet (MLP)":    MLPClassifier(hidden_layer_sizes=(100,50), max_iter=500, random_state=42)
    }
    
    print(f"\nTraining {len(models)} models... this might take a minute.")
    print("\n" + "="*70)
    print(f"{'Model Name':<20} | {'F1 Score':<10} | {'ROC-AUC':<10} | {'Accuracy':<10}")
    print("="*70)
    
    # Variables to track the winner
    best_score = -1
    best_model_name = ""
    best_pipeline = None

    for name, algo in models.items():
        try:
            # Build Pipeline
            pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('model', algo)])
            
            # Train
            pipeline.fit(X_train, y_train)
            
            # Predict
            y_pred = pipeline.predict(X_test)
            
            # Metrics
            if hasattr(pipeline, "predict_proba"):
                y_prob = pipeline.predict_proba(X_test)[:, 1]
                roc = roc_auc_score(y_test, y_prob)
            else:
                roc = 0.0 
            
            f1 = f1_score(y_test, y_pred)
            acc = accuracy_score(y_test, y_pred)
            
            print(f"{name:<20} | {f1:.4f}     | {roc:.4f}     | {acc:.4f}")
            
            # CHECK IF THIS IS THE NEW CHAMPION
            if f1 > best_score:
                best_score = f1
                best_model_name = name
                best_pipeline = pipeline
            
        except Exception as e:
            print(f"{name:<20} | FAILED: {str(e)}")

    print("-" * 70)
    
    # SAVE ONLY THE WINNER
    if best_pipeline:
        print(f"The winner is {best_model_name} with F1 Score: {best_score:.4f}")
        
        # Clean filename format
        filename = f"model_{best_model_name.replace(' ', '_').replace('(', '').replace(')', '')}.pkl"
        
        joblib.dump(best_pipeline, filename)
        print(f"SAVED: Only the best model was saved to '{filename}'.")
    else:
        print("No models were trained successfully.")