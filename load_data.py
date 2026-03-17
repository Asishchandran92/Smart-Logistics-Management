# load_all_data.py

import pandas as pd
import json
from db.db_connection import get_connection


# =====================================
# CLEAR TABLES SAFELY
# =====================================
def clear_tables():
    """
    Truncate all tables in the correct order, disabling foreign key checks temporarily.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        tables = [
            "shipment_tracking",
            "costs",
            "shipments",
            "routes",
            "warehouses",
            "courier_staff"
        ]

        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")

        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        print("All tables cleared successfully 🧹")

    except Exception as e:
        conn.rollback()
        print("Error clearing tables:", e)

    finally:
        conn.close()


# =====================================
# GENERIC INSERT FUNCTION
# =====================================
def insert_dataframe(df, table_name):
    """
    Inserts a Pandas DataFrame into MySQL table safely.
    Strips whitespace from all string columns.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        df.columns = df.columns.str.strip()

        # Strip whitespace from string/object columns
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].astype(str).str.strip()

        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        data = [tuple(row) for row in df.to_numpy()]
        cursor.executemany(query, data)

        conn.commit()
        print(f"{table_name} data inserted successfully ✅")

    except Exception as e:
        conn.rollback()
        print(f"Error inserting into {table_name}:", e)

    finally:
        conn.close()


# =====================================
# LOAD CSV
# =====================================
def load_csv(file_path):
    df = pd.read_csv(file_path)
    df = df.dropna()
    return df


# =====================================
# LOAD JSON
# =====================================
def load_json(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    df = pd.DataFrame(data)
    df = df.dropna()
    return df


# =====================================
# CLEAN CHILD TABLES BASED ON PARENT
# =====================================
def clean_child_table(child_df, parent_df, key_column, child_table_name):
    """
    Drop rows in child_df where foreign key does not exist in parent_df.
    """
    initial_count = len(child_df)
    cleaned_df = child_df[child_df[key_column].str.strip().isin(
        parent_df[key_column].astype(str).str.strip()
    )].copy()
    dropped_count = initial_count - len(cleaned_df)
    if dropped_count > 0:
        print(f"⚠️ {dropped_count} rows removed from {child_table_name} due to missing {key_column}s.")
    return cleaned_df


# =====================================
# REMOVE DUPLICATES
# =====================================
def remove_duplicates(df, subset, table_name):
    """
    Remove duplicate rows based on subset of columns (primary key).
    """
    initial_count = len(df)
    df = df.drop_duplicates(subset=subset, keep="first")
    removed_count = initial_count - len(df)
    if removed_count > 0:
        print(f"⚠️ {removed_count} duplicate rows removed from {table_name} based on {subset}.")
    return df


# =====================================
# MAIN EXECUTION
# =====================================
if __name__ == "__main__":

    clear_tables()

    # ==========================
    # ROOT TABLES (NO DEPENDENCY)
    # ==========================
    print("Loading root tables...")
    courier_df = load_csv("data/courier_staff.csv")
    insert_dataframe(courier_df, "courier_staff")

    warehouses_df = load_json("data/warehouses.json")
    insert_dataframe(warehouses_df, "warehouses")

    routes_df = load_csv("data/routes.csv")
    insert_dataframe(routes_df, "routes")

    # ==========================
    # PARENT TABLES (DEPENDENT CHILD TABLES WILL REFER TO THIS)
    # ==========================
    print("Loading parent tables...")
    shipments_df = load_json("data/shipments.json")
    insert_dataframe(shipments_df, "shipments")

    # ==========================
    # CHILD TABLES (REQUIRES shipment_id)
    # ==========================
    print("Loading child tables...")

    # Costs table
    costs_df = load_csv("data/costs.csv")
    costs_df = clean_child_table(costs_df, shipments_df, "shipment_id", "costs")
    costs_df = remove_duplicates(costs_df, subset=["shipment_id"], table_name="costs")
    insert_dataframe(costs_df, "costs")

    # Shipment tracking table
    tracking_df = load_csv("data/shipment_tracking.csv")
    tracking_df = clean_child_table(tracking_df, shipments_df, "shipment_id", "shipment_tracking")
    tracking_df = remove_duplicates(tracking_df, subset=["tracking_id"], table_name="shipment_tracking")
    insert_dataframe(tracking_df, "shipment_tracking")

    print("🎉 All Data Loaded Successfully!")
