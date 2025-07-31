from typing import Dict, Any

# Data quality checks

NOT_NULL_CHECKS = {
    'customers': ['full_name', 'date_of_birth', 'gender', 'phone_number', 'email', 'status', 'password_hash'],
    'customeridentitydocuments': ['customer_id', 'document_number', 'document_type', 'nationality'],
    'biometricdata': ['customer_id', 'biometric_type', 'template_hash'],
    'accounts': ['customer_id', 'account_number', 'account_type', 'balance', 'currency', 'status'],
    'devices': ['device_identifier', 'device_type', 'status'],
    'customerdevicelinks': ['customer_id', 'device_id', 'trust_status', 'is_active_session'],
    'transactionlimits': ['customer_id', 'limit_type', 'limit_amount', 'currency'],
    'dailylimittrackers': ['customer_id', 'transaction_type_group', 'total_daily_amount', 'running_total_amount', 'tracking_date'],
    'transactions': ['source_account_id', 'device_id', 'transaction_type', 'amount', 'status'],
    'authlogs': ['customer_id', 'device_id', 'auth_method', 'result'],
    'risktags': ['customer_id', 'tag_type'],
}

UNIQUE_CHECKS = {
    'customers': ['phone_number', 'email'],
    'customeridentitydocuments': ['document_number'],
    'biometricdata': ['customer_id'],
    'accounts': ['account_number'],
    'devices': ['device_identifier'],
}

FOREIGN_KEY_CHECKS = [
    {'table': 'customeridentitydocuments', 'fk_column': 'customer_id', 'parent_table': 'customers', 'pk_column': 'customer_id'},
    {'table': 'biometricdata', 'fk_column': 'customer_id', 'parent_table': 'customers', 'pk_column': 'customer_id'},
    {'table': 'accounts', 'fk_column': 'customer_id', 'parent_table': 'customers', 'pk_column': 'customer_id'},
    {'table': 'transactions', 'fk_column': 'source_account_id', 'parent_table': 'accounts', 'pk_column': 'account_id'},
    {'table': 'authlogs', 'fk_column': 'customer_id', 'parent_table': 'customers', 'pk_column': 'customer_id'},
]


def check_null_values(cur, table: str, column: str) -> Dict[str, Any]:
    """Generic function to check for NULL values in a column."""
    query = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL;"
    cur.execute(query)
    count = cur.fetchone()[0]
    if count == 0:
        return {"status": "PASS", "message": f"[{table}.{column}] No NULL values found."}
    else:
        return {
            "status": "FAIL",
            "message": f"[{table}.{column}] Found {count} NULL values.",
            "failed_count": count
        }

def check_uniqueness(cur, table: str, column: str) -> Dict[str, Any]:
    query = f"SELECT COUNT(*) FROM (SELECT {column} FROM {table} GROUP BY {column} HAVING COUNT(*) > 1) as duplicates;"
    cur.execute(query)
    duplicate_groups = cur.fetchone()[0]
    if duplicate_groups == 0:
        return {"status": "PASS", "message": f"[{table}.{column}] All values are unique."}
    else:
        return {
            "status": "FAIL",
            "message": f"[{table}.{column}] Found {duplicate_groups} groups of duplicate values.",
            "failed_count": duplicate_groups
        }

def check_foreign_key_integrity(cur, table: str, fk_column: str, parent_table: str, pk_column: str) -> Dict[str, Any]:
    query = f"""
        SELECT COUNT(t1.{fk_column})
        FROM {table} t1
        LEFT JOIN {parent_table} t2 ON t1.{fk_column} = t2.{pk_column}
        WHERE t1.{fk_column} IS NOT NULL AND t2.{pk_column} IS NULL;
    """
    cur.execute(query)
    orphaned_count = cur.fetchone()[0]
    if orphaned_count == 0:
        return {"status": "PASS", "message": f"FK Integrity OK: [{table}.{fk_column}] -> [{parent_table}.{pk_column}]."}
    else:
        return {
            "status": "FAIL",
            "message": f"FK Violation: Found {orphaned_count} orphaned records in [{table}].",
            "failed_count": orphaned_count
        }

def check_document_format(cur) -> Dict[str, Any]:
    query = r"""
        SELECT COUNT(*)
        FROM CustomerIdentityDocuments
        WHERE
            (document_type = 'CCCD' AND document_number !~ '^\d{12}$') OR
            (document_type = 'Passport' AND document_number !~ '^[A-Z]\d{7}$');
    """
    cur.execute(query)
    invalid_count = cur.fetchone()[0]
    if invalid_count == 0:
        return {"status": "PASS", "message": "[CustomerIdentityDocuments] CCCD and Passport formats are valid."}
    else:
        return {
            "status": "FAIL",
            "message": f"[CustomerIdentityDocuments] Found {invalid_count} documents with invalid format.",
            "failed_count": invalid_count
        }

# Risk-Based Checks

def check_high_value_txn_strong_auth(cur) -> Dict[str, Any]:
    strong_auth_methods = "('sms_otp', 'soft_otp', 'biometric_faceid')"
    
    query = f"""
        WITH high_value_txns AS (
            SELECT transaction_id
            FROM Transactions
            WHERE amount > 10000000 AND status = 'completed'
        ),
        strongly_authed_txns AS (
            SELECT DISTINCT transaction_id
            FROM AuthLogs
            WHERE result = 'success' AND auth_method IN {strong_auth_methods}
        )
        SELECT COUNT(hvt.transaction_id)
        FROM high_value_txns hvt
        LEFT JOIN strongly_authed_txns sa ON hvt.transaction_id = sa.transaction_id
        WHERE sa.transaction_id IS NULL;
    """
    cur.execute(query)
    count = cur.fetchone()[0]
    if count == 0:
        return {"status": "PASS", "message": "[Risk] High-value transactions (>10M VND) comply with strong auth."}
    else:
        return {
            "status": "FAIL",
            "message": f"[Risk] Found {count} high-value transactions lacking strong auth.",
            "failed_count": count
        }

def check_untrusted_device_transactions(cur) -> Dict[str, Any]:
    query = """
        SELECT t.status
        FROM Transactions t
        JOIN Accounts a ON t.source_account_id = a.account_id
        JOIN CustomerDeviceLinks cdl ON t.device_id = cdl.device_id AND a.customer_id = cdl.customer_id
        WHERE cdl.trust_status = 'unverified';
    """
    cur.execute(query)
    untrusted_transactions = cur.fetchall()
    
    total_untrusted_txns = len(untrusted_transactions)
    
    if total_untrusted_txns == 0:
        return {"status": "PASS", "message": "[Risk] No transactions found from unverified devices."}
    else:
        successful_count = sum(1 for txn in untrusted_transactions if txn[0] == 'completed')
        return {
            "status": "WARNING",
            "message": f"[Risk] Found {total_untrusted_txns} txns from unverified devices ({successful_count} successful).",
            "failed_count": total_untrusted_txns,
            "details": {"successful_from_untrusted": successful_count}
        }

def check_daily_total_over_20m_auth(cur) -> Dict[str, Any]:
    strong_auth_methods = "('sms_otp', 'soft_otp', 'biometric_faceid')"

    query = f"""
        WITH daily_totals AS (
            SELECT
                a.customer_id,
                t.created_at::date as transaction_date
            FROM Transactions t
            JOIN Accounts a ON t.source_account_id = a.account_id
            WHERE t.status = 'completed'
            GROUP BY 1, 2
            HAVING SUM(t.amount) > 20000000
        ),
        daily_strong_auths AS (
            SELECT DISTINCT
                a.customer_id,
                t.created_at::date as transaction_date
            FROM Transactions t
            JOIN Accounts a ON t.source_account_id = a.account_id
            JOIN AuthLogs al ON t.transaction_id = al.transaction_id
            WHERE t.status = 'completed'
              AND al.result = 'success'
              AND al.auth_method IN {strong_auth_methods}
        )
        SELECT COUNT(dt.customer_id)
        FROM daily_totals dt
        LEFT JOIN daily_strong_auths dsa ON dt.customer_id = dsa.customer_id AND dt.transaction_date = dsa.transaction_date
        WHERE dsa.customer_id IS NULL;
    """
    cur.execute(query)
    count = cur.fetchone()[0]
    if count == 0:
        return {"status": "PASS", "message": "[Risk] Daily totals >20M VND comply with strong auth."}
    else:
        return {
            "status": "FAIL",
            "message": f"[Risk] Found {count} customer/day instances violating the >20M daily total rule.",
            "failed_count": count
        }
