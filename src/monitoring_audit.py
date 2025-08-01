import psycopg2
from datetime import datetime
from typing import List, Dict, Any
import os

from data_quality_standards import (
    NOT_NULL_CHECKS,
    UNIQUE_CHECKS,
    FOREIGN_KEY_CHECKS,
    check_null_values,
    check_uniqueness,
    check_foreign_key_integrity,
    check_document_format,
    check_high_value_txn_strong_auth,
    check_untrusted_device_transactions,
    check_daily_total_over_20m_auth
)

CONN_PARAMS = {
    "host": os.getenv("BANKING_DB_HOST", "localhost"),
    "port": os.getenv("BANKING_DB_PORT", "5432"),
    "dbname": os.getenv("BANKING_DB_NAME", "banking_db"),
    "user": os.getenv("BANKING_DB_USER", "db_user"),
    "password": os.getenv("BANKING_DB_PASSWORD", "db_password")
}

def write_log_file(results: List[Dict[str, Any]]):
    log_dir = "/opt/airflow/logs" 
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"audit_log_{timestamp}.txt")

    passed_checks = [r for r in results if r.get('status') == 'PASS']
    failed_checks = [r for r in results if r.get('status') != 'PASS']
    
    passed_count = len(passed_checks)
    failed_count = len(failed_checks)

    with open(log_file_path, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write(f"DATA QUALITY AUDIT LOG - {datetime.now()}\n")
        f.write("="*80 + "\n\n")
        
        f.write("--- AUDIT SUMMARY ---\n")
        f.write(f"Total Checks Executed: {len(results)}\n")
        f.write(f"Passed: {passed_count}\n")
        f.write(f"Failed/Warnings: {failed_count}\n")
        f.write("\n" + "="*80 + "\n\n")
        
        if failed_checks:
            f.write("--- FAILED / WARNING CHECKS ---\n")
            for result in failed_checks:
                f.write(f"Check Name: {result.get('check_name', 'N/A')}\n")
                f.write(f"Status:     {result.get('status', 'ERROR')}\n")
                f.write(f"Message:    {result.get('message', 'An error occurred.')}\n")
                if 'failed_count' in result:
                    f.write(f"Violations: {result.get('failed_count')}\n")
                if 'failed_records' in result:
                    records_to_show = result['failed_records'][:5]
                    f.write(f"Examples:   {records_to_show}\n")
                f.write("-" * 40 + "\n")
            f.write("\n")

        if passed_checks:
            f.write("--- PASSED CHECKS ---\n")
            for result in passed_checks:
                f.write(f"Check Name: {result.get('check_name', 'N/A')}\n")
                f.write(f"Status:     {result.get('status')}\n")
                f.write(f"Message:    {result.get('message')}\n")
                f.write("-" * 40 + "\n")
            
    print(f"Detailed audit log saved to: {log_file_path}")

def print_summary_table(results: List[Dict[str, Any]]):
    print("\n" + "="*100)
    print("AUDIT SUMMARY TABLE".center(100))
    print("="*100)
    
    header = f"| {'STATUS':<8} | {'CHECK NAME':<50} | {'MESSAGE':<30} |"
    print(header)
    print("-" * 100)

    passed_count = 0
    failed_count = 0

    for result in sorted(results, key=lambda x: x.get('status') != 'PASS'):
        status = result.get('status', 'ERROR')
        check_name = result.get('check_name', 'Unknown Check')
        message = result.get('message', 'An error occurred.')

        if status == "PASS":
            passed_count += 1
        else:
            failed_count += 1
            
        if len(message) > 28:
            message = message[:25] + "..."

        row = f"| {status:<8} | {check_name:<50} | {message:<30} |"
        print(row)

    print("="*100)
    print(f"AUDIT COMPLETE: {passed_count} checks PASSED, {failed_count} checks FAILED/WARNING.")
    print("="*100)


def main():
    print(f"--- Data Quality Audit Started at {datetime.now()} ---")
    conn = None
    all_results = []

    try:
        conn = psycopg2.connect(**CONN_PARAMS)
        cur = conn.cursor()
        print("Database connection successful.")

        print("\nRunning configuration-driven checks...")

        # Null checks
        for table, columns in NOT_NULL_CHECKS.items():
            for column in columns:
                check_name = f"check_null_{table}_{column}"
                result = check_null_values(cur, table, column)
                result['check_name'] = check_name
                all_results.append(result)

        # Uniqueness checks
        for table, columns in UNIQUE_CHECKS.items():
            for column in columns:
                check_name = f"check_unique_{table}_{column}"
                result = check_uniqueness(cur, table, column)
                result['check_name'] = check_name
                all_results.append(result)
        
        # Foreign key checks
        for fk_check in FOREIGN_KEY_CHECKS:
            check_name = f"check_fk_{fk_check['table']}_{fk_check['fk_column']}"
            result = check_foreign_key_integrity(cur, **fk_check)
            result['check_name'] = check_name
            all_results.append(result)

        print("\nRunning custom format and integrity checks...")
        
        doc_format_result = check_document_format(cur)
        doc_format_result['check_name'] = "check_document_format"
        all_results.append(doc_format_result)

        # Execute risk-based checks ---
        print("\nRunning risk-based checks...")

        risk_checks_functions = [
            (check_high_value_txn_strong_auth, "risk_high_value_txn_strong_auth"),
            (check_untrusted_device_transactions, "risk_untrusted_device_transactions"),
            (check_daily_total_over_20m_auth, "risk_daily_total_over_20m_auth")
        ]

        for func, name in risk_checks_functions:
            result = func(cur)
            result['check_name'] = name
            all_results.append(result)

    except psycopg2.Error as e:
        print(f"\n DATABASE ERROR: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")

    # --- 4. Print and save results ---
    if all_results:
        print_summary_table(all_results)
        write_log_file(all_results)
    else:
        print("No checks were executed.")


if __name__ == "__main__":
    main()
