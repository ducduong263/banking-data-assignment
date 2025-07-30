import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
import hashlib
import uuid

# --- 1. THIẾT LẬP KẾT NỐI VÀ CÁC HẰNG SỐ ---
CONN_PARAMS = {
    "host": "localhost",
    "port": "5432",
    "dbname": "banking_db",
    "user": "db_user",
    "password": "db_password"
}

NUM_CUSTOMERS = 200
NUM_DEVICES = 150

fake = Faker('vi_VN')

used_account_numbers = set()

def get_db_connection():
    """Tạo và trả về một kết nối CSDL."""
    return psycopg2.connect(**CONN_PARAMS)

def clear_all_tables(conn):
    """Xóa toàn bộ dữ liệu từ các bảng để bắt đầu lại."""
    with conn.cursor() as cur:
        print("Clearing all existing data from tables...")
        cur.execute("""
            TRUNCATE TABLE 
                RiskTags, AuthLogs, DailyLimitTrackers, Transactions, 
                CustomerDeviceLinks, Devices, Accounts, TransactionLimits, 
                BiometricData, CustomerIdentityDocuments, Customers
            RESTART IDENTITY CASCADE;
        """)
        print("All tables cleared successfully.")

def generate_customers(cur, count):
    print(f"Generating {count} customers...")
    customers_data = []
    for _ in range(count):
        full_name = fake.name()
        for prefix in ['Bác ', 'Anh ', 'Chị ','Cô','Bà','Ông']:
            if full_name.startswith(prefix):
                full_name = full_name[len(prefix):]

        dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
        password = fake.password(length=12)
        pin = str(random.randint(100000, 999999))
        
        status = random.choices(['active', 'inactive', 'suspended'], weights=[0.90, 0.08, 0.02])[0]
        
        phone_number = f"+84{random.randint(100000000, 999999999)}"
        
        customers_data.append((
            full_name, dob, random.choice(['male', 'female', 'other']),
            fake.address(), 
            phone_number,
            fake.unique.email(), status,
            hashlib.sha256(password.encode()).hexdigest(),
            hashlib.sha256(pin.encode()).hexdigest(),
        ))
    
    insert_query = """
        INSERT INTO Customers (full_name, date_of_birth, gender, address, phone_number, email, status, password_hash, pin_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, customers_data)
    
    cur.execute("SELECT customer_id, status, address FROM Customers;")
    customers_info = cur.fetchall()
    print(f"-> Generated {len(customers_info)} customers.")
    return customers_info


def generate_devices(cur, count):
    print(f"Generating {count} devices...")
    devices_data = []
    
    device_name_map = {
        ('mobile', 'iOS'): ["iPhone 15 Pro Max", "iPhone 14", "iPhone 13 Pro"],
        ('mobile', 'Android'): ["Samsung Galaxy S24 Ultra", "Oppo Reno11", "Xiaomi 14"],
        ('desktop', 'Windows'): ["Dell XPS Tower", "HP Spectre", "Lenovo ThinkCentre"],
        ('desktop', 'macOS'): ["MacBook Pro 16-inch", "iMac 24-inch", "Mac mini"],
        ('tablet', 'iPadOS'): ["iPad Pro 12.9-inch", "iPad Air"],
        ('tablet', 'Android'): ["Samsung Galaxy Tab S9", "Xiaomi Pad 6"]
    }

    for _ in range(count):
        device_type = random.choice(['mobile', 'desktop', 'tablet'])
        os_map = {
            'mobile': ['iOS 17.5', 'Android 14', 'iOS 16.0'],
            'desktop': ['Windows 11', 'macOS Sonoma', 'Windows 10'],
            'tablet': ['iPadOS 17.5', 'Android 14']
        }
        device_os = random.choice(os_map[device_type])
        
        os_key = 'iOS' if 'iOS' in device_os else 'iPadOS' if 'iPadOS' in device_os else 'macOS' if 'macOS' in device_os else 'Windows' if 'Windows' in device_os else 'Android'
        device_name = random.choice(device_name_map.get((device_type, os_key), ["Generic Device"]))

        devices_data.append((
            str(uuid.uuid4()), device_name, device_type, device_os,
            datetime.now(), 'active'
        ))

    insert_query = """
        INSERT INTO Devices (device_identifier, device_name, device_type, device_os, last_login_at, status)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, devices_data)

    cur.execute("SELECT device_id FROM Devices;")
    device_ids = [row[0] for row in cur.fetchall()]
    print(f"-> Generated {len(device_ids)} devices.")
    return device_ids

def generate_customer_device_links(cur, customer_ids, device_ids):
    print("Generating customer-device links...")
    links_data = []
    for customer_id in customer_ids:
        num_devices_for_customer = random.randint(1, 3)
        assigned_devices = random.sample(device_ids, num_devices_for_customer)
        
        links_data.append((customer_id, assigned_devices[0], 'verified', True))
        
        for i in range(1, len(assigned_devices)):
            links_data.append((customer_id, assigned_devices[i], 'unverified', False))

    insert_query = """
        INSERT INTO CustomerDeviceLinks (customer_id, device_id, trust_status, is_active_session)
        VALUES (%s, %s, %s, %s);
    """
    cur.executemany(insert_query, links_data)
    print("-> Generated customer-device links.")

def generate_identity_documents(cur, customers_info):
    print("Generating customer identity documents...")
    docs_data = []
    for customer_id, status, address in customers_info:
        if status == 'active':
            doc_type = random.choice(['CCCD', 'Passport'])
            
            # Cải tiến: Tạo nơi cấp phát thực tế hơn dựa trên loại giấy tờ
            if doc_type == 'CCCD':
                doc_number = f"{random.randint(10**11, 10**12-1)}"
                issue_place = "Cục Cảnh sát quản lý hành chính về trật tự xã hội"
            else: # Passport
                doc_number = f"{random.choice('BCK')}{random.randint(10**6, 10**7-1)}"
                issue_place = "Cục Quản lý Xuất nhập cảnh"
            
            issue_date = fake.date_between(start_date='-3y', end_date='-2y')
            expiry_date = fake.date_between(start_date='+2y', end_date='+3y')
            
            docs_data.append((
                customer_id, doc_number, doc_type, 'Vietnam',
                issue_date, expiry_date, issue_place
            ))
    
    insert_query = """
        INSERT INTO CustomerIdentityDocuments (customer_id, document_number, document_type, nationality, issue_date, expiry_date, issue_place)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, docs_data)
    print(f"-> Generated {len(docs_data)} identity documents.")

def generate_biometric_data(cur, customers_info):
    print("Generating biometric data...")
    bio_data = []
    for customer_id, status, _ in customers_info:
        if status in ('active', 'suspended'):
            bio_data.append((
                customer_id,
                'face', # Chỉ có loại face
                hashlib.sha256(str(customer_id).encode()).hexdigest() # Hash giả lập
            ))
    
    insert_query = "INSERT INTO BiometricData (customer_id, biometric_type, template_hash) VALUES (%s, %s, %s);"
    cur.executemany(insert_query, bio_data)
    print(f"-> Generated {len(bio_data)} biometric records.")

def generate_transaction_limits(cur, customer_ids):
    print("Generating transaction limits...")
    limits_data = []
    
    daily_limit_options = [500000000.0, 1000000000.0, 2000000000.0, 5000000000.0]
    per_transaction_options = [100000000.0, 500000000.0, 1000000000.0]

    for customer_id in customer_ids:
        daily_total = random.choice(daily_limit_options)

        valid_per_transaction_options = [p for p in per_transaction_options if p <= daily_total]
        
        if not valid_per_transaction_options:
            per_transaction = min(per_transaction_options)
        else:
            per_transaction = random.choice(valid_per_transaction_options)

        limits_data.append((customer_id, 'DAILY_TOTAL', daily_total, 'VND'))
        limits_data.append((customer_id, 'PER_TRANSACTION', per_transaction, 'VND'))
    
    insert_query = "INSERT INTO TransactionLimits (customer_id, limit_type, limit_amount, currency) VALUES (%s, %s, %s, %s);"
    cur.executemany(insert_query, limits_data)
    print(f"-> Generated {len(limits_data)} transaction limit records.")

def generate_accounts(cur, customers_info):
    """Sửa đổi: Chỉ những customer đã active mới có account."""
    print("Generating accounts for active customers...")
    accounts_data = []
    used_account_numbers.clear()

    active_customers = [info for info in customers_info if info[1] == 'active']
    customer_ids = [info[0] for info in active_customers]

    for customer_id in customer_ids:
        for _ in range(random.randint(1, 2)):
            while True:
                acc_num = f"102{random.randint(10**9, 10**10-1)}"
                if acc_num not in used_account_numbers:
                    used_account_numbers.add(acc_num)
                    break
            
            has_card = random.choice([True, False])
            accounts_data.append((
                customer_id, acc_num,
                random.choices(['payment', 'savings'], weights=[0.8, 0.2])[0],
                random.uniform(100000, 50000000),
                random.choices(['active', 'inactive', 'closed', 'frozen'], weights=[0.9, 0.05, 0.03, 0.02])[0],
                f"512345******{random.randint(1000,9999)}" if has_card else None,
                fake.future_date(end_date="+3y") if has_card else None,
                'active' if has_card else None
            ))
    
    insert_query = """
        INSERT INTO Accounts (customer_id, account_number, account_type, balance, status, card_number_masked, card_expiry_date, card_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, accounts_data)
    
    cur.execute("SELECT customer_id, account_id FROM Accounts WHERE status = 'active';")
    customer_accounts_map = {}
    for cid, aid in cur.fetchall():
        if cid not in customer_accounts_map:
            customer_accounts_map[cid] = []
        customer_accounts_map[cid].append(aid)
    print("-> Generated accounts for active customers.")
    return customer_accounts_map

def generate_transactions(cur, customer_accounts_map, limits):
    """Sửa đổi: Chỉ account active giao dịch, tuân thủ hạn mức và chỉ dùng device đã verified."""
    print("Generating transactions...")
    transactions_data = []
    if not customer_accounts_map:
        print("-> No active accounts to generate transactions for.")
        return

    all_active_account_ids = [aid for sublist in customer_accounts_map.values() for aid in sublist]
    
    for customer_id, account_ids in customer_accounts_map.items():
        cur.execute("SELECT device_id FROM CustomerDeviceLinks WHERE customer_id = %s AND trust_status = 'verified';", (customer_id,))
        verified_devices = [row[0] for row in cur.fetchall()]
        if not verified_devices:
            continue 
        per_transaction_limit = limits.get(customer_id, {}).get('PER_TRANSACTION', 100000000)
        per_transaction_limit_float = float(per_transaction_limit)

        for account_id in account_ids:
            for _ in range(random.randint(10, 25)):
                amount = random.uniform(50000, per_transaction_limit_float * 0.5)
                if random.random() < 0.1: # 10% cơ hội giao dịch giá trị cao
                    amount = random.uniform(per_transaction_limit_float * 0.5, per_transaction_limit_float)

                status = random.choices(['completed', 'pending', 'failed'], weights=[0.9, 0.05, 0.05])[0]
                regulation_category = 'B' if amount <= 10000000 else 'C'
                
                is_external_transfer = random.random() < 0.2 
                destination_account = None if is_external_transfer else random.choice(all_active_account_ids)

                transactions_data.append((
                    account_id, destination_account,
                    random.choice(verified_devices),
                    random.choices(['P2P_TRANSFER', 'BILL_PAYMENT'], weights=[0.8, 0.2])[0],
                    amount, status, regulation_category,
                    fake.date_time_between(start_date='-30d', end_date='now')
                ))

    insert_query = """
        INSERT INTO Transactions (source_account_id, destination_account_id, device_id, transaction_type, amount, status, regulation_category, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, transactions_data)
    print(f"-> Generated {len(transactions_data)} transactions.")

def generate_auth_logs(cur):
    print("Generating authentication logs...")
    cur.execute("""
        SELECT t.transaction_id, a.customer_id, t.device_id, t.status, t.regulation_category, t.created_at
        FROM Transactions t
        JOIN Accounts a ON t.source_account_id = a.account_id;
    """)
    transactions_info = cur.fetchall()
    
    auth_logs_data = []
    for txn_id, customer_id, device_id, status, reg_cat, created_at in transactions_info:
        auth_method = 'biometric' if reg_cat in ('C', 'D') else random.choice(['pin', 'otp'])
        result = 'success' if status == 'completed' else 'failure'
        auth_logs_data.append((
            customer_id, device_id, txn_id,
            auth_method, result,
            created_at + timedelta(seconds=random.randint(1,5))
        ))

    insert_query = """
        INSERT INTO AuthLogs (customer_id, device_id, transaction_id, auth_method, result, created_at)
        VALUES (%s, %s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, auth_logs_data)
    print(f"-> Generated {len(auth_logs_data)} auth logs.")

def generate_daily_limit_trackers(cur):
    print("Generating daily limit trackers based on transaction history...")
    cur.execute("""
        SELECT 
            a.customer_id, 
            t.amount, 
            t.regulation_category, 
            t.created_at::date as tracking_date,
            -- Map transaction_type sang transaction_type_group
            CASE 
                WHEN t.transaction_type = 'P2P_TRANSFER' THEN 'NHOM_I.3'
                WHEN t.transaction_type = 'BILL_PAYMENT' THEN 'NHOM_I.2'
                ELSE 'NHOM_I.1'
            END as transaction_type_group
        FROM Transactions t
        JOIN Accounts a ON t.source_account_id = a.account_id
        WHERE t.status = 'completed'
        ORDER BY a.customer_id, t.created_at;
    """)
    transactions = cur.fetchall()

    trackers = {} # key: (customer_id, date, group), value: {'T': amount, 'Tksth': amount}
    for customer_id, amount, reg_cat, date, group in transactions:
        key = (customer_id, date, group)
        if key not in trackers:
            trackers[key] = {'T': 0, 'Tksth': 0}
        
        trackers[key]['T'] += amount
        trackers[key]['Tksth'] += amount

        if reg_cat in ('C', 'D'):
            trackers[key]['Tksth'] = 0
    
    trackers_data = []
    for (customer_id, date, group), values in trackers.items():
        trackers_data.append((
            customer_id, group, values['T'], values['Tksth'], date
        ))

    insert_query = """
        INSERT INTO DailyLimitTrackers (customer_id, transaction_type_group, total_daily_amount, running_total_amount, tracking_date)
        VALUES (%s, %s, %s, %s, %s);
    """
    cur.executemany(insert_query, trackers_data)
    print(f"-> Generated {len(trackers_data)} daily limit tracker records.")


def main():
    """Hàm chính để chạy toàn bộ quá trình sinh dữ liệu."""
    conn = None
    try:
        conn = get_db_connection()
        clear_all_tables(conn)

        with conn.cursor() as cur:
            # Sinh các thực thể chính
            customers_info = generate_customers(cur, NUM_CUSTOMERS)
            customer_ids = [info[0] for info in customers_info]
            device_ids = generate_devices(cur, NUM_DEVICES)
            conn.commit()

            # Sinh các bảng phụ thuộc
            generate_identity_documents(cur, customers_info)
            generate_biometric_data(cur, customers_info)
            generate_transaction_limits(cur, customer_ids)
            generate_customer_device_links(cur, customer_ids, device_ids)
            # Sửa đổi: Truyền customers_info để lọc khách hàng active
            customer_accounts_map = generate_accounts(cur, customers_info)
            conn.commit() 
            
            # Đọc lại hạn mức để dùng cho việc tạo giao dịch
            cur.execute("SELECT customer_id, limit_type, limit_amount FROM TransactionLimits;")
            limits = {}
            for cid, ltype, lamount in cur.fetchall():
                if cid not in limits:
                    limits[cid] = {}
                limits[cid][ltype] = lamount

            # Sinh giao dịch
            generate_transactions(cur, customer_accounts_map, limits)
            conn.commit()
            
            # Sinh dữ liệu dựa trên lịch sử giao dịch
            generate_auth_logs(cur)
            generate_daily_limit_trackers(cur)
            
            conn.commit()
            print("\n🎉 Sample data generated successfully!")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        print(f"\n❌ Database error: {e}")
    finally:
        if conn: conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
