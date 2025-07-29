-- Định nghĩa các kiểu dữ liệu ENUM 
CREATE TYPE enum_status_customer AS ENUM ('active', 'inactive', 'suspended');
CREATE TYPE enum_status_account AS ENUM ('active', 'inactive', 'closed', 'frozen');
CREATE TYPE enum_document_type AS ENUM ('CCCD', 'Passport');
CREATE TYPE enum_biometric_type AS ENUM ('face', 'voice');
CREATE TYPE enum_limit_type AS ENUM ('PER_TRANSACTION', 'DAILY_TOTAL');
CREATE TYPE enum_device_type AS ENUM ('mobile', 'desktop', 'tablet', 'unknown');
CREATE TYPE enum_device_status AS ENUM ('active', 'blocked');
CREATE TYPE enum_trust_status AS ENUM ('verified', 'unverified');
CREATE TYPE enum_transaction_status AS ENUM ('pending', 'completed', 'failed', 'flagged');
CREATE TYPE enum_regulation_category AS ENUM ('A', 'B', 'C', 'D');
CREATE TYPE enum_auth_method AS ENUM ('password', 'pin', 'otp', 'biometric');
CREATE TYPE enum_auth_result AS ENUM ('success', 'failure');
CREATE TYPE enum_gender AS ENUM ('male', 'female', 'other');


-- Bảng 1: Customers
CREATE TABLE Customers (
    customer_id BIGSERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender enum_gender NOT NULL DEFAULT 'other',
    address TEXT,
    phone_number VARCHAR(20) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    status enum_status_customer NOT NULL DEFAULT 'active',
    password_hash VARCHAR(255) NOT NULL,
    pin_hash VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Bảng 2: CustomerIdentityDocuments
CREATE TABLE CustomerIdentityDocuments (
    document_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES Customers(customer_id),
    document_number VARCHAR(50) NOT NULL UNIQUE,
    document_type enum_document_type NOT NULL,
    nationality VARCHAR(100) NOT NULL,
    issue_date DATE,
    expiry_date DATE,
    issue_place VARCHAR(255)
);

-- Bảng 3: BiometricData
CREATE TABLE BiometricData (
    biometric_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL UNIQUE REFERENCES Customers(customer_id),
    biometric_type enum_biometric_type NOT NULL,
    template_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Bảng 4: Accounts
CREATE TABLE Accounts (
    account_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES Customers(customer_id),
    account_number VARCHAR(20) NOT NULL UNIQUE,
    account_type VARCHAR(50) NOT NULL,
    balance DECIMAL(18, 2) NOT NULL CHECK (balance >= 0),
    currency CHAR(3) NOT NULL DEFAULT 'VND',
    status enum_status_account NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Các trường thẻ được gộp vào theo thiết kế đơn giản hóa
    card_number_masked VARCHAR(20),
    card_expiry_date DATE,
    card_status enum_status_account -- Dùng chung ENUM với tài khoản
);

-- Bảng 5: Devices
CREATE TABLE Devices (
    device_id BIGSERIAL PRIMARY KEY,
    device_identifier VARCHAR(255) NOT NULL UNIQUE, -- Dấu vân tay thiết bị
    device_name VARCHAR(100), -- Tên do người dùng đặt, ví dụ "iPhone của tôi"
    device_type enum_device_type NOT NULL,
    device_os VARCHAR(100), -- Rất quan trọng cho việc phân tích rủi ro
    last_login_at TIMESTAMPTZ,
    status enum_device_status NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Bảng 6: CustomerDeviceLinks (tên trong ERD: CustomerDevice)
CREATE TABLE CustomerDeviceLinks (
    customer_id BIGINT NOT NULL REFERENCES Customers(customer_id),
    device_id BIGINT NOT NULL REFERENCES Devices(device_id),
    trust_status enum_trust_status NOT NULL DEFAULT 'unverified',
    PRIMARY KEY (customer_id, device_id) -- Khóa chính phức hợp
);

-- Bảng 7: TransactionLimits (phiên bản đã đơn giản hóa)
CREATE TABLE TransactionLimits (
    limit_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES Customers(customer_id),
    limit_type enum_limit_type NOT NULL,
    limit_amount DECIMAL(18, 2) NOT NULL CHECK (limit_amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'VND',
    UNIQUE (customer_id, limit_type)
);

-- Bảng 8: DailyLimitTrackers
CREATE TABLE DailyLimitTrackers (
    tracker_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES Customers(customer_id),
    transaction_type_group VARCHAR(50) NOT NULL, -- Ví dụ: 'P2P_TRANSFER', 'BILL_PAYMENT'
    total_daily_amount DECIMAL(18, 2) NOT NULL DEFAULT 0.00, -- tương ứng với 'T'
    running_total_amount DECIMAL(18, 2) NOT NULL DEFAULT 0.00, -- tương ứng với 'Tksth'
    tracking_date DATE NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (customer_id, transaction_type_group, tracking_date)
);

-- Bảng 9: Transactions
CREATE TABLE Transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    source_account_id BIGINT NOT NULL REFERENCES Accounts(account_id),
    destination_account_id BIGINT REFERENCES Accounts(account_id), -- NULL nếu chuyển ra ngoài hệ thống
    device_id BIGINT NOT NULL REFERENCES Devices(device_id), -- Thiết bị khởi tạo
    transaction_type VARCHAR(50) NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,
    status enum_transaction_status NOT NULL,
    regulation_category enum_regulation_category, -- A, B, C, D
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Bảng 10: AuthLogs
CREATE TABLE AuthLogs (
    log_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES Customers(customer_id),
    device_id BIGINT NOT NULL REFERENCES Devices(device_id), -- Thiết bị thực hiện xác thực
    transaction_id BIGINT REFERENCES Transactions(transaction_id), -- NULL nếu là log đăng nhập
    auth_method enum_auth_method NOT NULL,
    result enum_auth_result NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Bảng 11: RiskTags
CREATE TABLE RiskTags (
    risk_tag_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES Customers(customer_id),
    transaction_id BIGINT REFERENCES Transactions(transaction_id),
    tag_type VARCHAR(100) NOT NULL, -- Tên thẻ rủi ro, ví dụ: 'HIGH_VALUE_TXN'
    description TEXT, -- Mô tả chi tiết về rủi ro
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- Tạo các chỉ mục
CREATE INDEX ON Accounts (customer_id);
CREATE INDEX ON CustomerIdentityDocuments (customer_id);
CREATE INDEX ON BiometricData (customer_id);
CREATE INDEX ON CustomerDeviceLinks (device_id);
CREATE INDEX ON TransactionLimits (customer_id);
CREATE INDEX ON DailyLimitTrackers (customer_id, tracking_date);
CREATE INDEX ON Transactions (source_account_id);
CREATE INDEX ON Transactions (destination_account_id);
CREATE INDEX ON Transactions (created_at);
CREATE INDEX ON AuthLogs (customer_id);
CREATE INDEX ON AuthLogs (device_id);
CREATE INDEX ON AuthLogs (transaction_id);
CREATE INDEX ON RiskTags (customer_id);
CREATE INDEX ON RiskTags (transaction_id);
