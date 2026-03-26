-- Echo Vendor Manager v1.0.0 — AI-Powered Vendor & Procurement Management
-- D1 Schema

CREATE TABLE IF NOT EXISTS organizations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  approval_threshold REAL DEFAULT 5000,
  payment_terms_default INTEGER DEFAULT 30,
  settings JSON DEFAULT '{}',
  status TEXT DEFAULT 'active',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  role TEXT DEFAULT 'viewer',
  approval_limit REAL DEFAULT 0,
  status TEXT DEFAULT 'active',
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(org_id, email)
);

CREATE TABLE IF NOT EXISTS vendors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  legal_name TEXT,
  tax_id TEXT,
  category TEXT,
  industry TEXT,
  website TEXT,
  primary_contact TEXT,
  primary_email TEXT,
  primary_phone TEXT,
  address TEXT,
  city TEXT,
  state TEXT,
  country TEXT DEFAULT 'US',
  zip TEXT,
  payment_terms INTEGER DEFAULT 30,
  payment_method TEXT DEFAULT 'ach',
  bank_name TEXT,
  bank_account TEXT,
  bank_routing TEXT,
  risk_level TEXT DEFAULT 'low',
  risk_score REAL DEFAULT 50,
  performance_score REAL DEFAULT 50,
  total_spend REAL DEFAULT 0,
  total_orders INTEGER DEFAULT 0,
  onboarding_complete INTEGER DEFAULT 0,
  diversity_cert TEXT,
  minority_owned INTEGER DEFAULT 0,
  woman_owned INTEGER DEFAULT 0,
  veteran_owned INTEGER DEFAULT 0,
  small_business INTEGER DEFAULT 0,
  tags JSON DEFAULT '[]',
  custom_fields JSON DEFAULT '{}',
  notes TEXT,
  status TEXT DEFAULT 'active',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_vendors_org ON vendors(org_id);
CREATE INDEX IF NOT EXISTS idx_vendors_category ON vendors(org_id, category);
CREATE INDEX IF NOT EXISTS idx_vendors_risk ON vendors(org_id, risk_level);

CREATE TABLE IF NOT EXISTS vendor_contacts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  title TEXT,
  email TEXT,
  phone TEXT,
  is_primary INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_contacts_vendor ON vendor_contacts(vendor_id);

CREATE TABLE IF NOT EXISTS contracts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_id INTEGER NOT NULL,
  org_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  contract_number TEXT,
  type TEXT DEFAULT 'service',
  value REAL DEFAULT 0,
  annual_value REAL DEFAULT 0,
  start_date TEXT,
  end_date TEXT,
  auto_renew INTEGER DEFAULT 0,
  renewal_notice_days INTEGER DEFAULT 60,
  payment_terms INTEGER DEFAULT 30,
  sla_terms TEXT,
  owner_id INTEGER,
  file_key TEXT,
  status TEXT DEFAULT 'active',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_contracts_vendor ON contracts(vendor_id);
CREATE INDEX IF NOT EXISTS idx_contracts_org ON contracts(org_id);

CREATE TABLE IF NOT EXISTS purchase_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id INTEGER NOT NULL,
  vendor_id INTEGER NOT NULL,
  po_number TEXT NOT NULL,
  requester_id INTEGER,
  approver_id INTEGER,
  items JSON DEFAULT '[]',
  subtotal REAL DEFAULT 0,
  tax REAL DEFAULT 0,
  shipping REAL DEFAULT 0,
  total REAL DEFAULT 0,
  delivery_date TEXT,
  notes TEXT,
  approval_status TEXT DEFAULT 'pending',
  approved_at TEXT,
  status TEXT DEFAULT 'draft',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_po_org ON purchase_orders(org_id);
CREATE INDEX IF NOT EXISTS idx_po_vendor ON purchase_orders(vendor_id);

CREATE TABLE IF NOT EXISTS performance_reviews (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_id INTEGER NOT NULL,
  org_id INTEGER NOT NULL,
  reviewer_id INTEGER,
  period_start TEXT,
  period_end TEXT,
  quality_score REAL DEFAULT 0,
  delivery_score REAL DEFAULT 0,
  communication_score REAL DEFAULT 0,
  pricing_score REAL DEFAULT 0,
  overall_score REAL DEFAULT 0,
  strengths TEXT,
  weaknesses TEXT,
  recommendation TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_reviews_vendor ON performance_reviews(vendor_id);

CREATE TABLE IF NOT EXISTS compliance_docs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_id INTEGER NOT NULL,
  doc_type TEXT NOT NULL,
  doc_name TEXT,
  issued_date TEXT,
  expiry_date TEXT,
  verified INTEGER DEFAULT 0,
  verified_by INTEGER,
  file_key TEXT,
  status TEXT DEFAULT 'active',
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_compliance_vendor ON compliance_docs(vendor_id);

CREATE TABLE IF NOT EXISTS spend_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id INTEGER NOT NULL,
  vendor_id INTEGER NOT NULL,
  amount REAL NOT NULL,
  category TEXT,
  description TEXT,
  invoice_number TEXT,
  invoice_date TEXT,
  paid_date TEXT,
  po_id INTEGER,
  status TEXT DEFAULT 'recorded',
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_spend_org ON spend_records(org_id);
CREATE INDEX IF NOT EXISTS idx_spend_vendor ON spend_records(vendor_id);

CREATE TABLE IF NOT EXISTS risk_assessments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  vendor_id INTEGER NOT NULL,
  assessed_by INTEGER,
  financial_risk REAL DEFAULT 0,
  operational_risk REAL DEFAULT 0,
  compliance_risk REAL DEFAULT 0,
  reputational_risk REAL DEFAULT 0,
  overall_risk REAL DEFAULT 0,
  notes TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_risk_vendor ON risk_assessments(vendor_id);

CREATE TABLE IF NOT EXISTS spend_daily (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id INTEGER NOT NULL,
  date TEXT NOT NULL,
  total_vendors INTEGER DEFAULT 0,
  total_spend REAL DEFAULT 0,
  new_vendors INTEGER DEFAULT 0,
  at_risk_vendors INTEGER DEFAULT 0,
  expiring_contracts INTEGER DEFAULT 0,
  pending_approvals INTEGER DEFAULT 0,
  UNIQUE(org_id, date)
);

CREATE TABLE IF NOT EXISTS activity_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  org_id INTEGER,
  actor TEXT,
  action TEXT NOT NULL,
  target TEXT,
  details TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
