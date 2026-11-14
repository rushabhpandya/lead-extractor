-- supabase_schema.sql — Global B2B Lead Extractor
-- Run against your Supabase project via the SQL Editor or CLI.

-- ──────────────────────────────────────────────────────────────────────────────
-- Enable UUID generation
-- ──────────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ──────────────────────────────────────────────────────────────────────────────
-- Workspaces (multi-tenant)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workspaces (
  id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name        TEXT NOT NULL,
  owner_id    UUID REFERENCES auth.users(id) ON DELETE CASCADE,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE workspaces ENABLE ROW LEVEL SECURITY;

CREATE POLICY "workspace_owner_access" ON workspaces
  USING (owner_id = auth.uid());

-- ──────────────────────────────────────────────────────────────────────────────
-- Leads
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS leads (
  id                TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  workspace_id      TEXT NOT NULL DEFAULT 'default_agency',

  -- Identity
  business_name     TEXT,
  category          TEXT,
  sub_locality      TEXT,
  city              TEXT,
  country_code      CHAR(2) DEFAULT 'IN',
  address           TEXT,

  -- Phone
  phone             TEXT,
  phone_e164        TEXT UNIQUE,
  is_whatsapp_ready BOOLEAN DEFAULT FALSE,

  -- Email
  email             TEXT,
  emails_all        JSONB DEFAULT '[]'::JSONB,

  -- Social
  instagram_handle  TEXT,
  instagram_url     TEXT,

  -- Web
  website           TEXT,
  rating            FLOAT,

  -- Meta
  source            TEXT DEFAULT 'cache',
  created_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_leads_workspace_category_city
  ON leads(workspace_id, category, city);

CREATE INDEX IF NOT EXISTS idx_leads_phone_e164
  ON leads(phone_e164)
  WHERE phone_e164 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_leads_instagram
  ON leads(instagram_handle)
  WHERE instagram_handle IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_leads_country
  ON leads(country_code, city, category);

-- RLS
ALTER TABLE leads ENABLE ROW LEVEL SECURITY;

-- Allow read for workspace members (or public for MVP)
CREATE POLICY "leads_workspace_read" ON leads
  FOR SELECT
  USING (workspace_id = 'default_agency' OR TRUE);

CREATE POLICY "leads_workspace_insert" ON leads
  FOR INSERT
  WITH CHECK (TRUE);

-- ──────────────────────────────────────────────────────────────────────────────
-- Extraction Sessions (audit log)
-- ──────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extraction_sessions (
  id            TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  workspace_id  TEXT DEFAULT 'default_agency',
  category      TEXT,
  location      TEXT,
  country_code  CHAR(2),
  volume        INT,
  delivered     INT,
  elapsed_s     FLOAT,
  source_mix    JSONB,       -- {"cache": N, "osm": N, "ddgs": N, "live": N}
  started_at    TIMESTAMPTZ DEFAULT NOW(),
  finished_at   TIMESTAMPTZ
);
