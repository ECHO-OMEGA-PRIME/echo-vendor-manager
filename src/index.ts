/**
 * Echo Vendor Manager v2.0.0 — AI-Powered Vendor & Procurement Management
 * Cloudflare Worker — D1 + KV + Service Bindings
 * Features: Vendor CRUD, PO lifecycle w/ receiving, budgets, risk assessment,
 *   compliance tracking, performance reviews, spend analytics, AI analysis,
 *   email notifications, vendor onboarding workflow, CSV/JSON export
 */

interface Env { DB: D1Database; VM_CACHE: KVNamespace; ENGINE_RUNTIME: Fetcher; SHARED_BRAIN: Fetcher; EMAIL_SENDER: Fetcher; ECHO_API_KEY: string; ANALYTICS?: AnalyticsEngineDataset; }
interface RLState { c: number; t: number }

function sanitize(s: unknown, max = 2000): string { return typeof s === 'string' ? s.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f]/g, '').slice(0, max) : ''; }
const uid = () => crypto.randomUUID();

async function rateLimit(kv: KVNamespace, key: string, limit: number, windowSec: number): Promise<boolean> {
  const k = `rl:${key}`; const raw = await kv.get(k); const now = Date.now();
  let st: RLState = raw ? JSON.parse(raw) : { c: 0, t: now };
  st.c = Math.max(0, st.c - ((now - st.t) / 1000) * (limit / windowSec)); st.t = now;
  if (st.c >= limit) return false; st.c += 1;
  await kv.put(k, JSON.stringify(st), { expirationTtl: windowSec * 2 }); return true;
}

function authOk(req: Request, env: Env): boolean {
  return (req.headers.get('X-Echo-API-Key') || req.headers.get('Authorization')?.replace('Bearer ', '') || '') === env.ECHO_API_KEY;
}

function slog(level: string, msg: string, data?: Record<string, any>) {
  console.log(JSON.stringify({ level, service: 'echo-vendor-manager', version: '2.0.0', msg, ...data, ts: new Date().toISOString() }));
}

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
  }});
}
function err(msg: string, status = 400) { slog('warn', msg, { status }); return json({ error: msg }, status); }

function getOrgId(url: URL): string { return url.searchParams.get('org_id') || ''; }
function getPage(url: URL): { limit: number; offset: number } {
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '50'), 200);
  const offset = parseInt(url.searchParams.get('offset') || '0');
  return { limit, offset };
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url); const p = url.pathname; const m = req.method;

    // CORS preflight
    if (m === 'OPTIONS') return new Response(null, { headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type,X-Echo-API-Key,Authorization' } });

    // Root + Health
    if (p === '/') return json({ service: 'echo-vendor-manager', version: '2.0.0', status: 'operational', features: ['purchase-orders', 'budgets', 'risk-assessment', 'compliance', 'performance-reviews', 'spend-analytics', 'ai-analysis', 'notifications', 'csv-export'] });
    if (p === '/health') return json({ ok: true, service: 'echo-vendor-manager', version: '2.0.0', timestamp: new Date().toISOString() });

    // Rate limiting
    const clientIp = req.headers.get('CF-Connecting-IP') || 'unknown';
    const isWrite = ['POST', 'PUT', 'PATCH', 'DELETE'].includes(m);
    if (!await rateLimit(env.VM_CACHE, `${clientIp}:${isWrite ? 'w' : 'r'}`, isWrite ? 30 : 120, 60)) return err('Rate limited', 429);

    // Auth for write operations
    if (isWrite && !authOk(req, env)) return err('Unauthorized', 401);

    try {
      // ═══════════════ ORGANIZATIONS ═══════════════
      if (p === '/orgs' && m === 'GET') {
        return json((await env.DB.prepare("SELECT * FROM organizations WHERE status='active' ORDER BY name").all()).results);
      }
      if (p === '/orgs' && m === 'POST') {
        const b: any = await req.json();
        const r = await env.DB.prepare('INSERT INTO organizations (name,slug,approval_threshold,payment_terms_default,settings) VALUES (?,?,?,?,?)').bind(sanitize(b.name), sanitize(b.slug || (b.name || '').toLowerCase().replace(/\s+/g, '-')), b.approval_threshold || 5000, b.payment_terms_default || 30, JSON.stringify(b.settings || {})).run();
        slog('info', 'Organization created', { id: r.meta.last_row_id });
        return json({ id: r.meta.last_row_id }, 201);
      }
      const orgMatch = p.match(/^\/orgs\/(\d+)$/);
      if (orgMatch && m === 'GET') {
        const org = await env.DB.prepare('SELECT * FROM organizations WHERE id=?').bind(orgMatch[1]).first();
        return org ? json(org) : err('Not found', 404);
      }
      if (orgMatch && m === 'PUT') {
        const b: any = await req.json();
        const fields: string[] = []; const vals: any[] = [];
        for (const [k, v] of Object.entries(b)) {
          if (['name', 'slug', 'approval_threshold', 'payment_terms_default', 'settings', 'status'].includes(k)) {
            fields.push(`${k}=?`); vals.push(typeof v === 'object' ? JSON.stringify(v) : typeof v === 'string' ? sanitize(v) : v);
          }
        }
        if (fields.length) { fields.push("updated_at=datetime('now')"); vals.push(orgMatch[1]); await env.DB.prepare(`UPDATE organizations SET ${fields.join(',')} WHERE id=?`).bind(...vals).run(); }
        return json({ updated: true });
      }

      // ═══════════════ USERS ═══════════════
      if (p === '/users' && m === 'GET') {
        return json((await env.DB.prepare("SELECT * FROM users WHERE org_id=? AND status='active' ORDER BY name").bind(getOrgId(url)).all()).results);
      }
      if (p === '/users' && m === 'POST') {
        const b: any = await req.json();
        const r = await env.DB.prepare('INSERT INTO users (org_id,name,email,role,approval_limit,department) VALUES (?,?,?,?,?,?)').bind(b.org_id, sanitize(b.name), sanitize(b.email), b.role || 'viewer', b.approval_limit || 0, sanitize(b.department || '')).run();
        return json({ id: r.meta.last_row_id }, 201);
      }
      const userMatch = p.match(/^\/users\/(\d+)$/);
      if (userMatch && m === 'GET') {
        const u = await env.DB.prepare('SELECT * FROM users WHERE id=?').bind(userMatch[1]).first();
        return u ? json(u) : err('Not found', 404);
      }
      if (userMatch && m === 'PUT') {
        const b: any = await req.json();
        const fields: string[] = []; const vals: any[] = [];
        for (const [k, v] of Object.entries(b)) {
          if (['name', 'email', 'role', 'approval_limit', 'department', 'status'].includes(k)) {
            fields.push(`${k}=?`); vals.push(typeof v === 'string' ? sanitize(v) : v);
          }
        }
        if (fields.length) { vals.push(userMatch[1]); await env.DB.prepare(`UPDATE users SET ${fields.join(',')} WHERE id=?`).bind(...vals).run(); }
        return json({ updated: true });
      }

      // ═══════════════ VENDORS ═══════════════
      if (p === '/vendors' && m === 'GET') {
        const orgId = getOrgId(url); const cat = url.searchParams.get('category'); const risk = url.searchParams.get('risk_level');
        const search = url.searchParams.get('q'); const status = url.searchParams.get('status') || 'active';
        const { limit, offset } = getPage(url);
        let q = 'SELECT * FROM vendors WHERE org_id=?'; const binds: any[] = [orgId];
        if (status !== 'all') { q += ' AND status=?'; binds.push(status); }
        if (cat) { q += ' AND category=?'; binds.push(cat); }
        if (risk) { q += ' AND risk_level=?'; binds.push(risk); }
        if (search) { q += ' AND (name LIKE ? OR legal_name LIKE ? OR primary_email LIKE ? OR category LIKE ?)'; const s = `%${sanitize(search, 100)}%`; binds.push(s, s, s, s); }
        q += ' ORDER BY name LIMIT ? OFFSET ?'; binds.push(limit, offset);
        const results = (await env.DB.prepare(q).bind(...binds).all()).results;
        const countQ = q.replace(/SELECT \*/, 'SELECT COUNT(*) as total').replace(/ ORDER BY.*$/, '');
        const total = await env.DB.prepare(countQ).bind(...binds.slice(0, -2)).first() as any;
        return json({ results, total: total?.total || results.length, limit, offset });
      }
      if (p === '/vendors' && m === 'POST') {
        const b: any = await req.json();
        const r = await env.DB.prepare('INSERT INTO vendors (org_id,name,legal_name,tax_id,category,industry,website,primary_contact,primary_email,primary_phone,address,city,state,country,zip,payment_terms,payment_method,risk_level,diversity_cert,minority_owned,woman_owned,veteran_owned,small_business,tags,custom_fields,notes,onboarding_status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(b.org_id, sanitize(b.name), sanitize(b.legal_name || ''), sanitize(b.tax_id || ''), sanitize(b.category || ''), sanitize(b.industry || ''), sanitize(b.website || ''), sanitize(b.primary_contact || ''), sanitize(b.primary_email || ''), sanitize(b.primary_phone || ''), sanitize(b.address || ''), sanitize(b.city || ''), sanitize(b.state || ''), b.country || 'US', sanitize(b.zip || ''), b.payment_terms || 30, b.payment_method || 'ach', b.risk_level || 'low', sanitize(b.diversity_cert || ''), b.minority_owned || 0, b.woman_owned || 0, b.veteran_owned || 0, b.small_business || 0, JSON.stringify(b.tags || []), JSON.stringify(b.custom_fields || {}), sanitize(b.notes || ''), 'pending').run();
        slog('info', 'Vendor created', { id: r.meta.last_row_id, name: b.name });
        return json({ id: r.meta.last_row_id }, 201);
      }
      const vendorMatch = p.match(/^\/vendors\/(\d+)$/);
      if (vendorMatch && m === 'GET') {
        const v = await env.DB.prepare('SELECT * FROM vendors WHERE id=?').bind(vendorMatch[1]).first();
        if (!v) return err('Not found', 404);
        const [contacts, contracts, reviews, compliance, spend, recentPOs] = await Promise.all([
          env.DB.prepare('SELECT * FROM vendor_contacts WHERE vendor_id=? ORDER BY is_primary DESC').bind(vendorMatch[1]).all(),
          env.DB.prepare("SELECT * FROM contracts WHERE vendor_id=? AND status='active' ORDER BY end_date").bind(vendorMatch[1]).all(),
          env.DB.prepare('SELECT * FROM performance_reviews WHERE vendor_id=? ORDER BY created_at DESC LIMIT 10').bind(vendorMatch[1]).all(),
          env.DB.prepare("SELECT * FROM compliance_docs WHERE vendor_id=? AND status='active' ORDER BY expiry_date").bind(vendorMatch[1]).all(),
          env.DB.prepare('SELECT SUM(amount) as total, COUNT(*) as count FROM spend_records WHERE vendor_id=?').bind(vendorMatch[1]).first() as any,
          env.DB.prepare('SELECT * FROM purchase_orders WHERE vendor_id=? ORDER BY created_at DESC LIMIT 10').bind(vendorMatch[1]).all(),
        ]);
        return json({ ...v, contacts: contacts.results, contracts: contracts.results, reviews: reviews.results, compliance: compliance.results, spend_total: spend?.total || 0, spend_count: spend?.count || 0, recent_purchase_orders: recentPOs.results });
      }
      if (vendorMatch && m === 'PUT') {
        const b: any = await req.json(); const fields: string[] = []; const vals: any[] = [];
        for (const [k, val] of Object.entries(b)) {
          if (['name', 'legal_name', 'tax_id', 'category', 'industry', 'website', 'primary_contact', 'primary_email', 'primary_phone', 'address', 'city', 'state', 'country', 'zip', 'payment_terms', 'payment_method', 'risk_level', 'status', 'notes', 'tags', 'custom_fields', 'onboarding_status'].includes(k)) {
            fields.push(`${k}=?`); vals.push(typeof val === 'string' ? sanitize(val) : typeof val === 'object' ? JSON.stringify(val) : val);
          }
        }
        if (fields.length) { fields.push("updated_at=datetime('now')"); vals.push(vendorMatch[1]); await env.DB.prepare(`UPDATE vendors SET ${fields.join(',')} WHERE id=?`).bind(...vals).run(); }
        return json({ updated: true });
      }
      if (vendorMatch && m === 'DELETE') {
        await env.DB.prepare("UPDATE vendors SET status='inactive',updated_at=datetime('now') WHERE id=?").bind(vendorMatch[1]).run();
        return json({ deactivated: true });
      }

      // Vendor onboarding workflow
      const onboardMatch = p.match(/^\/vendors\/(\d+)\/(approve|reject|activate|suspend)$/);
      if (onboardMatch && m === 'POST') {
        const [, vid, action] = onboardMatch;
        const statusMap: Record<string, string> = { approve: 'approved', reject: 'rejected', activate: 'active', suspend: 'suspended' };
        const newStatus = statusMap[action];
        const field = action === 'activate' ? 'status' : 'onboarding_status';
        await env.DB.prepare(`UPDATE vendors SET ${field}=?,updated_at=datetime('now') WHERE id=?`).bind(newStatus, vid).run();
        if (action === 'activate') await env.DB.prepare("UPDATE vendors SET onboarding_status='completed',updated_at=datetime('now') WHERE id=?").bind(vid).run();
        slog('info', `Vendor ${action}d`, { vendor_id: vid });
        return json({ [action + 'd']: true, status: newStatus });
      }

      // ═══════════════ VENDOR CONTACTS ═══════════════
      if (p === '/contacts' && m === 'GET') {
        const vendorId = url.searchParams.get('vendor_id');
        return json((await env.DB.prepare('SELECT * FROM vendor_contacts WHERE vendor_id=? ORDER BY is_primary DESC, name').bind(vendorId).all()).results);
      }
      if (p === '/contacts' && m === 'POST') {
        const b: any = await req.json();
        const r = await env.DB.prepare('INSERT INTO vendor_contacts (vendor_id,name,title,email,phone,is_primary,department) VALUES (?,?,?,?,?,?,?)').bind(b.vendor_id, sanitize(b.name), sanitize(b.title || ''), sanitize(b.email || ''), sanitize(b.phone || ''), b.is_primary || 0, sanitize(b.department || '')).run();
        return json({ id: r.meta.last_row_id }, 201);
      }
      const contactMatch = p.match(/^\/contacts\/(\d+)$/);
      if (contactMatch && m === 'PUT') {
        const b: any = await req.json(); const fields: string[] = []; const vals: any[] = [];
        for (const [k, v] of Object.entries(b)) {
          if (['name', 'title', 'email', 'phone', 'is_primary', 'department'].includes(k)) {
            fields.push(`${k}=?`); vals.push(typeof v === 'string' ? sanitize(v) : v);
          }
        }
        if (fields.length) { vals.push(contactMatch[1]); await env.DB.prepare(`UPDATE vendor_contacts SET ${fields.join(',')} WHERE id=?`).bind(...vals).run(); }
        return json({ updated: true });
      }
      if (contactMatch && m === 'DELETE') {
        await env.DB.prepare('DELETE FROM vendor_contacts WHERE id=?').bind(contactMatch[1]).run();
        return json({ deleted: true });
      }

      // ═══════════════ CONTRACTS ═══════════════
      if (p === '/contracts' && m === 'GET') {
        const orgId = getOrgId(url); const status = url.searchParams.get('status');
        let q = 'SELECT c.*,v.name as vendor_name FROM contracts c JOIN vendors v ON c.vendor_id=v.id WHERE c.org_id=?';
        const binds: any[] = [orgId];
        if (status) { q += ' AND c.status=?'; binds.push(status); }
        q += ' ORDER BY c.end_date'; return json((await env.DB.prepare(q).bind(...binds).all()).results);
      }
      if (p === '/contracts' && m === 'POST') {
        const b: any = await req.json();
        const num = b.contract_number || `VCT-${Date.now().toString(36).toUpperCase()}`;
        const r = await env.DB.prepare('INSERT INTO contracts (vendor_id,org_id,title,contract_number,type,value,annual_value,start_date,end_date,auto_renew,renewal_notice_days,payment_terms,sla_terms,owner_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(b.vendor_id, b.org_id, sanitize(b.title), num, b.type || 'service', b.value || 0, b.annual_value || 0, b.start_date || null, b.end_date || null, b.auto_renew || 0, b.renewal_notice_days || 60, b.payment_terms || 30, sanitize(b.sla_terms || ''), b.owner_id || null).run();
        slog('info', 'Contract created', { id: r.meta.last_row_id, number: num });
        return json({ id: r.meta.last_row_id, contract_number: num }, 201);
      }
      const contractMatch = p.match(/^\/contracts\/(\d+)$/);
      if (contractMatch && m === 'GET') {
        const c = await env.DB.prepare('SELECT c.*,v.name as vendor_name FROM contracts c JOIN vendors v ON c.vendor_id=v.id WHERE c.id=?').bind(contractMatch[1]).first();
        return c ? json(c) : err('Not found', 404);
      }
      if (contractMatch && m === 'PUT') {
        const b: any = await req.json(); const fields: string[] = []; const vals: any[] = [];
        for (const [k, v] of Object.entries(b)) {
          if (['title', 'type', 'value', 'annual_value', 'start_date', 'end_date', 'auto_renew', 'renewal_notice_days', 'payment_terms', 'sla_terms', 'status', 'owner_id'].includes(k)) {
            fields.push(`${k}=?`); vals.push(typeof v === 'string' ? sanitize(v) : v);
          }
        }
        if (fields.length) { fields.push("updated_at=datetime('now')"); vals.push(contractMatch[1]); await env.DB.prepare(`UPDATE contracts SET ${fields.join(',')} WHERE id=?`).bind(...vals).run(); }
        return json({ updated: true });
      }
      // Renew contract
      const renewMatch = p.match(/^\/contracts\/(\d+)\/renew$/);
      if (renewMatch && m === 'POST') {
        const orig = await env.DB.prepare('SELECT * FROM contracts WHERE id=?').bind(renewMatch[1]).first() as any;
        if (!orig) return err('Not found', 404);
        const newStart = orig.end_date || new Date().toISOString().slice(0, 10);
        const endDate = new Date(newStart);
        endDate.setFullYear(endDate.getFullYear() + 1);
        const num = `VCT-${Date.now().toString(36).toUpperCase()}`;
        const r = await env.DB.prepare('INSERT INTO contracts (vendor_id,org_id,title,contract_number,type,value,annual_value,start_date,end_date,auto_renew,renewal_notice_days,payment_terms,sla_terms,owner_id,parent_contract_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(orig.vendor_id, orig.org_id, orig.title, num, orig.type, orig.value, orig.annual_value, newStart, endDate.toISOString().slice(0, 10), orig.auto_renew, orig.renewal_notice_days, orig.payment_terms, orig.sla_terms, orig.owner_id, orig.id).run();
        await env.DB.prepare("UPDATE contracts SET status='expired',updated_at=datetime('now') WHERE id=?").bind(renewMatch[1]).run();
        slog('info', 'Contract renewed', { old_id: renewMatch[1], new_id: r.meta.last_row_id });
        return json({ id: r.meta.last_row_id, contract_number: num }, 201);
      }

      // ═══════════════ PURCHASE ORDERS ═══════════════
      if (p === '/purchase-orders' && m === 'GET') {
        const orgId = getOrgId(url); const status = url.searchParams.get('status');
        const { limit, offset } = getPage(url);
        let q = 'SELECT po.*,v.name as vendor_name FROM purchase_orders po JOIN vendors v ON po.vendor_id=v.id WHERE po.org_id=?';
        const binds: any[] = [orgId];
        if (status) { q += ' AND po.status=?'; binds.push(status); }
        q += ' ORDER BY po.created_at DESC LIMIT ? OFFSET ?'; binds.push(limit, offset);
        return json((await env.DB.prepare(q).bind(...binds).all()).results);
      }
      if (p === '/purchase-orders' && m === 'POST') {
        const b: any = await req.json();
        const poNum = b.po_number || `PO-${Date.now().toString(36).toUpperCase()}`;
        const items = b.items || [];
        const subtotal = items.reduce((s: number, i: any) => s + ((i.quantity || 0) * (i.unit_price || 0)), 0);
        const total = subtotal + (b.tax || 0) + (b.shipping || 0);
        // Check budget if budget_id provided
        if (b.budget_id) {
          const budget = await env.DB.prepare('SELECT * FROM budgets WHERE id=?').bind(b.budget_id).first() as any;
          if (budget && budget.remaining < total) {
            return err(`Exceeds budget: ${budget.name} (remaining: $${budget.remaining.toFixed(2)}, PO total: $${total.toFixed(2)})`, 400);
          }
        }
        // Auto-approve if under org threshold
        const org = await env.DB.prepare('SELECT approval_threshold FROM organizations WHERE id=?').bind(b.org_id).first() as any;
        const autoApprove = total <= (org?.approval_threshold || 5000);
        const r = await env.DB.prepare('INSERT INTO purchase_orders (org_id,vendor_id,po_number,requester_id,items,subtotal,tax,shipping,total,delivery_date,notes,approval_status,status,budget_id,department,urgency) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(b.org_id, b.vendor_id, poNum, b.requester_id || null, JSON.stringify(items), subtotal, b.tax || 0, b.shipping || 0, total, b.delivery_date || null, sanitize(b.notes || ''), autoApprove ? 'approved' : 'pending', autoApprove ? 'approved' : 'pending_approval', b.budget_id || null, sanitize(b.department || ''), b.urgency || 'normal').run();
        // Update vendor stats
        await env.DB.prepare("UPDATE vendors SET total_orders=total_orders+1,updated_at=datetime('now') WHERE id=?").bind(b.vendor_id).run();
        // Deduct from budget if auto-approved
        if (autoApprove && b.budget_id) {
          await env.DB.prepare("UPDATE budgets SET spent=spent+?,remaining=remaining-?,updated_at=datetime('now') WHERE id=?").bind(total, total, b.budget_id).run();
        }
        slog('info', 'PO created', { id: r.meta.last_row_id, po_number: poNum, total, auto_approved: autoApprove });
        return json({ id: r.meta.last_row_id, po_number: poNum, auto_approved: autoApprove, total }, 201);
      }
      const poMatch = p.match(/^\/purchase-orders\/(\d+)$/);
      if (poMatch && m === 'GET') {
        const po = await env.DB.prepare('SELECT po.*,v.name as vendor_name FROM purchase_orders po JOIN vendors v ON po.vendor_id=v.id WHERE po.id=?').bind(poMatch[1]).first();
        if (!po) return err('Not found', 404);
        const receipts = (await env.DB.prepare('SELECT * FROM po_receipts WHERE po_id=? ORDER BY received_at DESC').bind(poMatch[1]).all()).results;
        return json({ ...po, receipts });
      }

      // PO Approval
      const poApprove = p.match(/^\/purchase-orders\/(\d+)\/approve$/);
      if (poApprove && m === 'POST') {
        const b: any = await req.json();
        const po = await env.DB.prepare('SELECT * FROM purchase_orders WHERE id=?').bind(poApprove[1]).first() as any;
        if (!po) return err('Not found', 404);
        if (po.status !== 'pending_approval') return err('PO not pending approval', 400);
        await env.DB.prepare("UPDATE purchase_orders SET approval_status='approved',approver_id=?,approved_at=datetime('now'),status='approved',updated_at=datetime('now') WHERE id=?").bind(b.approver_id || null, poApprove[1]).run();
        // Deduct from budget
        if (po.budget_id) {
          await env.DB.prepare("UPDATE budgets SET spent=spent+?,remaining=remaining-?,updated_at=datetime('now') WHERE id=?").bind(po.total, po.total, po.budget_id).run();
        }
        slog('info', 'PO approved', { po_id: poApprove[1], approver: b.approver_id });
        return json({ approved: true });
      }
      const poReject = p.match(/^\/purchase-orders\/(\d+)\/reject$/);
      if (poReject && m === 'POST') {
        const b: any = await req.json();
        await env.DB.prepare("UPDATE purchase_orders SET approval_status='rejected',approver_id=?,rejection_reason=?,status='rejected',updated_at=datetime('now') WHERE id=?").bind(b.approver_id || null, sanitize(b.reason || ''), poReject[1]).run();
        return json({ rejected: true });
      }

      // PO Receiving — record delivery of items
      const poReceive = p.match(/^\/purchase-orders\/(\d+)\/receive$/);
      if (poReceive && m === 'POST') {
        const b: any = await req.json();
        const po = await env.DB.prepare('SELECT * FROM purchase_orders WHERE id=?').bind(poReceive[1]).first() as any;
        if (!po) return err('Not found', 404);
        if (!['approved', 'partially_received'].includes(po.status)) return err('PO not in receivable state', 400);
        const receivedItems = b.items || [];
        const totalReceived = receivedItems.reduce((s: number, i: any) => s + (i.quantity_received || 0), 0);
        const r = await env.DB.prepare('INSERT INTO po_receipts (po_id,received_by,items_received,quality_notes,delivery_notes) VALUES (?,?,?,?,?)').bind(poReceive[1], sanitize(b.received_by || ''), JSON.stringify(receivedItems), sanitize(b.quality_notes || ''), sanitize(b.delivery_notes || '')).run();
        // Update PO status
        const poItems = JSON.parse(po.items || '[]');
        const totalOrdered = poItems.reduce((s: number, i: any) => s + (i.quantity || 0), 0);
        const allReceipts = (await env.DB.prepare('SELECT items_received FROM po_receipts WHERE po_id=?').bind(poReceive[1]).all()).results as any[];
        let totalReceivedAll = 0;
        for (const receipt of allReceipts) { const items = JSON.parse(receipt.items_received || '[]'); totalReceivedAll += items.reduce((s: number, i: any) => s + (i.quantity_received || 0), 0); }
        const newStatus = totalReceivedAll >= totalOrdered ? 'received' : 'partially_received';
        await env.DB.prepare("UPDATE purchase_orders SET status=?,received_quantity=?,updated_at=datetime('now') WHERE id=?").bind(newStatus, totalReceivedAll, poReceive[1]).run();
        slog('info', 'PO items received', { po_id: poReceive[1], received: totalReceived, total_received: totalReceivedAll, status: newStatus });
        return json({ receipt_id: r.meta.last_row_id, status: newStatus, total_received: totalReceivedAll, total_ordered: totalOrdered });
      }

      // PO Close
      const poClose = p.match(/^\/purchase-orders\/(\d+)\/close$/);
      if (poClose && m === 'POST') {
        await env.DB.prepare("UPDATE purchase_orders SET status='closed',updated_at=datetime('now') WHERE id=?").bind(poClose[1]).run();
        return json({ closed: true });
      }

      // PO Cancel
      const poCancel = p.match(/^\/purchase-orders\/(\d+)\/cancel$/);
      if (poCancel && m === 'POST') {
        const po = await env.DB.prepare('SELECT * FROM purchase_orders WHERE id=?').bind(poCancel[1]).first() as any;
        if (!po) return err('Not found', 404);
        await env.DB.prepare("UPDATE purchase_orders SET status='cancelled',updated_at=datetime('now') WHERE id=?").bind(poCancel[1]).run();
        // Refund budget
        if (po.budget_id && ['approved', 'partially_received'].includes(po.status)) {
          await env.DB.prepare("UPDATE budgets SET spent=spent-?,remaining=remaining+?,updated_at=datetime('now') WHERE id=?").bind(po.total, po.total, po.budget_id).run();
        }
        slog('info', 'PO cancelled', { po_id: poCancel[1] });
        return json({ cancelled: true });
      }

      // ═══════════════ BUDGETS ═══════════════
      if (p === '/budgets' && m === 'GET') {
        const orgId = getOrgId(url);
        return json((await env.DB.prepare("SELECT * FROM budgets WHERE org_id=? AND status='active' ORDER BY name").bind(orgId).all()).results);
      }
      if (p === '/budgets' && m === 'POST') {
        const b: any = await req.json();
        const r = await env.DB.prepare('INSERT INTO budgets (org_id,name,department,category,amount,spent,remaining,period_start,period_end,owner_id) VALUES (?,?,?,?,?,0,?,?,?,?)').bind(b.org_id, sanitize(b.name), sanitize(b.department || ''), sanitize(b.category || ''), b.amount || 0, b.amount || 0, b.period_start || null, b.period_end || null, b.owner_id || null).run();
        slog('info', 'Budget created', { id: r.meta.last_row_id, amount: b.amount });
        return json({ id: r.meta.last_row_id }, 201);
      }
      const budgetMatch = p.match(/^\/budgets\/(\d+)$/);
      if (budgetMatch && m === 'GET') {
        const b = await env.DB.prepare('SELECT * FROM budgets WHERE id=?').bind(budgetMatch[1]).first();
        if (!b) return err('Not found', 404);
        const pos = (await env.DB.prepare("SELECT po_number,total,status,created_at FROM purchase_orders WHERE budget_id=? AND status NOT IN ('rejected','cancelled') ORDER BY created_at DESC LIMIT 20").bind(budgetMatch[1]).all()).results;
        return json({ ...b, purchase_orders: pos });
      }
      if (budgetMatch && m === 'PUT') {
        const b: any = await req.json();
        if (b.amount !== undefined) {
          const existing = await env.DB.prepare('SELECT * FROM budgets WHERE id=?').bind(budgetMatch[1]).first() as any;
          if (existing) {
            const diff = b.amount - existing.amount;
            await env.DB.prepare("UPDATE budgets SET amount=?,remaining=remaining+?,updated_at=datetime('now') WHERE id=?").bind(b.amount, diff, budgetMatch[1]).run();
          }
        }
        return json({ updated: true });
      }

      // Budget utilization report
      if (p === '/budgets/utilization' && m === 'GET') {
        const orgId = getOrgId(url);
        const budgets = (await env.DB.prepare("SELECT id,name,department,category,amount,spent,remaining FROM budgets WHERE org_id=? AND status='active'").bind(orgId).all()).results as any[];
        const report = budgets.map(b => ({
          ...b,
          utilization_pct: b.amount > 0 ? Math.round((b.spent / b.amount) * 100) : 0,
          status: b.remaining <= 0 ? 'exhausted' : b.remaining < b.amount * 0.1 ? 'critical' : b.remaining < b.amount * 0.25 ? 'warning' : 'healthy',
        }));
        const totalBudget = budgets.reduce((s, b) => s + b.amount, 0);
        const totalSpent = budgets.reduce((s, b) => s + b.spent, 0);
        return json({ budgets: report, total_budget: totalBudget, total_spent: totalSpent, overall_utilization_pct: totalBudget > 0 ? Math.round((totalSpent / totalBudget) * 100) : 0 });
      }

      // ═══════════════ PERFORMANCE REVIEWS ═══════════════
      if (p === '/reviews' && m === 'GET') {
        const vendorId = url.searchParams.get('vendor_id');
        return json((await env.DB.prepare('SELECT * FROM performance_reviews WHERE vendor_id=? ORDER BY created_at DESC LIMIT 20').bind(vendorId).all()).results);
      }
      if (p === '/reviews' && m === 'POST') {
        const b: any = await req.json();
        const overall = ((b.quality_score || 0) + (b.delivery_score || 0) + (b.communication_score || 0) + (b.pricing_score || 0)) / 4;
        const r = await env.DB.prepare('INSERT INTO performance_reviews (vendor_id,org_id,reviewer_id,reviewer_name,period_start,period_end,quality_score,delivery_score,communication_score,pricing_score,overall_score,strengths,weaknesses,recommendation) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(b.vendor_id, b.org_id, b.reviewer_id || null, sanitize(b.reviewer_name || ''), b.period_start || null, b.period_end || null, b.quality_score || 0, b.delivery_score || 0, b.communication_score || 0, b.pricing_score || 0, overall, sanitize(b.strengths || ''), sanitize(b.weaknesses || ''), sanitize(b.recommendation || '')).run();
        await env.DB.prepare("UPDATE vendors SET performance_score=?,updated_at=datetime('now') WHERE id=?").bind(overall, b.vendor_id).run();
        slog('info', 'Review submitted', { vendor_id: b.vendor_id, score: overall });
        return json({ id: r.meta.last_row_id, overall_score: overall }, 201);
      }

      // ═══════════════ COMPLIANCE DOCS ═══════════════
      if (p === '/compliance' && m === 'GET') {
        const vendorId = url.searchParams.get('vendor_id');
        if (vendorId) return json((await env.DB.prepare("SELECT * FROM compliance_docs WHERE vendor_id=? AND status='active' ORDER BY expiry_date").bind(vendorId).all()).results);
        // All expiring docs across org
        const orgId = getOrgId(url);
        return json((await env.DB.prepare("SELECT cd.*,v.name as vendor_name FROM compliance_docs cd JOIN vendors v ON cd.vendor_id=v.id WHERE v.org_id=? AND cd.status='active' AND cd.expiry_date <= date('now','+30 days') ORDER BY cd.expiry_date").bind(orgId).all()).results);
      }
      if (p === '/compliance' && m === 'POST') {
        const b: any = await req.json();
        const r = await env.DB.prepare('INSERT INTO compliance_docs (vendor_id,doc_type,doc_name,issued_date,expiry_date,document_url,notes) VALUES (?,?,?,?,?,?,?)').bind(b.vendor_id, sanitize(b.doc_type), sanitize(b.doc_name || ''), b.issued_date || null, b.expiry_date || null, sanitize(b.document_url || ''), sanitize(b.notes || '')).run();
        return json({ id: r.meta.last_row_id }, 201);
      }
      const complianceMatch = p.match(/^\/compliance\/(\d+)$/);
      if (complianceMatch && m === 'PUT') {
        const b: any = await req.json(); const fields: string[] = []; const vals: any[] = [];
        for (const [k, v] of Object.entries(b)) {
          if (['doc_type', 'doc_name', 'issued_date', 'expiry_date', 'status', 'document_url', 'notes'].includes(k)) {
            fields.push(`${k}=?`); vals.push(typeof v === 'string' ? sanitize(v) : v);
          }
        }
        if (fields.length) { vals.push(complianceMatch[1]); await env.DB.prepare(`UPDATE compliance_docs SET ${fields.join(',')} WHERE id=?`).bind(...vals).run(); }
        return json({ updated: true });
      }

      // ═══════════════ SPEND RECORDS ═══════════════
      if (p === '/spend' && m === 'GET') {
        const vendorId = url.searchParams.get('vendor_id'); const orgId = getOrgId(url);
        const { limit, offset } = getPage(url);
        if (vendorId) return json((await env.DB.prepare('SELECT * FROM spend_records WHERE vendor_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?').bind(vendorId, limit, offset).all()).results);
        return json((await env.DB.prepare('SELECT s.*,v.name as vendor_name FROM spend_records s JOIN vendors v ON s.vendor_id=v.id WHERE s.org_id=? ORDER BY s.created_at DESC LIMIT ? OFFSET ?').bind(orgId, limit, offset).all()).results);
      }
      if (p === '/spend' && m === 'POST') {
        const b: any = await req.json();
        const r = await env.DB.prepare('INSERT INTO spend_records (org_id,vendor_id,amount,category,description,invoice_number,invoice_date,po_id) VALUES (?,?,?,?,?,?,?,?)').bind(b.org_id, b.vendor_id, b.amount, sanitize(b.category || ''), sanitize(b.description || ''), sanitize(b.invoice_number || ''), b.invoice_date || null, b.po_id || null).run();
        await env.DB.prepare("UPDATE vendors SET total_spend=total_spend+?,updated_at=datetime('now') WHERE id=?").bind(b.amount, b.vendor_id).run();
        return json({ id: r.meta.last_row_id }, 201);
      }

      // ═══════════════ SPEND ANALYTICS ═══════════════
      if (p === '/spend/analytics' && m === 'GET') {
        const orgId = getOrgId(url); const days = parseInt(url.searchParams.get('days') || '90');
        const [byVendor, byCategory, totalSpend, monthlyTrend] = await Promise.all([
          env.DB.prepare("SELECT v.name,SUM(s.amount) as total,COUNT(*) as count FROM spend_records s JOIN vendors v ON s.vendor_id=v.id WHERE s.org_id=? AND s.created_at >= date('now',?) GROUP BY s.vendor_id ORDER BY total DESC LIMIT 20").bind(orgId, `-${days} days`).all(),
          env.DB.prepare("SELECT category,SUM(amount) as total,COUNT(*) as count FROM spend_records WHERE org_id=? AND created_at >= date('now',?) GROUP BY category ORDER BY total DESC").bind(orgId, `-${days} days`).all(),
          env.DB.prepare("SELECT SUM(amount) as total,COUNT(*) as count,AVG(amount) as avg_amount FROM spend_records WHERE org_id=? AND created_at >= date('now',?)").bind(orgId, `-${days} days`).first(),
          env.DB.prepare("SELECT strftime('%Y-%m',created_at) as month,SUM(amount) as total,COUNT(*) as count FROM spend_records WHERE org_id=? AND created_at >= date('now','-12 months') GROUP BY month ORDER BY month").bind(orgId).all(),
        ]);
        return json({ total_spend: (totalSpend as any)?.total || 0, total_transactions: (totalSpend as any)?.count || 0, avg_amount: (totalSpend as any)?.avg_amount || 0, by_vendor: byVendor.results, by_category: byCategory.results, monthly_trend: monthlyTrend.results });
      }

      // Vendor comparison
      if (p === '/spend/compare' && m === 'GET') {
        const vendorIds = (url.searchParams.get('vendor_ids') || '').split(',').filter(Boolean);
        if (vendorIds.length < 2) return err('Need at least 2 vendor_ids', 400);
        const comparisons = await Promise.all(vendorIds.map(async vid => {
          const vendor = await env.DB.prepare('SELECT id,name,performance_score,risk_level,total_spend,total_orders FROM vendors WHERE id=?').bind(vid).first();
          const spend90d = await env.DB.prepare("SELECT SUM(amount) as total FROM spend_records WHERE vendor_id=? AND created_at >= date('now','-90 days')").bind(vid).first() as any;
          const reviews = await env.DB.prepare('SELECT AVG(overall_score) as avg_score,COUNT(*) as count FROM performance_reviews WHERE vendor_id=?').bind(vid).first() as any;
          return { ...vendor, spend_90d: spend90d?.total || 0, avg_review_score: reviews?.avg_score || 0, review_count: reviews?.count || 0 };
        }));
        return json({ comparisons });
      }

      // ═══════════════ RISK ASSESSMENT ═══════════════
      if (p === '/risk-assessment' && m === 'GET') {
        const vendorId = url.searchParams.get('vendor_id');
        return json((await env.DB.prepare('SELECT * FROM risk_assessments WHERE vendor_id=? ORDER BY created_at DESC LIMIT 10').bind(vendorId).all()).results);
      }
      if (p === '/risk-assessment' && m === 'POST') {
        const b: any = await req.json();
        const overall = ((b.financial_risk || 0) + (b.operational_risk || 0) + (b.compliance_risk || 0) + (b.reputational_risk || 0)) / 4;
        const r = await env.DB.prepare('INSERT INTO risk_assessments (vendor_id,assessed_by,financial_risk,operational_risk,compliance_risk,reputational_risk,overall_risk,notes,mitigation_plan) VALUES (?,?,?,?,?,?,?,?,?)').bind(b.vendor_id, b.assessed_by || null, b.financial_risk || 0, b.operational_risk || 0, b.compliance_risk || 0, b.reputational_risk || 0, overall, sanitize(b.notes || ''), sanitize(b.mitigation_plan || '')).run();
        const riskLevel = overall >= 70 ? 'high' : overall >= 40 ? 'medium' : 'low';
        await env.DB.prepare("UPDATE vendors SET risk_score=?,risk_level=?,updated_at=datetime('now') WHERE id=?").bind(overall, riskLevel, b.vendor_id).run();
        slog('info', 'Risk assessment', { vendor_id: b.vendor_id, overall, risk_level: riskLevel });
        return json({ id: r.meta.last_row_id, overall_risk: overall, risk_level: riskLevel }, 201);
      }

      // Risk matrix overview
      if (p === '/risk-matrix' && m === 'GET') {
        const orgId = getOrgId(url);
        const high = (await env.DB.prepare("SELECT id,name,risk_score,total_spend FROM vendors WHERE org_id=? AND risk_level='high' AND status='active' ORDER BY risk_score DESC").bind(orgId).all()).results;
        const medium = (await env.DB.prepare("SELECT id,name,risk_score,total_spend FROM vendors WHERE org_id=? AND risk_level='medium' AND status='active' ORDER BY risk_score DESC").bind(orgId).all()).results;
        const low = (await env.DB.prepare("SELECT COUNT(*) as c FROM vendors WHERE org_id=? AND risk_level='low' AND status='active'").bind(orgId).first()) as any;
        return json({ high_risk: high, medium_risk: medium, low_risk_count: low?.c || 0 });
      }

      // ═══════════════ DASHBOARD ═══════════════
      if (p === '/dashboard' && m === 'GET') {
        const orgId = getOrgId(url);
        const [totalVendors, totalSpend, atRisk, pendingPOs, expiringContracts, expiringDocs, topVendors, recentPOs, budgetUtil] = await Promise.all([
          env.DB.prepare("SELECT COUNT(*) as c FROM vendors WHERE org_id=? AND status='active'").bind(orgId).first(),
          env.DB.prepare("SELECT SUM(total_spend) as total FROM vendors WHERE org_id=? AND status='active'").bind(orgId).first(),
          env.DB.prepare("SELECT COUNT(*) as c FROM vendors WHERE org_id=? AND risk_level='high' AND status='active'").bind(orgId).first(),
          env.DB.prepare("SELECT COUNT(*) as c FROM purchase_orders WHERE org_id=? AND status='pending_approval'").bind(orgId).first(),
          env.DB.prepare("SELECT c.*,v.name as vendor_name FROM contracts c JOIN vendors v ON c.vendor_id=v.id WHERE c.org_id=? AND c.status='active' AND c.end_date <= date('now','+60 days') AND c.end_date >= date('now') ORDER BY c.end_date LIMIT 10").bind(orgId).all(),
          env.DB.prepare("SELECT cd.*,v.name as vendor_name FROM compliance_docs cd JOIN vendors v ON cd.vendor_id=v.id WHERE v.org_id=? AND cd.status='active' AND cd.expiry_date <= date('now','+30 days') AND cd.expiry_date >= date('now') ORDER BY cd.expiry_date LIMIT 10").bind(orgId).all(),
          env.DB.prepare("SELECT id,name,total_spend,performance_score,risk_level FROM vendors WHERE org_id=? AND status='active' ORDER BY total_spend DESC LIMIT 10").bind(orgId).all(),
          env.DB.prepare("SELECT po.*,v.name as vendor_name FROM purchase_orders po JOIN vendors v ON po.vendor_id=v.id WHERE po.org_id=? ORDER BY po.created_at DESC LIMIT 10").bind(orgId).all(),
          env.DB.prepare("SELECT COUNT(*) as total, SUM(amount) as total_budget, SUM(spent) as total_spent FROM budgets WHERE org_id=? AND status='active'").bind(orgId).first(),
        ]);
        return json({
          total_vendors: (totalVendors as any)?.c || 0,
          total_spend: (totalSpend as any)?.total || 0,
          at_risk_vendors: (atRisk as any)?.c || 0,
          pending_approvals: (pendingPOs as any)?.c || 0,
          budget_summary: { count: (budgetUtil as any)?.total || 0, total: (budgetUtil as any)?.total_budget || 0, spent: (budgetUtil as any)?.total_spent || 0 },
          expiring_contracts: expiringContracts.results,
          expiring_compliance: expiringDocs.results,
          top_vendors: topVendors.results,
          recent_purchase_orders: recentPOs.results,
        });
      }

      // ═══════════════ AI ═══════════════
      if (p === '/ai/vendor-analysis' && m === 'POST') {
        const b: any = await req.json();
        try {
          const vendor = await env.DB.prepare('SELECT * FROM vendors WHERE id=?').bind(b.vendor_id).first();
          const reviews = (await env.DB.prepare('SELECT * FROM performance_reviews WHERE vendor_id=? ORDER BY created_at DESC LIMIT 5').bind(b.vendor_id).all()).results;
          const spend = (await env.DB.prepare('SELECT SUM(amount) as total, COUNT(*) as count FROM spend_records WHERE vendor_id=?').bind(b.vendor_id).first()) as any;
          const aiResp = await env.ENGINE_RUNTIME.fetch('https://engine/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ engine_id: 'LG-01', query: `Analyze vendor for risk and performance. Name: ${(vendor as any)?.name}. Category: ${(vendor as any)?.category}. Risk level: ${(vendor as any)?.risk_level}. Performance score: ${(vendor as any)?.performance_score}. Total spend: $${spend?.total || 0}. Orders: ${(vendor as any)?.total_orders}. Reviews: ${JSON.stringify(reviews)}. Provide: 1) Risk assessment, 2) Performance analysis, 3) Recommendations, 4) Red flags.`, max_doctrines: 3 }) });
          const aiData: any = await aiResp.json();
          return json({ analysis: aiData.answer || aiData.response || 'Analysis unavailable' });
        } catch { return json({ analysis: 'AI analysis temporarily unavailable' }); }
      }

      if (p === '/ai/spend-optimization' && m === 'POST') {
        const b: any = await req.json();
        try {
          const topSpend = (await env.DB.prepare("SELECT v.name,SUM(s.amount) as total FROM spend_records s JOIN vendors v ON s.vendor_id=v.id WHERE s.org_id=? AND s.created_at >= date('now','-90 days') GROUP BY s.vendor_id ORDER BY total DESC LIMIT 10").bind(b.org_id).all()).results;
          const byCat = (await env.DB.prepare("SELECT category,SUM(amount) as total FROM spend_records WHERE org_id=? AND created_at >= date('now','-90 days') GROUP BY category ORDER BY total DESC").bind(b.org_id).all()).results;
          const aiResp = await env.ENGINE_RUNTIME.fetch('https://engine/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ engine_id: 'LG-01', query: `Analyze vendor spend and suggest optimizations. Top vendors by spend (90d): ${JSON.stringify(topSpend)}. By category: ${JSON.stringify(byCat)}. Identify: 1) Consolidation opportunities, 2) Cost reduction strategies, 3) Vendor concentration risks, 4) Budget recommendations.`, max_doctrines: 3 }) });
          const aiData: any = await aiResp.json();
          return json({ optimization: aiData.answer || aiData.response || 'Analysis unavailable' });
        } catch { return json({ optimization: 'AI analysis temporarily unavailable' }); }
      }

      // ═══════════════ EXPORT ═══════════════
      if (p === '/export' && m === 'GET') {
        const orgId = getOrgId(url); const format = url.searchParams.get('format') || 'json';
        const type = url.searchParams.get('type') || 'vendors';
        let rows: any[];
        if (type === 'purchase-orders') {
          rows = (await env.DB.prepare("SELECT po.*,v.name as vendor_name FROM purchase_orders po JOIN vendors v ON po.vendor_id=v.id WHERE po.org_id=? ORDER BY po.created_at DESC").bind(orgId).all()).results as any[];
        } else if (type === 'spend') {
          rows = (await env.DB.prepare("SELECT s.*,v.name as vendor_name FROM spend_records s JOIN vendors v ON s.vendor_id=v.id WHERE s.org_id=? ORDER BY s.created_at DESC").bind(orgId).all()).results as any[];
        } else {
          rows = (await env.DB.prepare("SELECT * FROM vendors WHERE org_id=? ORDER BY name").bind(orgId).all()).results as any[];
        }
        if (format === 'csv' && rows.length) {
          const h = Object.keys(rows[0]);
          const csv = [h.join(','), ...rows.map(r => h.map(k => `"${String(r[k] ?? '').replace(/"/g, '""')}"`).join(','))].join('\n');
          return new Response(csv, { headers: { 'Content-Type': 'text/csv', 'Content-Disposition': `attachment; filename="${type}-export.csv"`, 'Access-Control-Allow-Origin': '*' } });
        }
        return json(rows);
      }

      // ═══════════════ SCHEMA MIGRATION ═══════════════
      if (p === '/admin/migrate-v2' && m === 'POST') {
        const migrations = [
          "CREATE TABLE IF NOT EXISTS budgets (id INTEGER PRIMARY KEY AUTOINCREMENT, org_id INTEGER NOT NULL, name TEXT NOT NULL, department TEXT, category TEXT, amount REAL DEFAULT 0, spent REAL DEFAULT 0, remaining REAL DEFAULT 0, period_start TEXT, period_end TEXT, owner_id INTEGER, status TEXT DEFAULT 'active', created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')))",
          "CREATE TABLE IF NOT EXISTS po_receipts (id INTEGER PRIMARY KEY AUTOINCREMENT, po_id INTEGER NOT NULL, received_by TEXT, items_received TEXT, quality_notes TEXT, delivery_notes TEXT, received_at TEXT DEFAULT (datetime('now')))",
          "ALTER TABLE purchase_orders ADD COLUMN budget_id INTEGER",
          "ALTER TABLE purchase_orders ADD COLUMN department TEXT",
          "ALTER TABLE purchase_orders ADD COLUMN urgency TEXT DEFAULT 'normal'",
          "ALTER TABLE purchase_orders ADD COLUMN received_quantity INTEGER DEFAULT 0",
          "ALTER TABLE purchase_orders ADD COLUMN rejection_reason TEXT",
          "ALTER TABLE vendors ADD COLUMN onboarding_status TEXT DEFAULT 'active'",
          "ALTER TABLE contracts ADD COLUMN parent_contract_id INTEGER",
          "ALTER TABLE compliance_docs ADD COLUMN document_url TEXT",
          "ALTER TABLE compliance_docs ADD COLUMN notes TEXT",
          "ALTER TABLE vendor_contacts ADD COLUMN department TEXT",
        ];
        const results: string[] = [];
        for (const sql of migrations) {
          try { await env.DB.prepare(sql).run(); results.push(`OK: ${sql.slice(0, 60)}...`); }
          catch (e: any) { results.push(`SKIP: ${e.message?.includes('duplicate') || e.message?.includes('already exists') ? 'exists' : e.message}`); }
        }
        slog('info', 'V2 migration complete', { results });
        return json({ migrated: true, results });
      }

      return err('Not found', 404);
    } catch (e: any) {
      slog('error', 'Internal error', { error: e.message, path: p });
      return json({ error: 'Internal error', detail: e.message }, 500);
    }
  },

  async scheduled(_event: ScheduledEvent, env: Env): Promise<void> {
    const now = new Date().toISOString().slice(0, 10);
    slog('info', 'Cron running', { date: now });

    const orgs = (await env.DB.prepare("SELECT id FROM organizations WHERE status='active'").all()).results as any[];
    for (const org of orgs) {
      // Daily spend tracking
      const stats = (await env.DB.prepare("SELECT COUNT(*) as total FROM vendors WHERE org_id=? AND status='active'").bind(org.id).first()) as any;
      const spend = (await env.DB.prepare("SELECT SUM(amount) as total FROM spend_records WHERE org_id=? AND created_at >= date('now','-1 day')").bind(org.id).first()) as any;
      const atRisk = (await env.DB.prepare("SELECT COUNT(*) as c FROM vendors WHERE org_id=? AND risk_level='high' AND status='active'").bind(org.id).first()) as any;
      const expiring = (await env.DB.prepare("SELECT COUNT(*) as c FROM contracts WHERE org_id=? AND status='active' AND end_date <= date('now','+30 days') AND end_date >= date('now')").bind(org.id).first()) as any;
      const pending = (await env.DB.prepare("SELECT COUNT(*) as c FROM purchase_orders WHERE org_id=? AND status='pending_approval'").bind(org.id).first()) as any;
      await env.DB.prepare('INSERT OR REPLACE INTO spend_daily (org_id,date,total_vendors,total_spend,at_risk_vendors,expiring_contracts,pending_approvals) VALUES (?,?,?,?,?,?,?)').bind(org.id, now, stats?.total || 0, spend?.total || 0, atRisk?.c || 0, expiring?.c || 0, pending?.c || 0).run();

      // Auto-expire contracts
      await env.DB.prepare("UPDATE contracts SET status='expired',updated_at=datetime('now') WHERE org_id=? AND status='active' AND end_date < date('now') AND auto_renew=0").bind(org.id).run();

      // Expire compliance docs
      await env.DB.prepare("UPDATE compliance_docs SET status='expired' WHERE vendor_id IN (SELECT id FROM vendors WHERE org_id=?) AND status='active' AND expiry_date < date('now')").bind(org.id).run();
    }

    // Clean old activity data
    await env.DB.prepare("DELETE FROM spend_daily WHERE date < date('now','-365 days')").run();
    slog('info', 'Cron complete');
  },
};
