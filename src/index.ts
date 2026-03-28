// Echo Vendor Manager v1.0.0 — AI-Powered Vendor & Procurement Management
// Cloudflare Worker — D1 + KV

interface Env { DB: D1Database; VM_CACHE: KVNamespace; ENGINE_RUNTIME: Fetcher; SHARED_BRAIN: Fetcher; EMAIL_SENDER: Fetcher; ECHO_API_KEY: string; }
interface RLState { c: number; t: number }
function sanitize(s: string, max = 2000): string { return s.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f]/g, '').slice(0, max); }
async function rateLimit(kv: KVNamespace, key: string, limit: number, windowSec: number): Promise<boolean> {
  const k = `rl:${key}`; const raw = await kv.get(k); const now = Date.now();
  let st: RLState = raw ? JSON.parse(raw) : { c: 0, t: now };
  st.c = Math.max(0, st.c - ((now - st.t) / 1000) * (limit / windowSec)); st.t = now;
  if (st.c >= limit) return false; st.c += 1;
  await kv.put(k, JSON.stringify(st), { expirationTtl: windowSec * 2 }); return true;
}
function authOk(req: Request, env: Env): boolean { return (req.headers.get('X-Echo-API-Key') || req.headers.get('Authorization')?.replace('Bearer ', '') || '') === env.ECHO_API_KEY; }
function log(level: string, msg: string, data?: Record<string, any>) { console.log(JSON.stringify({ level, service: 'echo-vendor-manager', version: '1.0.1', msg, ...data, ts: new Date().toISOString() })); }
function json(data: unknown, status = 200) { return new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } }); }
function err(msg: string, status = 400) { log('warn', msg, { status }); return json({ error: msg }, status); }

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url); const p = url.pathname; const m = req.method;
    if (m === 'OPTIONS') return new Response(null, { headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type,X-Echo-API-Key,Authorization' } });
    if (p === '/') return json({ service: 'echo-vendor-manager', version: '1.0.0', status: 'operational' });
    if (p === '/health') return json({ ok: true, service: 'echo-vendor-manager', version: '1.0.0', timestamp: new Date().toISOString().replace('T', ' ').slice(0, 19) });
    if (m === 'GET') { const ip = req.headers.get('CF-Connecting-IP') || 'unknown'; if (!await rateLimit(env.VM_CACHE, `get:${ip}`, 60, 60)) return err('Rate limited', 429); }
    if (m !== 'GET' && m !== 'OPTIONS' && !authOk(req, env)) return err('Unauthorized', 401);

    try {
      // ── Organizations ──
      if (p === '/orgs' && m === 'GET') return json((await env.DB.prepare("SELECT * FROM organizations WHERE status='active' ORDER BY name").all()).results);
      if (p === '/orgs' && m === 'POST') { const b: any = await req.json(); const r = await env.DB.prepare('INSERT INTO organizations (name,slug,approval_threshold,payment_terms_default,settings) VALUES (?,?,?,?,?)').bind(sanitize(b.name), sanitize(b.slug || b.name.toLowerCase().replace(/\s+/g, '-')), b.approval_threshold || 5000, b.payment_terms_default || 30, JSON.stringify(b.settings || {})).run(); return json({ id: r.meta.last_row_id }, 201); }

      // ── Users ──
      if (p === '/users' && m === 'GET') { return json((await env.DB.prepare("SELECT * FROM users WHERE org_id=? AND status='active' ORDER BY name").bind(url.searchParams.get('org_id')).all()).results); }
      if (p === '/users' && m === 'POST') { const b: any = await req.json(); const r = await env.DB.prepare('INSERT INTO users (org_id,name,email,role,approval_limit) VALUES (?,?,?,?,?)').bind(b.org_id, sanitize(b.name), sanitize(b.email), b.role || 'viewer', b.approval_limit || 0).run(); return json({ id: r.meta.last_row_id }, 201); }

      // ── Vendors ──
      if (p === '/vendors' && m === 'GET') {
        const orgId = url.searchParams.get('org_id'); const cat = url.searchParams.get('category'); const risk = url.searchParams.get('risk_level');
        let q = "SELECT * FROM vendors WHERE org_id=? AND status='active'"; const binds: any[] = [orgId];
        if (cat) { q += ' AND category=?'; binds.push(cat); }
        if (risk) { q += ' AND risk_level=?'; binds.push(risk); }
        q += ' ORDER BY name'; return json((await env.DB.prepare(q).bind(...binds).all()).results);
      }
      if (p === '/vendors' && m === 'POST') {
        const b: any = await req.json();
        const r = await env.DB.prepare('INSERT INTO vendors (org_id,name,legal_name,tax_id,category,industry,website,primary_contact,primary_email,primary_phone,address,city,state,country,zip,payment_terms,payment_method,risk_level,diversity_cert,minority_owned,woman_owned,veteran_owned,small_business,tags,custom_fields,notes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(b.org_id, sanitize(b.name), sanitize(b.legal_name || ''), sanitize(b.tax_id || ''), sanitize(b.category || ''), sanitize(b.industry || ''), sanitize(b.website || ''), sanitize(b.primary_contact || ''), sanitize(b.primary_email || ''), sanitize(b.primary_phone || ''), sanitize(b.address || ''), sanitize(b.city || ''), sanitize(b.state || ''), b.country || 'US', sanitize(b.zip || ''), b.payment_terms || 30, b.payment_method || 'ach', b.risk_level || 'low', sanitize(b.diversity_cert || ''), b.minority_owned || 0, b.woman_owned || 0, b.veteran_owned || 0, b.small_business || 0, JSON.stringify(b.tags || []), JSON.stringify(b.custom_fields || {}), sanitize(b.notes || '')).run();
        return json({ id: r.meta.last_row_id }, 201);
      }
      const vendorMatch = p.match(/^\/vendors\/(\d+)$/);
      if (vendorMatch && m === 'GET') {
        const v = await env.DB.prepare('SELECT * FROM vendors WHERE id=?').bind(vendorMatch[1]).first();
        if (!v) return err('Not found', 404);
        const contacts = (await env.DB.prepare('SELECT * FROM vendor_contacts WHERE vendor_id=? ORDER BY is_primary DESC').bind(vendorMatch[1]).all()).results;
        const contracts = (await env.DB.prepare("SELECT * FROM contracts WHERE vendor_id=? AND status='active' ORDER BY end_date").bind(vendorMatch[1]).all()).results;
        const reviews = (await env.DB.prepare('SELECT * FROM performance_reviews WHERE vendor_id=? ORDER BY created_at DESC LIMIT 5').bind(vendorMatch[1]).all()).results;
        const compliance = (await env.DB.prepare("SELECT * FROM compliance_docs WHERE vendor_id=? AND status='active' ORDER BY expiry_date").bind(vendorMatch[1]).all()).results;
        const spend = (await env.DB.prepare('SELECT SUM(amount) as total, COUNT(*) as count FROM spend_records WHERE vendor_id=?').bind(vendorMatch[1]).first()) as any;
        return json({ ...v, contacts, contracts, reviews, compliance, spend_total: spend?.total || 0, spend_count: spend?.count || 0 });
      }
      if (vendorMatch && m === 'PUT') {
        const b: any = await req.json(); const fields: string[] = []; const vals: any[] = [];
        for (const [k, val] of Object.entries(b)) {
          if (['name','legal_name','tax_id','category','industry','website','primary_contact','primary_email','primary_phone','address','city','state','country','zip','payment_terms','payment_method','risk_level','status','notes','tags','custom_fields'].includes(k)) {
            fields.push(`${k}=?`); vals.push(typeof val === 'string' ? sanitize(val as string) : typeof val === 'object' ? JSON.stringify(val) : val);
          }
        }
        if (fields.length) { fields.push("updated_at=datetime('now')"); vals.push(vendorMatch[1]); await env.DB.prepare(`UPDATE vendors SET ${fields.join(',')} WHERE id=?`).bind(...vals).run(); }
        return json({ updated: true });
      }

      // ── Vendor Contacts ──
      if (p === '/contacts' && m === 'POST') { const b: any = await req.json(); const r = await env.DB.prepare('INSERT INTO vendor_contacts (vendor_id,name,title,email,phone,is_primary) VALUES (?,?,?,?,?,?)').bind(b.vendor_id, sanitize(b.name), sanitize(b.title || ''), sanitize(b.email || ''), sanitize(b.phone || ''), b.is_primary || 0).run(); return json({ id: r.meta.last_row_id }, 201); }

      // ── Contracts ──
      if (p === '/contracts' && m === 'GET') { const orgId = url.searchParams.get('org_id'); return json((await env.DB.prepare("SELECT c.*,v.name as vendor_name FROM contracts c JOIN vendors v ON c.vendor_id=v.id WHERE c.org_id=? AND c.status='active' ORDER BY c.end_date").bind(orgId).all()).results); }
      if (p === '/contracts' && m === 'POST') { const b: any = await req.json(); const r = await env.DB.prepare('INSERT INTO contracts (vendor_id,org_id,title,contract_number,type,value,annual_value,start_date,end_date,auto_renew,renewal_notice_days,payment_terms,sla_terms,owner_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(b.vendor_id, b.org_id, sanitize(b.title), sanitize(b.contract_number || ''), b.type || 'service', b.value || 0, b.annual_value || 0, b.start_date || null, b.end_date || null, b.auto_renew || 0, b.renewal_notice_days || 60, b.payment_terms || 30, sanitize(b.sla_terms || ''), b.owner_id || null).run(); return json({ id: r.meta.last_row_id }, 201); }

      // ── Purchase Orders ──
      if (p === '/purchase-orders' && m === 'GET') { const orgId = url.searchParams.get('org_id'); const status = url.searchParams.get('status'); let q = "SELECT po.*,v.name as vendor_name FROM purchase_orders po JOIN vendors v ON po.vendor_id=v.id WHERE po.org_id=?"; const binds: any[] = [orgId]; if (status) { q += ' AND po.status=?'; binds.push(status); } q += ' ORDER BY po.created_at DESC'; return json((await env.DB.prepare(q).bind(...binds).all()).results); }
      if (p === '/purchase-orders' && m === 'POST') {
        const b: any = await req.json();
        const poNum = `PO-${Date.now().toString(36).toUpperCase()}`;
        const items = b.items || []; const subtotal = items.reduce((s: number, i: any) => s + ((i.quantity || 0) * (i.unit_price || 0)), 0);
        const total = subtotal + (b.tax || 0) + (b.shipping || 0);
        // Auto-approve if under threshold
        const org = await env.DB.prepare('SELECT approval_threshold FROM organizations WHERE id=?').bind(b.org_id).first() as any;
        const autoApprove = total <= (org?.approval_threshold || 5000);
        const r = await env.DB.prepare('INSERT INTO purchase_orders (org_id,vendor_id,po_number,requester_id,items,subtotal,tax,shipping,total,delivery_date,notes,approval_status,status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(b.org_id, b.vendor_id, poNum, b.requester_id || null, JSON.stringify(items), subtotal, b.tax || 0, b.shipping || 0, total, b.delivery_date || null, sanitize(b.notes || ''), autoApprove ? 'approved' : 'pending', autoApprove ? 'approved' : 'pending_approval').run();
        // Update vendor spend
        await env.DB.prepare('UPDATE vendors SET total_orders=total_orders+1,updated_at=datetime(\'now\') WHERE id=?').bind(b.vendor_id).run();
        return json({ id: r.meta.last_row_id, po_number: poNum, auto_approved: autoApprove }, 201);
      }
      const poApprove = p.match(/^\/purchase-orders\/(\d+)\/approve$/);
      if (poApprove && m === 'POST') { const b: any = await req.json(); await env.DB.prepare("UPDATE purchase_orders SET approval_status='approved',approver_id=?,approved_at=datetime('now'),status='approved',updated_at=datetime('now') WHERE id=?").bind(b.approver_id, poApprove[1]).run(); return json({ approved: true }); }
      const poReject = p.match(/^\/purchase-orders\/(\d+)\/reject$/);
      if (poReject && m === 'POST') { const b: any = await req.json(); await env.DB.prepare("UPDATE purchase_orders SET approval_status='rejected',approver_id=?,status='rejected',updated_at=datetime('now') WHERE id=?").bind(b.approver_id, poReject[1]).run(); return json({ rejected: true }); }

      // ── Performance Reviews ──
      if (p === '/reviews' && m === 'POST') {
        const b: any = await req.json();
        const overall = ((b.quality_score || 0) + (b.delivery_score || 0) + (b.communication_score || 0) + (b.pricing_score || 0)) / 4;
        const r = await env.DB.prepare('INSERT INTO performance_reviews (vendor_id,org_id,reviewer_id,period_start,period_end,quality_score,delivery_score,communication_score,pricing_score,overall_score,strengths,weaknesses,recommendation) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)').bind(b.vendor_id, b.org_id, b.reviewer_id || null, b.period_start || null, b.period_end || null, b.quality_score || 0, b.delivery_score || 0, b.communication_score || 0, b.pricing_score || 0, overall, sanitize(b.strengths || ''), sanitize(b.weaknesses || ''), sanitize(b.recommendation || '')).run();
        await env.DB.prepare("UPDATE vendors SET performance_score=?,updated_at=datetime('now') WHERE id=?").bind(overall, b.vendor_id).run();
        return json({ id: r.meta.last_row_id, overall_score: overall }, 201);
      }

      // ── Compliance Docs ──
      if (p === '/compliance' && m === 'GET') { const vendorId = url.searchParams.get('vendor_id'); return json((await env.DB.prepare("SELECT * FROM compliance_docs WHERE vendor_id=? AND status='active' ORDER BY expiry_date").bind(vendorId).all()).results); }
      if (p === '/compliance' && m === 'POST') { const b: any = await req.json(); const r = await env.DB.prepare('INSERT INTO compliance_docs (vendor_id,doc_type,doc_name,issued_date,expiry_date) VALUES (?,?,?,?,?)').bind(b.vendor_id, sanitize(b.doc_type), sanitize(b.doc_name || ''), b.issued_date || null, b.expiry_date || null).run(); return json({ id: r.meta.last_row_id }, 201); }

      // ── Spend Records ──
      if (p === '/spend' && m === 'POST') { const b: any = await req.json(); const r = await env.DB.prepare('INSERT INTO spend_records (org_id,vendor_id,amount,category,description,invoice_number,invoice_date,po_id) VALUES (?,?,?,?,?,?,?,?)').bind(b.org_id, b.vendor_id, b.amount, sanitize(b.category || ''), sanitize(b.description || ''), sanitize(b.invoice_number || ''), b.invoice_date || null, b.po_id || null).run(); await env.DB.prepare("UPDATE vendors SET total_spend=total_spend+?,updated_at=datetime('now') WHERE id=?").bind(b.amount, b.vendor_id).run(); return json({ id: r.meta.last_row_id }, 201); }
      if (p === '/spend/analytics' && m === 'GET') {
        const orgId = url.searchParams.get('org_id'); const days = parseInt(url.searchParams.get('days') || '90');
        const byVendor = (await env.DB.prepare("SELECT v.name,SUM(s.amount) as total FROM spend_records s JOIN vendors v ON s.vendor_id=v.id WHERE s.org_id=? AND s.created_at >= date('now',?) GROUP BY s.vendor_id ORDER BY total DESC LIMIT 20").bind(orgId, `-${days} days`).all()).results;
        const byCategory = (await env.DB.prepare("SELECT category,SUM(amount) as total FROM spend_records WHERE org_id=? AND created_at >= date('now',?) GROUP BY category ORDER BY total DESC").bind(orgId, `-${days} days`).all()).results;
        const totalSpend = (await env.DB.prepare("SELECT SUM(amount) as total FROM spend_records WHERE org_id=? AND created_at >= date('now',?)").bind(orgId, `-${days} days`).first()) as any;
        return json({ total_spend: totalSpend?.total || 0, by_vendor: byVendor, by_category: byCategory });
      }

      // ── Risk Assessment ──
      if (p === '/risk-assessment' && m === 'POST') {
        const b: any = await req.json();
        const overall = ((b.financial_risk || 0) + (b.operational_risk || 0) + (b.compliance_risk || 0) + (b.reputational_risk || 0)) / 4;
        const r = await env.DB.prepare('INSERT INTO risk_assessments (vendor_id,assessed_by,financial_risk,operational_risk,compliance_risk,reputational_risk,overall_risk,notes) VALUES (?,?,?,?,?,?,?,?)').bind(b.vendor_id, b.assessed_by || null, b.financial_risk || 0, b.operational_risk || 0, b.compliance_risk || 0, b.reputational_risk || 0, overall, sanitize(b.notes || '')).run();
        const riskLevel = overall >= 70 ? 'high' : overall >= 40 ? 'medium' : 'low';
        await env.DB.prepare("UPDATE vendors SET risk_score=?,risk_level=?,updated_at=datetime('now') WHERE id=?").bind(overall, riskLevel, b.vendor_id).run();
        return json({ id: r.meta.last_row_id, overall_risk: overall, risk_level: riskLevel }, 201);
      }

      // ── Dashboard ──
      if (p === '/dashboard' && m === 'GET') {
        const orgId = url.searchParams.get('org_id');
        const totalVendors = (await env.DB.prepare("SELECT COUNT(*) as c FROM vendors WHERE org_id=? AND status='active'").bind(orgId).first()) as any;
        const totalSpend = (await env.DB.prepare("SELECT SUM(total_spend) as total FROM vendors WHERE org_id=? AND status='active'").bind(orgId).first()) as any;
        const atRisk = (await env.DB.prepare("SELECT COUNT(*) as c FROM vendors WHERE org_id=? AND risk_level='high' AND status='active'").bind(orgId).first()) as any;
        const pendingPOs = (await env.DB.prepare("SELECT COUNT(*) as c FROM purchase_orders WHERE org_id=? AND status='pending_approval'").bind(orgId).first()) as any;
        const expiringContracts = (await env.DB.prepare("SELECT c.*,v.name as vendor_name FROM contracts c JOIN vendors v ON c.vendor_id=v.id WHERE c.org_id=? AND c.status='active' AND c.end_date <= date('now','+60 days') AND c.end_date >= date('now') ORDER BY c.end_date LIMIT 10").bind(orgId).all()).results;
        const expiringDocs = (await env.DB.prepare("SELECT cd.*,v.name as vendor_name FROM compliance_docs cd JOIN vendors v ON cd.vendor_id=v.id WHERE v.org_id=? AND cd.status='active' AND cd.expiry_date <= date('now','+30 days') AND cd.expiry_date >= date('now') ORDER BY cd.expiry_date LIMIT 10").bind(orgId).all()).results;
        const topVendors = (await env.DB.prepare("SELECT id,name,total_spend,performance_score,risk_level FROM vendors WHERE org_id=? AND status='active' ORDER BY total_spend DESC LIMIT 10").bind(orgId).all()).results;
        const recentPOs = (await env.DB.prepare("SELECT po.*,v.name as vendor_name FROM purchase_orders po JOIN vendors v ON po.vendor_id=v.id WHERE po.org_id=? ORDER BY po.created_at DESC LIMIT 10").bind(orgId).all()).results;
        return json({ total_vendors: totalVendors?.c || 0, total_spend: totalSpend?.total || 0, at_risk_vendors: atRisk?.c || 0, pending_approvals: pendingPOs?.c || 0, expiring_contracts: expiringContracts, expiring_compliance: expiringDocs, top_vendors: topVendors, recent_purchase_orders: recentPOs });
      }

      // ── AI Vendor Analysis ──
      if (p === '/ai/vendor-analysis' && m === 'POST') {
        const b: any = await req.json();
        try {
          const vendor = await env.DB.prepare('SELECT * FROM vendors WHERE id=?').bind(b.vendor_id).first();
          const reviews = (await env.DB.prepare('SELECT * FROM performance_reviews WHERE vendor_id=? ORDER BY created_at DESC LIMIT 5').bind(b.vendor_id).all()).results;
          const aiResp = await env.ENGINE_RUNTIME.fetch('https://engine/query', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ engine_id: 'LG01', query: `Analyze this vendor for risk and performance. Vendor: ${JSON.stringify(vendor)}. Performance reviews: ${JSON.stringify(reviews)}. Provide risk assessment, performance analysis, and recommendations.`, max_doctrines: 3 }) });
          const aiData: any = await aiResp.json();
          return json({ analysis: aiData.answer || aiData.response || 'Analysis unavailable' });
        } catch { return json({ analysis: 'AI analysis temporarily unavailable' }); }
      }

      // ── Export ──
      if (p === '/export' && m === 'GET') {
        const orgId = url.searchParams.get('org_id'); const format = url.searchParams.get('format') || 'json';
        const rows = (await env.DB.prepare("SELECT * FROM vendors WHERE org_id=? ORDER BY name").bind(orgId).all()).results as any[];
        if (format === 'csv' && rows.length) { const h = Object.keys(rows[0]); const csv = [h.join(','), ...rows.map(r => h.map(k => `"${String(r[k] ?? '').replace(/"/g, '""')}"`).join(','))].join('\n'); return new Response(csv, { headers: { 'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename="vendors-export.csv"', 'Access-Control-Allow-Origin': '*' } }); }
        return json(rows);
      }

      return err('Not found', 404);
    } catch (e: any) { log('error', 'Internal error', { error: e.message, path: p }); return json({ error: 'Internal error', detail: e.message }, 500); }
  },

  async scheduled(_event: ScheduledEvent, env: Env): Promise<void> {
    const now = new Date().toISOString().slice(0, 10);
    const orgs = (await env.DB.prepare("SELECT id FROM organizations WHERE status='active'").all()).results as any[];
    for (const org of orgs) {
      const stats = (await env.DB.prepare("SELECT COUNT(*) as total FROM vendors WHERE org_id=? AND status='active'").bind(org.id).first()) as any;
      const spend = (await env.DB.prepare("SELECT SUM(amount) as total FROM spend_records WHERE org_id=? AND created_at >= date('now','-1 day')").bind(org.id).first()) as any;
      const atRisk = (await env.DB.prepare("SELECT COUNT(*) as c FROM vendors WHERE org_id=? AND risk_level='high' AND status='active'").bind(org.id).first()) as any;
      const expiring = (await env.DB.prepare("SELECT COUNT(*) as c FROM contracts WHERE org_id=? AND status='active' AND end_date <= date('now','+30 days') AND end_date >= date('now')").bind(org.id).first()) as any;
      const pending = (await env.DB.prepare("SELECT COUNT(*) as c FROM purchase_orders WHERE org_id=? AND status='pending_approval'").bind(org.id).first()) as any;
      await env.DB.prepare('INSERT OR REPLACE INTO spend_daily (org_id,date,total_vendors,total_spend,at_risk_vendors,expiring_contracts,pending_approvals) VALUES (?,?,?,?,?,?,?)').bind(org.id, now, stats?.total || 0, spend?.total || 0, atRisk?.c || 0, expiring?.c || 0, pending?.c || 0).run();
    }
  },
};
