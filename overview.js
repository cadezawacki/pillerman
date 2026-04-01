
import {BaseWidget} from "@/pt/js/widgets/baseWidget.js";
import interact from 'interactjs';
import {generateGradientChroma} from '@/utils/colorHelpers.js';
import {writeObjectToClipboard} from "@/utils/clipboardHelpers.js";
import {QT_MAP} from '@/pt/js/grids/portfolio/portfolioColumns.js';
import {coerceToNumeric, roundToNumeric, coerceToBool} from "@/utils/NumberFormatter.js";
import {ENumberFlow} from '@/utils/eNumberFlow.js';
import {debounce, asArray} from '@/utils/helpers.js';
import {InfoCardStack} from '@/global/js/cardStack.js';
import {sort_dictionary_by_value} from "@/utils/utility.js";
import {NumberFormatter} from "@/utils/NumberFormatter.js";
import {StringFormatter} from "@/utils/StringFormatter.js";
import {MICRO_GRID_GROUPS} from "@/global/js/microGridConfigs.js";

const clean_camel = StringFormatter.clean_camel;

/* ═══════════════════════════════════════════════════════════════════════════
   MODULE-LEVEL CONSTANTS & HELPERS
   ═══════════════════════════════════════════════════════════════════════════ */

const EMPTY_I32 = Object.freeze(new Int32Array(0));

/**
 * Columns the overview widget always needs for KPI computation,
 * distribution bars, liquidity cards, pill flags, and pivot breakdowns.
 */
const REQUIRED_COLUMNS = Object.freeze([
    // KPI top-line + net
    'grossSize', 'grossDv01', 'netSize', 'netDv01', '_normalizedRisk',
    // KPI secondary metrics
    'duration', 'liqScoreCombined', 'signalLiveStats',
    // Axed / anti-axed
    'isAxed', 'userSide', 'isAntiAxe', 'axeDirection', 'direction',
    // BSR / Yield / Algo
    'firmAggBsrSize', 'unitDv01', 'bvalMidYld', 'isInAlgoUniverse',
    // Right-panel chart pivots
    'emProductType', 'yieldCurvePosition',
    // Liquidity charges
    'QT',
    // Distribution bar dropdown options (categorical)
    'deskAsset', 'regionCountry', 'desigName', 'maturityBucket',
    'ratingCombined', 'ratingMnemonic', 'industrySector',
    // Pill flags
    'description', 'isin', 'currency', 'isPriced', 'claimed',
    'isMuni', 'isNewIssue', 'isInDefault', 'isDnt', 'restrictedCode',
    'daysToSettle', 'isRfqBenchmarkMismatch', 'isBvalBenchmarkMismatch',
    'isMacpBenchmarkMismatch', 'isStub', 'assignedTrader', 'grossSize',
    'isReal', 'quoteType',
]);

const SORT_ORDERS = Object.freeze({
    ratingCombined: ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-', 'B+', 'B', 'B-', 'CCC+', 'CCC', 'CCC-', 'CC', 'C', 'D', 'NR', 'N/A'],
    maturityBucket: ['<2Y', '2-5Y', '5-10Y', '10-20Y', '20Y+'],
    yieldCurvePosition: ['Front-end', 'Belly', 'Intermediate', 'Long-end'],
    ratingMnemonic: ['PRIME', 'IG_HIGH_GRADE', 'IG_MEDIUM_GRADE', 'IG_LOW_GRADE', 'HY_UPPER_GRADE', 'HY_LOWER_GRADE', 'JUNK_UPPER_GRADE', 'JUNK_MEDIUM_GRADE', 'JUNK_LOW_GRADE', 'IN_DEFAULT'],
});


/* ─────────── Pill tooltip/modal helpers ─────────── */

/**
 * Given an engine and a predicate on a column, return a boolean mask
 * of which rows match. O(n) with hoisted getter.
 */
function maskByPredicate(engine, col, pred) {
    const n = engine.numRows() | 0;
    const getter = engine._getValueGetter(col);
    const mask = new Uint8Array(n);
    for (let i = 0; i < n; i++) {
        if (pred(getter(i))) mask[i] = 1;
    }
    return mask;
}

/**
 * Returns true when a value should be considered "missing" for
 * benchmark comparison purposes — null, undefined, empty string,
 * the literal strings "null"/"undefined"/"N/A", zero, or false.
 */
function _isMissingBenchmark(v) {
    if (v == null) return true;                   // null | undefined
    if (v === 0 || v === false) return true;      // falsy non-string
    const s = String(v).trim().toLowerCase();
    return s === '' || s === 'null' || s === 'undefined' || s === 'n/a';
}

/**
 * Given an engine and two column names, return a boolean mask
 * of which rows have mismatched values.
 * A row is skipped (never flagged) when either side is missing.
 */
function maskByMismatch(engine, colA, colB) {
    const n = engine.numRows() | 0;
    const getA = engine._getValueGetter(colA);
    const getB = engine._getValueGetter(colB);
    const mask = new Uint8Array(n);
    for (let i = 0; i < n; i++) {
        const a = getA(i), b = getB(i);
        if (_isMissingBenchmark(a) || _isMissingBenchmark(b)) continue;
        if (a !== b) mask[i] = 1;
    }
    return mask;
}

/**
 * Build human-readable summary lines from a mask.
 * Returns array of strings like "AAPL 5.25 2030 — ISIN: US037833..."
 */
function buildLines(engine, mask, cols) {
    cols = cols || ['description', 'isin'];
    const n = engine.numRows() | 0;
    const getters = cols.map(c => engine._getValueGetter(c));
    const lines = [];
    for (let i = 0; i < n; i++) {
        if (!mask[i]) continue;
        const parts = [];
        for (let c = 0; c < cols.length; c++) {
            const v = getters[c](i);
            if (v != null && String(v).trim()) parts.push(String(v).trim());
        }
        if (parts.length) lines.push(parts.join(' — '));
    }
    return lines;
}

function tooltipFromLines(lines, max = 8) {
    if (!lines || !lines.length) return '';
    const out = lines.length > max ? [...lines.slice(0, max), `+${lines.length - max} more`] : lines;
    return out.join('\n');
}

/**
 * Factory: returns a function suitable for pill.tooltipFn that lazily
 * evaluates the mask from the engine.
 */
function mkTooltip(maskFn, cols) {
    return (_display, pill) => {
        const eng = pill.mgr.engine;
        if (!eng) return '';
        const mask = maskFn(eng);
        const lines = buildLines(eng, mask, cols);
        return tooltipFromLines(lines);
    };
}

/**
 * Build a modal payload object from lines.
 */
function modalPayload(title, lines, cols, type) {
    const content = lines.length
        ? buildTable([cols || ['Description'], ...lines.map(l => [l])])?.outerHTML || '<p>No details.</p>'
        : '<p>No matching rows.</p>';
    return { title, content, type: type || 'info' };
}

/**
 * Factory: returns a function suitable for pill.modalFn.
 * colSpec can be an array of column names, or an object { colName: headerLabel }.
 */
function mkModal(title, maskFn, type, colSpec, subtitle, sortCols) {
    return (pill) => {
        const eng = pill.mgr.engine;
        if (!eng) return { title, content: '<p>No data.</p>', type: type || 'info' };

        const mask = maskFn(eng);
        let displayCols;
        let headers;

        if (colSpec && typeof colSpec === 'object' && !Array.isArray(colSpec)) {
            displayCols = Object.keys(colSpec);
            headers = Object.values(colSpec);
        } else {
            displayCols = Array.isArray(colSpec) ? colSpec : ['description', 'isin'];
            headers = displayCols.map(c => clean_camel(c));
        }

        const getters = displayCols.map(c => eng._getValueGetter(c));
        const n = eng.numRows() | 0;
        const rows = [];
        for (let i = 0; i < n; i++) {
            if (!mask[i]) continue;
            const row = [];
            for (let c = 0; c < displayCols.length; c++) {
                const v = getters[c](i);
                row.push(v == null ? '' : String(v));
            }
            rows.push(row);
        }

        if (sortCols && rows.length > 1) {
            const sortIndices = (Array.isArray(sortCols) ? sortCols : [sortCols])
                .map(c => displayCols.indexOf(c))
                .filter(i => i >= 0);
            if (sortIndices.length) {
                rows.sort((a, b) => {
                    for (const idx of sortIndices) {
                        const cmp = String(a[idx]).localeCompare(String(b[idx]));
                        if (cmp !== 0) return cmp;
                    }
                    return 0;
                });
            }
        }

        const tableData = [headers, ...rows];
        const table = buildTable(tableData);
        const sub = subtitle || (rows.length > 0 ? `${rows.length} row${rows.length > 1 ? 's' : ''}` : null);
        const html = table ? table.outerHTML : '<p>No matching rows.</p>';

        pill.mgr.createInfoModal(title, html, type || 'info', sub);
    };
}

/* ─────────── DOM helpers ─────────── */

function buildTable(dataArray) {
    if (!dataArray || dataArray.length < 1) return null;
    const table = document.createElement('table');
    table.classList.add('overview-modal-table');
    table.style.borderCollapse = 'collapse';
    table.style.width = '100%';

    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    for (let i = 0; i < dataArray[0].length; i++) {
        const th = document.createElement('th');
        th.textContent = dataArray[0][i];
        headerRow.appendChild(th);
    }
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    for (let i = 1; i < dataArray.length; i++) {
        const tr = document.createElement('tr');
        const rowData = dataArray[i];
        for (let j = 0; j < rowData.length; j++) {
            const td = document.createElement('td');
            td.textContent = rowData[j];
            tr.appendChild(td);
        }
        tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    return table;
}

/* ─────────── Formatting helpers (for summary string) ─────────── */

function normalizeMeta(src) {
    if (!src) return {};
    if (src?.asObject && (typeof src.asObject === 'function')) return src.asObject();
    if (Array.isArray(src)) { try { return Object.fromEntries(src); } catch { return _arrayPairsToObj(src); } }
    if (src instanceof Map) return Object.fromEntries(src);
    if (typeof src === 'object') return src;
    return {};
}

function _arrayPairsToObj(pairs) {
    const o = {};
    for (let i = 0; i < pairs.length; i++) {
        const p = pairs[i];
        if (p && typeof p[0] === 'string') o[p[0]] = p[1];
    }
    return o;
}

function nonEmpty(v) { if (v == null) return ''; const s = String(v).trim(); return s.length ? s : ''; }
function num(v) { const n = Number(v); return Number.isFinite(n) ? n : 0; }
function int(v) { const n = Number(v); if (!Number.isFinite(n)) return 0; return n < 0 ? Math.ceil(n) : Math.floor(n); }
function coalesce(...vals) { for (let i = 0; i < vals.length; i++) if (vals[i] !== undefined && vals[i] !== null) return vals[i]; return undefined; }
function preferNumber(...vals) { for (let i = 0; i < vals.length; i++) { const n = Number(vals[i]); if (Number.isFinite(n)) return n; } return undefined; }
function kv(k, v) { return (v === undefined || v === '' || v === null) ? `${k} -` : `${k} ${v}`; }
function fmtInt(n) { return String(int(n)); }
function round1(v) { return Number.isFinite(v) ? Math.round(v * 10) / 10 : v; }

function stripTrailingZero(s) {
    if (typeof s !== 'string') s = String(s);
    if (s.indexOf('.') === -1) return s;
    return s.replace(/\.0+$/, '').replace(/(\.\d*?[1-9])0+$/, '$1');
}

function fmtDur(n) { return !Number.isFinite(n) ? '-' : stripTrailingZero(round1(n)); }
function fmtLiq(n) { return !Number.isFinite(n) ? '-' : stripTrailingZero(round1(n)); }

function fmtSize(n) {
    const a = Math.abs(n);
    if (a >= 1e9) return stripTrailingZero((n / 1e9).toFixed(0)) + 'b';
    if (a >= 1e6) return stripTrailingZero((n / 1e6).toFixed(0)) + 'm';
    if (a >= 1e3) return stripTrailingZero((n / 1e3).toFixed(0)) + 'k';
    return String(int(n));
}

function fmtK(n) {
    const a = Math.abs(n);
    if (!Number.isFinite(a)) return '-';
    if (a >= 1e3) return stripTrailingZero((n / 1e3).toFixed(1)) + 'k';
    return stripTrailingZero(n.toFixed(0));
}

function truncateName(name) { const s = nonEmpty(name); if (!s) return '-'; return s.length <= 18 ? s : s.slice(0, 17) + '…'; }

function buildDirFlags(meta) {
    const flags = [];
    if (int(meta.isAon) === 1) flags.push('AON');
    if (int(meta.isCrossed) === 1) flags.push('Xed');
    if (nonEmpty(meta.inquiryType)) flags.push(meta.inquiryType);
    if (nonEmpty(meta.venue)) flags.push(meta.venue);
    if (nonEmpty(meta.assetClass)) flags.push(meta.assetClass);
    if (nonEmpty(meta.region)) flags.push(meta.region);
    return flags.join(' ');
}

function sideLine(label, s) {
    return `${label}: ${[kv('Ntl', fmtSize(s.gs)), kv('DV01', fmtK(s.gdv)), kv(fmtInt(s.ct), 'bonds'), kv('Liq', fmtLiq(s.liq)), s.rating || '', kv('Dur', fmtDur(s.dur)), kv('Sig', fmtDur(s.signal))].join(', ')}`;
}

function pickStatus(meta) {
    const a = (meta.state || '').toString().trim().toUpperCase();
    const b = (meta.rfqState || '').toString().trim().toUpperCase();
    return a || b || '';
}

function fmtDue(nowMs, dueIso) {
    const dueMs = Date.parse(dueIso);
    if (!Number.isFinite(dueMs)) return '-';
    let delta = Math.round((dueMs - nowMs) / 1000);
    const sign = delta >= 0 ? 1 : -1;
    delta = Math.abs(delta);
    const h = Math.floor(delta / 3600); delta -= h * 3600;
    const m = Math.floor(delta / 60);
    const s = delta - m * 60;
    if (h > 0 && m > 0) return sign > 0 ? `${h}h ${m}m` : `-${h}h ${m}m`;
    if (h > 0) return sign > 0 ? `${h}h` : `-${h}h`;
    if (m > 0) return sign > 0 ? `${m}m` : `-${m}m`;
    return sign > 0 ? `${s}s` : `-${s}s`;
}

function maturityBucket(y) {
    if (y == null || isNaN(+y)) return null;
    const v = +y;
    if (v <= 2) return 'Front-end';
    if (v <= 5) return 'Belly';
    if (v <= 10) return 'Intermediate';
    return 'Long-End';
}

function hhiClass(v) { return v == null ? null : (v > 0.7 ? 'Highly Concentrated' : (v < 0.2 ? 'Diversified' : null)); }

/* ═══════════════════════════════════════════════════════════════════════════
   OVERVIEW WIDGET
   ═══════════════════════════════════════════════════════════════════════════ */

export class OverviewWidget extends BaseWidget {

    static REQUIRED_COLUMNS = REQUIRED_COLUMNS;

    constructor(context, widgetId, managerId, selector, config = {}) {
        super(context, widgetId, managerId, selector, config);
        this.bidSkew = null;
        this.askSkew = null;
        this.resizer = null;
        this.pillManager = this.context.page.pillManager;
        this._rendered = false;
        this._ownedPills = [];
        this._debouncedMetaRefresh = null;
        this._stack = null;
        this._headerRow = null;
    }

    /* ────────── Lifecycle ────────── */

    async onInit() {
        this._permanentSubs = [];

        this.cacheDom();
        this.bindEvents();
        this.setupResizer();
        this.setupFlags();
        this.setupReactions();
        this._setupHotTickersButton();
        this.pivotEngine = this.context.page.getWidget('pivotWidget')?.ptPivot?.pivotEngine ?? null;
    }

    _setupHotTickersButton() {
        // Try to find an existing button, otherwise create one next to the settings button
        let btn = document.getElementById('hot-tickers-btn');
        if (!btn && this.context.page.settingsButton) {
            btn = document.createElement('button');
            btn.id = 'hot-tickers-btn';
            btn.className = 'btn btn-xs btn-ghost';
            btn.title = 'AI Tickers';
            btn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>';
            this.context.page.settingsButton.parentNode?.insertBefore(btn, this.context.page.settingsButton);
        }
        if (btn) {
            this.context.page.addEventListener(btn, 'click', () => this.openHotTickers());
        }
    }

    async openHotTickers() {
        if (!this.context.page._microGridManager) return;
        await this.context.page._microGridManager.openGroup(MICRO_GRID_GROUPS.pt_tools);
    }

    async afterMount() {
        if (this?.manager?.grid) {
            const unsub = this.manager.grid.grid$.pick('weight').onChanges(async (ch) => {
                if (!this.isActive) return;
                const w = ch.current.get('weight');
                if (this.kpi_elements) {
                    this.kpi_elements.forEach(elem => {
                        if (elem.getAttribute('data-weight')) elem.setAttribute('data-weight', w);
                    });
                }
                await this.refreshOverview();
            });
            this._permanentSubs.push(unsub);
        }
    }

    async onActivate() {
        await this.refreshOverview();
    }

    onResumeSubscriptions() {}

    async onCleanup() {
        // Kill permanent subs (the ones that bypass addSubscription)
        if (this._permanentSubs) {
            for (let i = 0; i < this._permanentSubs.length; i++) {
                const sub = this._permanentSubs[i];
                try {
                    if (typeof sub === 'function') sub();
                    else if (sub && typeof sub.unsubscribe === 'function') sub.unsubscribe();
                } catch (_) {}
            }
            this._permanentSubs.length = 0;
        }

        try { this.dynamicController.abort(); } catch (_) {}

        if (this._debouncedMetaRefresh?.cancel) this._debouncedMetaRefresh.cancel();
        this._debouncedMetaRefresh = null;

        try { this.bidSkew?.destroy(); } catch (_) {}
        try { this.askSkew?.destroy(); } catch (_) {}
        this.bidSkew = null;
        this.askSkew = null;

        try { this._stack?.destroy?.(); } catch (_) {}
        this._stack = null;

        try { this._headerRow?.remove(); } catch (_) {}
        this._headerRow = null;

        try { this.resizer?.unset(); } catch (_) {}
        this.resizer = null;

        for (let i = 0; i < this._ownedPills.length; i++) {
            try { this._ownedPills[i]?.destroy?.(); } catch (_) {}
        }
        this._ownedPills.length = 0;

        this.pivotEngine = null;
    }

    /* ────────── DOM ────────── */

    cacheDom() {
        this.pillContent = document.getElementById('overview-pill-section');
        this.flagContent = document.getElementById('overview-flag-section');
        this.currentSkewBid = document.getElementById('kpi-current-skew-bid');
        this.currentSkewAsk = document.getElementById('kpi-current-skew-ask');
        this.currentSkewDivider = document.getElementById('kpi-current-skew-divider');
        this.kpiGrid = document.getElementById('kpi-metrics-grid');
        this.kpi_elements = this.kpiGrid ? this.kpiGrid.querySelectorAll('.kpi-metric') : [];
    }

    onRender() {
        if (this._rendered) return;
        this._rendered = true;

        this.widgetDiv.innerHTML = `
<div class="overview-widget">
    <div class="widget-body">
    <div class="kpi-section-top">
    <div class="kpi-main-row">
    <div class="kpi-large"><span class="label">Notional</span><span class="value" data-agg="sumMetricWeight" data-metric="grossSize"></span></div>
<div class="kpi-large kpi-net"><span class="label">Net</span><span class="value" data-agg="sumMetricWeight" data-metric="netSize"></span></div>
<div class="kpi-large kpi-not-net"><span class="label">DV01</span><span class="value" data-agg="sumMetricWeight" data-metric="grossDv01"></span></div>
<div class="kpi-large kpi-net"><span class="label">Net DV01</span><span class="value" data-agg="sumMetricWeight" data-metric="netDv01"></span></div>
<div class="kpi-large"><span class="label">Count</span><span class="value" data-agg="count" data-metric="tnum"></span></div>
<div class="kpi-large"><span class="label">Current Skew</span>
    <div class="kpi-skew-wrapper">
        <div class="value" id="kpi-current-skew-bid"></div>
        <div class="value" id="kpi-current-skew-divider">/</div>
        <div class="value" id="kpi-current-skew-ask"></div>
    </div>
</div>
</div>
<div id="kpi-metrics-grid">
    <div class="kpi-secondary-row kpi-row">
        <span>Dur: <strong class="kpi-metric" data-agg="weightedAvg" data-metric="duration" data-weight="grossSize" data-sigfigs="2"></strong></span>
        <span>Liq: <strong class="kpi-metric" data-agg="weightedAvg" data-metric="liqScoreCombined" data-weight="grossSize" data-sigfigs="1"></strong></span>
        <span>Signal: <strong class="kpi-metric" data-agg="weightedAvg" data-metric="signalLiveStats" data-weight="grossSize" data-sigfigs="2"></strong></span>
    </div>
    <div class="kpi-tertiary-row kpi-row">
        <span>Axed: <strong class="kpi-metric" data-metric="isAxed" data-filter="MKT,AXED" data-agg="wavg" data-weight="grossSize" id="kpi-axed-pct">--</strong></span>
        <span>Anti: <strong class="kpi-metric" data-metric="isAxed" data-filter="ANTI" data-agg="wavg" data-weight="grossSize" id="kpi-anti-axed-pct">--</strong></span>
        <span>BSR: <strong class="kpi-metric" data-agg="percent" data-metric="firmAggBsrSize" data-weight="grossSize" data-sigfigs="0" data-format="percent" data-fill=0></strong></span>
</div>
<div class="bs-breakdown kpi-quad-row kpi-row">
    <span>Yld: <strong class="kpi-metric" data-agg="weightedAvg" data-metric="bvalMidYld" data-weight="grossSize" data-sigfigs="2" data-format="percent"></strong></span>
    <span>BSIFR: <strong class="kpi-metric" data-agg="percentOfWeight" data-metric="firmAggBsifrSize" data-weight="grossSize" data-sigfigs="0" data-format="NA" data-fill=0></strong></span>
<span>Algo: <strong class="kpi-metric" data-agg="percentOfWeight" data-metric="isInAlgoUniverse" data-weight="grossSize" data-sigfigs="0" data-format="percent" data-fill=0></strong></span>
</div>
</div>
</div>
<div class="distribution-section">
    <div class="distribution-controls">
        <span class="label">Distribution by (% of Total)</span>
        <select id="distribution-metric-select">
            <option value="deskAsset">Asset Class</option><option value="regionCountry">Country</option><option value="emProductType">Corp/Quasi/Sov</option><option value="yieldCurvePosition">Curve</option><option value="desigName">Desig</option><option value="maturityBucket">Maturity</option><option value="ratingCombined" selected>Rating</option><option value="ratingMnemonic">Rating Bucket</option><option value="quoteType">Req. QuoteType</option><option value="industrySector">Sector</option>
        </select>
    </div>
    <div class="rating-distribution-bar" id="distribution-bar-container"></div>
    <div class="dynamic-legend" id="distribution-legend-container"></div>
</div>
<div id="overview-pill-section"></div>
</div>
<div class="right-side">
    <div class="right-side-upper">
        <div class="right-side-content" id="portfolio-breakdown-container"></div>
    </div>
    <div class="right-side-middle">
        <div id="overview-string-section" class="overview-string-wrapper">
            <div id="overview-pill-content"></div>
            <div id="overview-flag-section"></div>
            <div id="hot-tickers-btn" class="toolbar-button" data-name="hotTickers">
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"><path fill="currentColor" d="M13.09 3.294c1.924.95 3.422 1.69 5.472.692a1 1 0 0 1 1.438.9v9.54a1 1 0 0 1-.562.9c-2.981 1.45-5.382.24-7.25-.701a39 39 0 0 0-.622-.31c-1.033-.497-1.887-.812-2.756-.77c-.76.036-1.672.357-2.81 1.396V21a1 1 0 1 1-2 0V4.971a1 1 0 0 1 .297-.71c1.522-1.506 2.967-2.185 4.417-2.255c1.407-.068 2.653.453 3.72.967q.337.163.655.32Z"/></svg>
            </div>
        </div>
    </div>
    <div class="right-side-lower">
        <span> </span>
        <button class="btn btn-ghost btn-sm" id="jump-to-pivot">Pivot<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24"><path fill="currentColor" d="M2 12a10 10 0 0 0 10 10a10 10 0 0 0 10-10A10 10 0 0 0 12 2A10 10 0 0 0 2 12m2 0a8 8 0 0 1 8-8a8 8 0 0 1 8 8a8 8 0 0 1-8 8a8 8 0 0 1-8-8m6 5l5-5l-5-5z"/></svg></button>
    </div>
</div>
</div>`;

        const s = this.context.page._metaStore.get('rfqSide');
        if (s === 'BWIC' || s === 'OWIC') {
            document.querySelectorAll('.kpi-net').forEach(e => e.style.display = 'none');
        } else {
            document.querySelectorAll('.kpi-not-net').forEach(e => e.style.display = 'none');
        }
    }

    /* ────────── Event binding ────────── */

    bindEvents() {
        const dropdown = document.getElementById('distribution-metric-select');
        if (dropdown) {
            this.context.page.addEventListener(dropdown, 'change', async () => {
                await this.refreshOverview();
            });
        }

        this._debouncedMetaRefresh = debounce(async () => {
            if (!this.isActive) return;
            await this.refreshOverview();
        }, 500);

        const unsub = this.context.page.portfolioMeta$.onChanges(this._debouncedMetaRefresh);
        this._permanentSubs.push(unsub);

        const jumpBtn = document.querySelector('#jump-to-pivot');
        const pivotTab = document.querySelector('.toolbar-button[data-tooltip="Pivot"]');
        if (jumpBtn && pivotTab) {
            this.context.page.addEventListener(jumpBtn, 'click', () => pivotTab.click());
        }
    }

    setupResizer() {
        const target = document.querySelector('.right-side');
        if (!target) return;
        this.resizer = interact(target).resizable({
            edges: { top: false, left: true, bottom: false, right: false },
            listeners: {
                move(event) {
                    const p = event.target.parentElement.getBoundingClientRect();
                    let xPos = 1 - (event.client.x - p.left) / (p.right - p.left);
                    xPos = Math.max(0.3, Math.min(0.7, xPos));
                    event.target.style.width = `${xPos * 100}%`;
                }
            }
        });
    }

    setupReactions() {
        const side = this.context.page._metaStore.get('rfqSide');

        if (side === 'BOWIC' || side === 'BWIC') {
            this.bidSkew = new ENumberFlow(this.currentSkewBid, {
                displayMode: 'number', showSign: 'always', duration: 200,
                minimumFractionDigits: 2, maximumFractionDigits: 2,
            });
        }

        if (side === 'BOWIC' || side === 'OWIC') {
            this.askSkew = new ENumberFlow(this.currentSkewAsk, {
                displayMode: 'number', showSign: 'always', duration: 200,
                minimumFractionDigits: 2, maximumFractionDigits: 2,
            });
        }

        // Store directly — NOT addSubscription
        const unsub = this.context.page.overallSkew$.onChanges(() => {
            if (!this.isActive) return;
            this.updateSkew(this.context.page.overallSkew$.asObject());
        });
        this._permanentSubs.push(unsub);

        // Also listen for ref market/side/quote type changes
        const refUnsub = this.context.page.activeRefSettingsWaterfall$.onChanges(() => {
            if (!this.isActive) return;
            this.refreshOverview();
        });
        this._permanentSubs.push(refUnsub);
    }

    /* ────────── Active weight ────────── */

    getActiveWeightKey() {
        return this.manager?.grid?.grid$?.get('weight') || 'grossSize';
    }

    /* ════════════════════════════════════════════════════════════════════════
       FLAGS & PILLS — Comprehensive set of at-a-glance indicators
       ════════════════════════════════════════════════════════════════════════ */

    setupFlags() {
        const pills = this.context.page.pillManager;
        if (!pills) return;

        const DETAILS = this.pillContent;
        const FLAGS = this.flagContent;
        const self = this;

        // Helper: create pill, track it, mount it, swallow errors
        const pill = (recipe, opts, container) => {
            pills.createPill(recipe, opts).then(p => {
                if (p) { self._ownedPills.push(p); p.mount(container); }
            }).catch(() => {});
        };

        /* ═══════ DETAIL PILLS (informational, green/teal/gray) ═══════ */

        pill('meta', {
            id: 'algoPct', columns: 'algoPct', label: 'Algo Eligible:', color: 'gray',
            valueFormatter: (v) => v ? this.formatNumber(v * 100, false, true, 0) : null,
        }, DETAILS);

        pill('meta', {
            id: 'creditRating', columns: 'creditRating', label: 'Credit Rating', type: 'portfolio', nullPolicy: 'showX',
        }, DETAILS);

        pill('meta', {
            id: 'bsrPct', columns: 'bsrPct', type: 'status',
            valueFormatter: (v) => v && v > 0.25 ? `Balance Sheet Reducing: ${this.formatNumber(v * 100, false, true, 0)}` : null,
        }, DETAILS);

        pill('meta', {
            id: 'axePct', columns: 'axePct', type: 'status',
            valueFormatter: (v) => v && v > 0.15 ? `Axed: ${this.formatNumber(v * 100, false, true, 0)}` : null,
        }, DETAILS);

        pill('custom', {
            id: 'liquidityBasket', type: 'portfolio',
            valueGetter: async (_data, p) => p.mgr.context.page._metaStore?.get('liqScoreCombined') ?? null,
            valueFormatter: (score) => {
                if (score == null) return null;
                if (score > 7) return 'Liquid Basket';
                if (score < 4) return 'Illiquid Basket';
                return null;
            },
            styleRules: [{ gt: 7, color: 'green', type: 'status' }, { lt: 4, color: 'red', type: 'warning' }],
        }, DETAILS);

        pill('custom', {
            id: 'clientSellingBucket', columns: 'rfqSide', source: 'store', type: 'portfolio',
            valueGetter: async (_data, p) => {
                const m = p.mgr.context.page._metaStore; if (!m) return null;
                const side = m.get('rfqSide');
                if (side !== 'BWIC' && side !== 'BOWIC') return null;
                let y = m.get('bwicYrsToMaturity') ?? m.get('yrsToMaturity');
                const b = maturityBucket(y);
                return b ? `Client Selling: ${b}` : null;
            },
        }, DETAILS);

        pill('custom', {
            id: 'clientBuyingBucket', columns: 'rfqSide', source: 'store', type: 'portfolio',
            valueGetter: async (_data, p) => {
                const m = p.mgr.context.page._metaStore; if (!m) return null;
                const side = m.get('rfqSide');
                if (side !== 'OWIC' && side !== 'BOWIC') return null;
                let y = m.get('owicYrsToMaturity') ?? m.get('yrsToMaturity');
                const b = maturityBucket(y);
                return b ? `Client Buying: ${b}` : null;
            },
        }, DETAILS);

        // HHI concentration pills
        for (const [id, label] of [['hhiDesigId', 'Desig'], ['hhiMaturityBucket', 'Maturity Bucket'], ['hhiRatingAssetClass', 'Asset Class']]) {
            pill('meta', {
                id, columns: id, label, type: 'portfolio',
                valueFormatter: (v) => hhiClass(v),
                styleRules: [{ gt: 0.7, color: 'red', type: 'warning' }, { lt: 0.2, color: 'green', type: 'portfolio' }],
            }, DETAILS);
        }

        // ETF overlap pills (18 ETFs)
        const ETF_LIST = [
            'inEtfAgg', 'inEtfLqd', 'inEtfHyg', 'inEtfJnk', 'inEtfEmb', 'inEtfSpsb', 'inEtfSpib',
            'inEtfSplb', 'inEtfVcst', 'inEtfVcit', 'inEtfVclt', 'inEtfSpab', 'inEtfIgib', 'inEtfIglb',
            'inEtfIgsb', 'inEtfIemb', 'inEtfUshy', 'inEtfSjnk',
        ];

        for (const etf of ETF_LIST) {
            pill('custom', {
                id: `pctInEtf-${etf}`, type: 'status',
                columns: () => [etf, self.getActiveWeightKey()],
                valueGetter: (data) => {
                    const wc = Object.keys(data).filter(x => !x.startsWith('inEtf'));
                    const ws = data[wc[0]]; const es = data[etf];
                    let num = 0, den = 0;
                    for (let i = 0; i < es.length; i++) {
                        const w = ws[i]; den += w;
                        if (es[i] != null && es[i] !== 0) num += w;
                    }
                    return den !== 0 ? num / den : null;
                },
                valueFormatter: (val, p) => {
                    const ticker = etf.replace('inEtf', '').toUpperCase();
                    let label = `${ticker} Overlap:`;
                    if (val < 0.25) {
                        label = `${ticker} Overlap:`;
                        p.root.classList.add('pill-warning');
                        p.root.classList.remove('pill-success');
                    } else if (val > 0.9) {
                        label = `${ticker} Overlap:`;
                        p.root.classList.remove('pill-warning');
                        p.root.classList.add('pill-success');
                    }
                    return `${label} ${Math.round(val * 100)}%`;
                },
                label: null,
                condition: (v, p) => {
                    const a = p.mgr.context.page._metaStore.get('assetClass')?.split(',');
                    if (a == null) return false;
                    return (
                        (a.includes('IG') && etf === 'inEtfLqd') ||
                        (a.includes('HY') && etf === 'inEtfHyg') ||
                        (a.includes('EM') && etf === 'inEtfEmb') ||
                        (v && v > 0.75)
                    );
                },
            }, DETAILS);
        }

        /* ═══════ FLAG PILLS (warnings / errors — mounted to FLAGS) ═══════ */

        // Inline count helper — pill.equals is NOT persisted by the Pill
        // constructor, so the count recipe's default test always falls back
        // to (v) => v != null.  We provide our own valueGetter to embed
        // the predicate directly inside the closure.
        const countBy = (pred) => async (data) => (data || []).filter(pred).length;

        // ────── Missing Data ──────

        pill('count', {
            id: 'pill_missing_descriptions', columns: 'description', type: 'error',
            valueGetter: countBy((v) => v == null || String(v).trim() === ''),
            valueFormatter: (count) => count > 0 ? `Missing Descriptions: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'description', (v) => v == null || String(v).trim() === '')));
                },
            },
            modal: (_e, p) => mkModal('Missing Descriptions', (eng) => maskByPredicate(eng, 'description', (v) => v == null || String(v).trim() === ''))(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_missing_dv01', columns: 'grossDv01', type: 'error',
            valueGetter: countBy((v) => v == null),
            valueFormatter: (count) => count > 0 ? `Missing DV01: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'grossDv01', (v) => v == null)));
                },
            },
            modal: (_e, p) => mkModal('Missing DV01', (eng) => maskByPredicate(eng, 'grossDv01', (v) => v == null), 'info', ['description', 'isin', 'grossSize'])(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_missing_desig', columns: 'desigName', type: 'error',
            valueGetter: countBy((v) => v == null || String(v).trim() === ''),
            valueFormatter: (count) => count > 0 ? `Missing Desig: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'desigName', (v) => v == null || String(v).trim() === ''), ['description']));
                },
            },
            modal: (_e, p) => mkModal('Missing Desig', (eng) => maskByPredicate(eng, 'desigName', (v) => v == null || String(v).trim() === ''))(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_missing_assigned', columns: 'assignedTrader', type: 'error',
            valueGetter: countBy((v) => v == null || String(v).trim() === ''),
            valueFormatter: (count) => count > 0 ? `Unassigned: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'assignedTrader', (v) => v == null || String(v).trim() === ''), ['description']));
                },
            },
            modal: (_e, p) => mkModal('Unassigned Bonds', (eng) => maskByPredicate(eng, 'assignedTrader', (v) => v == null || String(v).trim() === ''))(p),
        }, FLAGS);

        // ────── Trade Flags ──────

        pill('count', {
            id: 'pill_dnt_count', columns: 'isDnt', type: 'error',
            valueGetter: countBy((v) => coerceToBool(v)),
            valueFormatter: (count) => count > 0 ? `DNT Bonds: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'isDnt', (v) => coerceToBool(v)), ['description']));
                },
            },
            modal: (_e, p) => mkModal('DNT Bonds', (eng) => maskByPredicate(eng, 'isDnt', (v) => coerceToBool(v)), 'info', {
                isin: 'ISIN', description: 'Description', userSide: 'Side', grossSize: 'Size',
                desigName: 'Desig', assignedTrader: 'Assigned', dntComment: 'DNT', dntEventEnd: 'End Time', dntModifiedBy: 'Modified By',
            })(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_restrictedCode_count', columns: 'restrictedCode', type: 'error',
            valueGetter: countBy((v) => v != null),
            valueFormatter: (count) => count > 0 ? `RESTRICTED: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'restrictedCode', (v) => v != null), ['description']));
                },
            },
            modal: (_e, p) => mkModal('Restricted Bonds', (eng) => maskByPredicate(eng, 'restrictedCode', (v) => v != null), 'info', {
                isin: 'ISIN', description: 'Description', userSide: 'Side', grossSize: 'Size',
                desigName: 'Desig', assignedTrader: 'Assigned', restrictedCode: 'Code', restrictionTier: 'Tier', restrictionTime: 'Time',
            })(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_contains_default', columns: 'isInDefault', type: 'error',
            valueGetter: countBy((v) => +v === 1),
            valueFormatter: (count) => count ? 'Contains Default' : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'isInDefault', (v) => +v === 1)));
                },
            },
            modal: (_e, p) => mkModal('Contains Default', (eng) => maskByPredicate(eng, 'isInDefault', (v) => +v === 1))(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_contains_new_issue', columns: 'isNewIssue', type: 'warning',
            valueGetter: countBy((v) => +v === 1),
            valueFormatter: (count) => count ? 'Contains New Issue' : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'isNewIssue', (v) => +v === 1)));
                },
            },
            modal: (_e, p) => mkModal('Contains New Issue', (eng) => maskByPredicate(eng, 'isNewIssue', (v) => +v === 1), 'info', ['description', 'isin', 'issueDate', 'isWhenIssued', 'announcementDate'])(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_contains_muni', columns: 'isMuni', type: 'warning',
            valueGetter: countBy((v) => +v === 1),
            valueFormatter: (count) => count ? 'Contains Muni' : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'isMuni', (v) => +v === 1)));
                },
            },
            modal: (_e, p) => mkModal('Contains Muni', (eng) => maskByPredicate(eng, 'isMuni', (v) => +v === 1))(p),
        }, FLAGS);

        // ────── Market / Benchmark ──────

        pill('notDistinct', {
            id: 'quoteType', columns: 'quoteType', type: 'warning', label: 'Multi QT:',
            valueFormatter: (val) => Array.from(val?.seenValues || []).toSorted().join(', '),
            nullPolicy: 'hide',
        }, FLAGS);

        pill('distinct', {
            id: 'currency', columns: 'currency', type: 'warning',
            condition: (val) => val != null && (val.distinct > 1 || !val.seenValues.includes('USD')),
            valueFormatter: (val) => {
                if (val.distinct > 1) return `Multi Currency: ${Array.from(val.seenValues).toSorted().join(', ')}`;
                if (!val.seenValues.includes('USD')) return `Non-USD: ${Array.from(val.seenValues).toSorted().join(', ')}`;
                return null;
            },
        }, FLAGS);

        pill('meta', {
            id: 'isCrossed', columns: 'isCrossed', type: 'warning',
            valueFormatter: (v) => v ? 'TSY CROSSED' : null,
        }, FLAGS);

        pill('meta', {
            id: 'removedCount', columns: 'removedCount', type: 'warning',
            valueFormatter: (v) => v > 0 ? `Removed bonds: ${v | 0}` : null,
        }, FLAGS);

        pill('custom', {
            id: 'pill_rfq_bmk_mismatch', columns: ['rfqBenchmarkIsin', 'benchmarkIsin'], type: 'error',
            valueGetter: async (data) => {
                const left = data?.rfqBenchmarkIsin || [];
                const right = data?.benchmarkIsin || [];
                let count = 0;
                for (let i = 0; i < Math.max(left.length, right.length); i++) {
                    if (_isMissingBenchmark(left[i]) || _isMissingBenchmark(right[i])) continue;
                    if (left[i] !== right[i]) count++;
                }
                return count > 0 ? count : null;
            },
            valueFormatter: (count) => count ? 'RFQ Benchmark Mismatch' : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByMismatch(eng, 'rfqBenchmarkIsin', 'benchmarkIsin')));
                },
            },
            modal: (_e, p) => mkModal('RFQ Benchmark Mismatch', (eng) => maskByMismatch(eng, 'rfqBenchmarkIsin', 'benchmarkIsin'), 'error')(p),
        }, FLAGS);

        pill('custom', {
            id: 'pill_bval_mismatch', columns: ['bvalBenchmarkIsin', 'benchmarkIsin'], type: 'error',
            valueGetter: async (data) => {
                const left = data?.bvalBenchmarkIsin || [];
                const right = data?.benchmarkIsin || [];
                let count = 0;
                for (let i = 0; i < Math.max(left.length, right.length); i++) {
                    if (_isMissingBenchmark(left[i]) || _isMissingBenchmark(right[i])) continue;
                    if (left[i] !== right[i]) count++;
                }
                return count > 0 ? count : null;
            },
            valueFormatter: (count) => count > 0 ? `BVAL Mismatch: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByMismatch(eng, 'bvalBenchmarkIsin', 'benchmarkIsin'), ['description']));
                },
            },
            modal: (_e, p) => mkModal('BVAL Benchmark Mismatch', (eng) => maskByMismatch(eng, 'bvalBenchmarkIsin', 'benchmarkIsin'), 'info', {
                isin: 'ISIN', description: 'Description', bvalBenchmarkTenor: 'BVAL BM', bvalBenchmarkIsin: 'BVAL ISIN',
                benchmarkName: 'PT BM', benchmarkIsin: 'PT Bench ISIN', bvalBenchYldDiff: 'Approx. BPS Diff',
            }, 'Note: [approx bps diff] = ( [live yld of BVAL tenor] - [live yld of PT tenor] ) * 100', ['benchmarkName', 'description'])(p),
        }, FLAGS);

        pill('custom', {
            id: 'pill_macp_mismatch', columns: ['macpBenchmarkIsin', 'benchmarkIsin'], type: 'error',
            valueGetter: async (data) => {
                const left = data?.macpBenchmarkIsin || [];
                const right = data?.benchmarkIsin || [];
                let count = 0;
                for (let i = 0; i < Math.max(left.length, right.length); i++) {
                    if (_isMissingBenchmark(left[i]) || _isMissingBenchmark(right[i])) continue;
                    if (left[i] !== right[i]) count++;
                }
                return count > 0 ? count : null;
            },
            valueFormatter: (count) => count > 0 ? `CP+ Mismatch: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByMismatch(eng, 'macpBenchmarkIsin', 'benchmarkIsin'), ['description']));
                },
            },
            modal: (_e, p) => mkModal('CP+ Benchmark Mismatch', (eng) => maskByMismatch(eng, 'macpBenchmarkIsin', 'benchmarkIsin'), 'info', {
                isin: 'ISIN', description: 'Description', macpBenchmarkTenor: 'CP+ BM', macpBenchmarkIsin: 'CP+ ISIN',
                benchmarkName: 'PT BM', benchmarkIsin: 'PT Bench ISIN',
            }, null, ['benchmarkName', 'description'])(p),
        }, FLAGS);

        // ────── Size / Settlement ──────

        pill('count', {
            id: 'pill_blocks_over_10m', columns: 'grossSize', type: 'warning',
            valueGetter: countBy((v) => Math.abs(+v || 0) > 10_000_000),
            valueFormatter: (count) => count ? `Blocks: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'grossSize', (v) => Math.abs(+v || 0) > 10_000_000)));
                },
            },
            modal: (_e, p) => mkModal('Blocks > 10mm', (eng) => maskByPredicate(eng, 'grossSize', (v) => Math.abs(+v || 0) > 10_000_000), 'info', ['description', 'isin', 'userSide', 'grossSize'])(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_stub', columns: 'isStub', type: 'warning',
            valueGetter: countBy((v) => v === 1),
            valueFormatter: (count) => count > 0 ? `Stub Sizes: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'isStub', (v) => v === 1), ['description']));
                },
            },
            modal: (_e, p) => mkModal('Stub Sizes', (eng) => maskByPredicate(eng, 'isStub', (v) => v === 1), 'info', ['isin', 'description', 'grossSize'])(p),
        }, FLAGS);

        pill('count', {
            id: 'pill_non_standard_settle', columns: 'daysToSettle', type: 'warning',
            valueGetter: countBy((v) => (+v || 0) > 2),
            valueFormatter: (count) => count ? `Non-Standard Settle: ${count}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'daysToSettle', (v) => (+v || 0) > 2), ['description', 'daysToSettle']));
                },
            },
            modal: (_e, p) => mkModal('Non-Standard Settle', (eng) => maskByPredicate(eng, 'daysToSettle', (v) => (+v || 0) > 2), 'info', ['description', 'isin', 'daysToSettle', 'isNewIssue'])(p),
        }, FLAGS);

        // ────── Claimed ──────

        pill('custom', {
            id: 'pill_claimed_bonds_sum', columns: ['claimed'], type: 'portfolio',
            valueGetter: async (data) => {
                const arr = Array.isArray(data) ? data : Object.values(data || {})[0] || [];
                let sum = 0; let any = false;
                const mask = new Array(arr.length);
                for (let i = 0; i < arr.length; i++) {
                    const v = arr[i];
                    const n = (v == null || v === '') ? 0 : (v ? 1 : 0);
                    if (n > 0) { any = true; sum += n; mask[i] = true; } else mask[i] = false;
                }
                return any ? { sum, mask } : null;
            },
            valueFormatter: (obj) => obj ? `Claimed Bonds: ${Math.round(obj.sum)}` : null,
            tooltip: true,
            tooltipConfig: {
                content: () => {
                    const eng = pills.engine;
                    if (!eng) return '';
                    return tooltipFromLines(buildLines(eng, maskByPredicate(eng, 'claimed', (v) => v != null && v !== '' && v)));
                },
            },
            modal: (_e, p) => {
                const eng = p.mgr.engine;
                if (!eng) return;
                const mask = maskByPredicate(eng, 'claimed', (v) => v != null && v !== '' && v);
                let sum = 0;
                for (let i = 0; i < mask.length; i++) if (mask[i]) sum++;
                const lines = buildLines(eng, mask);
                const payload = modalPayload(`Claimed Bonds: ${sum}`, lines, ['description', 'isin'], 'portfolio');
                p.mgr.createInfoModal(payload.title, payload.content, payload.type);
            },
        }, FLAGS);
    }

    /* ════════════════════════════════════════════════════════════════════════
       CORE REFRESH — single data pass, zero redundant materialization
       ════════════════════════════════════════════════════════════════════════ */

    _getColumnsToEnsure() {
        const cols = [...REQUIRED_COLUMNS];
        const distCol = document.getElementById('distribution-metric-select')?.value;
        if (distCol) cols.push(distCol);
        return Array.from(new Set(cols));
    }

    async refreshOverview() {
        if (!this.manager?.grid) return;
        if (!this.isActive) return;

        const engine = this.manager.grid.engine;
        if (!engine) return;
        const n = engine.numRows() | 0;

        if (n === 0) { this.clearWidget(); return; }

        // ──── SINGLE materialization pass ────
        // This is the ONLY place we call getAllRows in the entire refresh cycle.
        const cols = this._getColumnsToEnsure();
        const portfolio = engine.getAllRows({ columns: cols });

        // ──── Compute totals in a single scan ────
        let totalGrossSize = 0, totalDv01 = 0, totalRisk = 0;
        for (let i = 0; i < portfolio.length; i++) {
            totalGrossSize += Math.abs(portfolio[i].grossSize || 0);
            totalDv01 += Math.abs(portfolio[i].grossDv01 || 0);
            totalRisk += Math.abs(portfolio[i]._normalizedRisk || 0)
        }

        const weightKey = this.getActiveWeightKey();
        const totalWeight = weightKey === 'grossSize' ? totalGrossSize : (weightKey === 'grossDv01' ? totalDv01 : totalRisk);

        const totals = { totalGrossSize, totalDv01, totalRisk, totalCount: portfolio.length, totalWeight, weightKey };

        // ──── Fan out to sub-renderers (all receive the same data) ────
        this.updateAllKpis(portfolio, totals);
        this.renderDistributionBar(portfolio, totals);
        await this.renderRightPanel(portfolio, totals);
    }

    clearWidget() {
        this.widgetDiv?.querySelectorAll('[data-agg], .value, strong').forEach(el => el.textContent = '--');
        const barC = document.getElementById('distribution-bar-container');
        const legC = document.getElementById('distribution-legend-container');
        const brkC = document.getElementById('portfolio-breakdown-container');
        if (barC) barC.innerHTML = '';
        if (legC) legC.innerHTML = '';
        if (brkC) brkC.innerHTML = '<div class="empty-state">No data in portfolio</div>';
    }

    /* ────────── KPI computation — single pass over all rows ────────── */

    updateAllKpis(portfolio, totals) {
        const { totalGrossSize } = totals;
        const kpiElements = Array.from(this.widgetDiv.querySelectorAll('[data-agg]'));

        // Build KPI specs from DOM attributes
        const kpiSpecs = [];
        for (const el of kpiElements) {
            const { metric, agg, sigfigs = '1', format = 'number', fill, weight } = el.dataset;
            if (format === 'NA' || !metric) {
                el.textContent = '--';
                continue;
            }
            kpiSpecs.push({ el, metric, agg, passWeight: weight || null, format, sigfigs: parseInt(sigfigs), fill });
        }

        // ──── Single pass over all rows for all KPIs simultaneously ────
        const accs = kpiSpecs.map(() => ({ sum: 0, weightSum: 0, metricSum: 0, count: 0 }));

        for (let i = 0; i < portfolio.length; i++) {
            const row = portfolio[i];
            for (let k = 0; k < kpiSpecs.length; k++) {
                const spec = kpiSpecs[k];
                const acc = accs[k];

                let m = coerceToNumeric(row?.[spec.metric], { onNaN: spec.fill != null ? Number(spec.fill) : undefined });
                if (m == null || isNaN(m)) continue;

                // BSR special handling
                if (spec.passWeight === 'grossDv01' && spec.metric === 'firmAggBsrSize') {
                    m = (row['unitDv01'] || 0) / 10_000 * m;
                }

                const hasWeight = spec.passWeight && spec.passWeight !== '__count__';
                const w = coerceToNumeric(hasWeight ? row[spec.passWeight] : 1, { onNaN: null, emptyStringIsZero: false });
                if (w == null || isNaN(w)) continue;

                acc.sum += m * w;
                acc.weightSum += w;
                acc.metricSum += m;
                acc.count++;
            }
        }

        // Resolve and render
        for (let k = 0; k < kpiSpecs.length; k++) {
            const spec = kpiSpecs[k];
            const acc = accs[k];
            let value;

            switch (spec.agg) {
                case 'weightedAvg':
                case 'wavg':
                    value = acc.weightSum ? acc.sum / acc.weightSum : null; break;
                case 'sumMetricWeight':
                    value = acc.metricSum; break;
                case 'count':
                    value = acc.count; break;
                case 'percent':
                    value = acc.weightSum > 0 ? (acc.metricSum / acc.weightSum) * 100 : 0; break;
                case 'percentOfWeight':
                    value = acc.weightSum > 0 ? (acc.sum / acc.weightSum) * 100 : 0; break;
                default:
                    value = acc.sum;
            }

            const isPct = spec.format === 'percent' || spec.agg === 'percent';
            spec.el.textContent = (value != null && !isNaN(value))
                ? this.formatNumber(value, !isPct, isPct, spec.sigfigs)
                : '--';
        }

        // Axed / anti-axed (needs direction columns)
        this._updateAxedFromRows(portfolio, totalGrossSize);
    }

    _updateAxedFromRows(rows, totalGrossSize) {
        let axedSize = 0, antiAxedSize = 0;
        for (let i = 0; i < rows.length; i++) {
            const bond = rows[i];
            if (bond.axeDirection && bond.direction) {
                const size = Math.abs(bond.grossSize || 0);
                if (bond.direction.toLowerCase() === bond.axeDirection.toLowerCase()) axedSize += size;
                else antiAxedSize += size;
            }
        }
        const axedEl = document.getElementById('kpi-axed-pct');
        const antiEl = document.getElementById('kpi-anti-axed-pct');
        if (axedEl) axedEl.textContent = this.formatNumber(totalGrossSize > 0 ? (axedSize / totalGrossSize) * 100 : 0, false, true, 0);
        if (antiEl) antiEl.textContent = this.formatNumber(totalGrossSize > 0 ? (antiAxedSize / totalGrossSize) * 100 : 0, false, true, 0);
    }

    /* ────────── Distribution bar ────────── */

    renderDistributionBar(portfolioData, totals) {
        const metricKey = document.getElementById('distribution-metric-select')?.value;
        if (!metricKey) return;

        const { totalWeight, weightKey } = totals;
        const barContainer = document.getElementById('distribution-bar-container');
        const legendContainer = document.getElementById('distribution-legend-container');
        if (!barContainer || !legendContainer) return;
        barContainer.innerHTML = '';
        legendContainer.innerHTML = '';
        if (totalWeight === 0) return;

        const distribution = {};
        for (let i = 0; i < portfolioData.length; i++) {
            const bond = portfolioData[i];
            const key = bond[metricKey] || 'N/A';
            let w = 0;
            if (weightKey === '__count__') w = 1;
            else if (weightKey === 'grossSize') w = Math.abs(bond.grossSize || 0);
            else if (weightKey === 'grossDv01') w = Math.abs(bond.grossDv01 || 0);
            else if (weightKey === '_normalizedRisk') w = Math.abs(bond._normalizedRisk || 0);
            distribution[key] = (distribution[key] || 0) + w;
        }

        let sorted = Object.entries(distribution);
        const custom = SORT_ORDERS[metricKey];
        if (custom) sorted.sort(([a], [b]) => (custom.indexOf(a) === -1 ? 999 : custom.indexOf(a)) - (custom.indexOf(b) === -1 ? 999 : custom.indexOf(b)));
        else sorted.sort(([, a], [, b]) => b - a);

        const colors = generateGradientChroma('#8bc1bb', '#3e3649', sorted.length);
        const colorMap = new Map();

        for (let i = 0; i < sorted.length; i++) {
            const [category, value] = sorted[i];
            const pct = (value / totalWeight) * 100;
            if (pct < 0.1) continue;
            const color = colors[i % colors.length];
            colorMap.set(category, color);

            const segment = document.createElement('div');
            segment.className = 'rating-segment tooltip tooltip-top';
            segment.style.width = `${pct}%`;
            segment.style.backgroundColor = color;
            segment.setAttribute('data-tooltip', `${category}: ${pct.toFixed(1)}%`);
            barContainer.appendChild(segment);
        }

        const legendItems = sorted.slice(0, 5);
        for (const [category, value] of legendItems) {
            const pct = (value / totalWeight) * 100;
            if (pct < 0.1) continue;
            const color = colorMap.get(category) || 'gray';
            const item = document.createElement('div');
            item.className = 'legend-item';
            item.innerHTML = `<div class="legend-color-box" style="background-color: ${color};"></div><span>${category}: ${pct.toFixed(1)}%</span>`;
            legendContainer.appendChild(item);
        }
    }

    /* ────────── Right panel (cards) ────────── */

    async renderRightPanel(portfolio, totals) {
        const container = document.getElementById('portfolio-breakdown-container');
        if (!container) return;

        if (!this._stack) {
            container.innerHTML = '';
            this._stack = new InfoCardStack(container, { showNav: true, pauseOnHover: true, autoCycleMs: false });
        }

        // 1) Liquidity recs + skew header strip
        const row = document.createElement('div');
        row.className = 'breakdown-row-1';
        const charges = this._calculateLiquidityCharges(portfolio);
        row.appendChild(this._createLiquidityCard(charges));
        row.appendChild(this._createSkewCard());

        if (!this._headerRow) {
            container.parentElement?.insertBefore(row, container);
            this._headerRow = row;
        } else {
            this._headerRow.replaceWith(row);
            this._headerRow = row;
        }

        // 2) Stack cards
        const weightKey = this.getActiveWeightKey();

        // Pivot-based breakdown
        if (this.pivotEngine) {
            try {
                const em = await this.pivotData('emProductType', weightKey, ['SUM', 'PERCENT_OF_COL_SUM']);
                const emCols = ['Asset Type', `${weightKey}_SUM`, `${weightKey}_PERCENT_OF_COL_SUM`];
                const emCardEl = this._createBreakdownCard('Asset Type Breakdown', em.rows || [], emCols, { limit: 8 });
                this._upsertCard('bdn_emProductType', 'Asset Type Breakdown', 'raw_html', emCardEl);
            } catch {}
        }

        // Histogram + scatter from the already-fetched portfolio (NO extra engine call)
        const buckets = new Array(10).fill(0);
        const points = [];
        let maxW = 0;
        for (let i = 0; i < portfolio.length; i++) {
            const r = portfolio[i];
            const liq = Math.min(10, Math.max(1, Math.round(+r.liqScoreCombined || 0)));
            const w = weightKey === '__count__' ? 1 : Math.abs(+r[weightKey] || 0);
            if (liq >= 1) buckets[liq - 1] += w;

            const x = +r.duration; const y = +r.liqScoreCombined;
            if (isFinite(x) && isFinite(y)) {
                maxW = Math.max(maxW, w);
                points.push({ x, y, _w: w });
            }
        }

        this._upsertCard('hist_liq', 'Liquidity Score Histogram', 'histogram', {
            labels: Array.from({ length: 10 }, (_, i) => `${i + 1}`),
            values: buckets,
        });

        this._upsertCard('scatter_liq_dur', 'Liq vs Duration', 'scatter', {
            points: points.map(p => ({ x: p.x, y: p.y, r: 2 + (maxW ? 6 * Math.sqrt(p._w / maxW) : 0) })),
        });

        // Bar: exposures by Curve
        if (this.pivotEngine) {
            try {
                const res = await this.pivotData('yieldCurvePosition', weightKey, ['SUM']);
                const curveRows = res.rows || [];
                this._upsertCard('bar_curve', 'Exposures by Curve', 'vertical-bar', {
                    labels: curveRows.map(r => r[0] == null ? 'N/A' : String(r[0])),
                    values: curveRows.map(r => +r[1] || 0),
                });
            } catch {}
        }
    }

    _upsertCard(name, title, type, data, options) {
        if (!this._stack) return;
        const existing = this._stack.cards.find(c => c.name === name);
        if (existing) existing.update(data, options);
        else this._stack.registerCard(name, title, type, data, undefined, '', options);
    }

    /* ────────── Liquidity charges (from already-fetched data) ────────── */

    _calculateLiquidityCharges(portfolio) {
        let pxChargeTotal = 0, pxDv01Total = 0, bpsChargeTotal = 0, bpsDv01Total = 0;

        for (let i = 0; i < portfolio.length; i++) {
            const bond = portfolio[i];
            const score = bond.liqScoreCombined;
            const quoteType = bond.QT;
            const dv01 = Math.abs(bond.grossDv01 || 0);
            let charge = 0;

            if (score <= 4) charge = quoteType === 'PX' ? 0.50 : 3.0;
            else if (score <= 6) charge = quoteType === 'PX' ? 0.25 : 1.0;

            if (charge > 0) {
                if (quoteType === 'PX') { pxChargeTotal += charge * dv01; pxDv01Total += dv01; }
                else { bpsChargeTotal += charge * dv01; bpsDv01Total += dv01; }
            }
        }

        return {
            pxCharge: pxDv01Total > 0 ? pxChargeTotal / pxDv01Total : 0,
            bpsCharge: bpsDv01Total > 0 ? bpsChargeTotal / bpsDv01Total : 0,
            pxDv01Total, pxChargeTotal, bpsChargeTotal, bpsDv01Total,
        };
    }

    _createLiquidityCard(liq) {
        const wrapper = document.createElement('div');
        wrapper.className = 'info-card';

        const title = document.createElement('div');
        title.className = 'info-card-title';
        title.textContent = 'Liquidity Cost Recs';
        wrapper.appendChild(title);

        const addRow = (label, value) => {
            const r = document.createElement('div'); r.className = 'liq-cost-row';
            const l = document.createElement('span'); l.textContent = label;
            const v = document.createElement('span'); v.className = 'liq-cost-value'; v.textContent = value;
            r.appendChild(l); r.appendChild(v); wrapper.appendChild(r);
        };

        if (liq.pxDv01Total > 0) addRow('$PX (c)', (liq.pxCharge || 0).toFixed(2));
        if (liq.bpsDv01Total > 0) addRow('SPD (bps)', (liq.bpsCharge || 0).toFixed(1));
        if (!liq.pxDv01Total && !liq.bpsDv01Total) addRow('No eligible quotes', '--');

        return wrapper;
    }

    _createSkewCard() {
        const wrapper = document.createElement('div');
        wrapper.className = 'info-card';
        wrapper.innerHTML = `<div class="info-card-title">Recommended Skew</div><div class="skew-row"><span>---</span></div>`;
return wrapper;
}

_createBreakdownCard(title, rows, columns, opts = {}) {
    const {
        limit = 7, gradient = true,
        gradientStart = '#8bc1bb', gradientEnd = '#3e3649',
        percentIndex = Math.max(0, columns.findIndex(c => /PERCENT/i.test(c))),
        rawIndex = Math.max(0, columns.findIndex(c => /_SUM$|_COUNT$|_MEAN$|_MEDIAN$/i.test(c))),
        labelIndex = 0,
    } = opts;

    const list = document.createElement('div');
    list.className = 'breakdown-list';

    const sorted = rows
    .map(r => ({ label: r[labelIndex], pct: +r[percentIndex] || 0, raw: +r[rawIndex] || 0 }))
     .sort((a, b) => b.pct - a.pct)
     .slice(0, limit);

    const colors = gradient ? generateGradientChroma(gradientStart, gradientEnd, Math.max(1, sorted.length)) : Array(sorted.length).fill('var(--teal-500)');
    const maxPct = Math.max(...sorted.map(d => d.pct), 0.000001);

    for (let i = 0; i < sorted.length; i++) {
        const item = sorted[i];
        const pct100 = item.pct * 100;
        const row = document.createElement('div');
        row.className = 'breakdown-row tooltip tooltip-top';

        const text = document.createElement('div'); text.className = 'breakdown-text';
        const label = document.createElement('div'); label.className = 'breakdown-label';
        label.textContent = item.label == null ? 'N/A' : String(item.label);
        const value = document.createElement('div'); value.className = 'breakdown-value';
        value.textContent = `${pct100.toFixed(1)}%`;

        const barBg = document.createElement('div'); barBg.className = 'breakdown-bar-bg';
        const barFg = document.createElement('div'); barFg.className = 'breakdown-bar-fg';
        barFg.style.width = `${Math.max(2, (item.pct / maxPct) * 100)}%`;
        barFg.style.backgroundColor = colors[i];

        row.setAttribute('data-tooltip', `${NumberFormatter.formatNumber(item.raw, { sigFigs: { global: 2 } })} • ${pct100.toFixed(1)}%`);
        barBg.appendChild(barFg); text.appendChild(label); text.appendChild(value);
        row.append(text); row.appendChild(barBg); list.appendChild(row);
    }

    return list;
}

/* ────────── Pivot data helpers ────────── */

async pivotData(groups, weightKey, aggs) {
    const pe = this.pivotEngine;
    if (!pe) return { rows: [] };
    groups = Array.isArray(groups) ? groups : [groups];
    aggs = Array.isArray(aggs) ? aggs : [aggs];
    const arrowTbl = this.engine.asTable();
    return pe.compute(arrowTbl, { groupBy: groups, aggregations: aggs.map(a => ({ [weightKey]: a })) });
}

getTopConcentrations(portfolio, key, totalSize, count) {
    if (!portfolio.length || totalSize === 0) return [];
    const groups = {};
    for (let i = 0; i < portfolio.length; i++) {
        const gk = portfolio[i][key] || 'N/A';
        groups[gk] = (groups[gk] || 0) + Math.abs(portfolio[i].grossSize || 0);
    }
    return Object.entries(groups)
                  .sort(([, a], [, b]) => b - a)
                  .slice(0, count)
                  .map(([name, value]) => ({ name, value, pct: (value / totalSize) * 100 }));
}

/* ────────── Skew display ────────── */

updateSkew(s) {
    if (this.askSkew && s?.ask) {
        const unit = s.ask.unit;
        if (this.askSkew.config.suffix !== unit) this.askSkew.setConfig({ suffix: unit });
        const v = roundToNumeric(s.ask.value, 2);
        if (this.askSkew.currentValue !== v) this.askSkew.update(v);
    } else if (this.bidSkew && s?.bid) {
        const unit = s.bid?.unit;
        if (this.bidSkew.config.suffix !== unit) this.bidSkew.setConfig({ suffix: unit });
    }
    if (this.bidSkew && s?.bid) {
        const v = roundToNumeric(s.bid.value, 2);
        if (this.bidSkew.currentValue !== v) this.bidSkew.update(v);
    }
}

/* ────────── Formatting ────────── */

formatNumber(n, isCurrency = true, isPercent = false, decimals = 1, prefix = '', postfix = '') {
    if (n == null || typeof n === 'undefined' || isNaN(n)) return '--';
    return NumberFormatter.formatNumber(n, {
        prefix,
        postfix: isPercent ? '%' : postfix,
        sigFigs: { global: decimals },
    });
}

/** Compute a KPI from pre-fetched rows (public API for generateSummaryString). */
_computeFromRows(rows, metric, weight = null, output = 'weightedAvg', missing = {}) {
    let totalWeight = 0, totalMetricWeight = 0, totalMetric = 0, count = 0;
    const hasWeight = weight != null && weight !== '__count__';

    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        let m = coerceToNumeric(row?.[metric], { onNaN: missing?.value });
        if (m == null || isNaN(m)) continue;
        if (weight === 'grossDv01' && metric === 'firmAggBsrSize') m = (row['unitDv01'] || 0) / 10_000 * m;
        const w = coerceToNumeric(hasWeight ? row[weight] : 1, { onNaN: null, emptyStringIsZero: false });
        if (w == null || isNaN(w)) continue;
        totalWeight += w; totalMetricWeight += m * w; totalMetric += m; count++;
    }

    switch (output) {
        case 'percentOfWeight': return totalWeight > 0 ? (totalMetricWeight / totalWeight) * 100 : 0;
        case 'percent': return totalWeight > 0 ? (totalMetric / totalWeight) * 100 : 0;
        case 'sumWeight': return totalWeight;
        case 'count': return count;
        case 'weightedAvg': return totalWeight ? totalMetricWeight / totalWeight : null;
        case 'sumMetricWeight': return totalMetric;
        default: return null;
    }
}

async computeValue(portfolio, metric, weight = null, output = 'weightedAvg', missing = {}) {
    const cols = [metric];
    if (weight && weight !== '__count__') cols.push(weight);
    if (weight === 'grossDv01') cols.push('unitDv01');
    const rows = this.engine.getAllRows({ columns: cols });
    return this._computeFromRows(rows, metric, weight, output, missing);
}

/* ────────── Summary string ────────── */

getShortString() {
    const html = formatPortfolioSummary(this.context.page.portfolioMeta$.asObject());
    const txt = html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
    return `${txt}\n${this.getLinkString()}`;
}

getLinkString() {
    const client = clean_camel(this.context.page._metaStore.get('client') ?? '').toUpperCase();
    let sub = client.substring(0, 10);
    if (sub.endsWith('ASS')) sub += client.substring(10);
    return window.location.href.split('?')[0] + `?name=${sub}`;
}

getKeyString() {
    return window.location.href.split('/pt/')[1]?.split('?')[0] || '';
}

async generateSummaryString() {
    const portfolio = this.engine.getAllRows({ columns: REQUIRED_COLUMNS });
    if (portfolio.length === 0) return 'Empty portfolio.';
    const weightKey = this.getActiveWeightKey();

    const fNum = (val, dec = 0, prefix = '', postfix = '') => this.formatNumber(val, true, false, dec, prefix, postfix);
    const fPct = (val, dec = 0) => this.formatNumber(val, false, true, dec);

    const grossSize = this._computeFromRows(portfolio, 'grossSize', null, 'sumMetricWeight');
    const netDv01 = this._computeFromRows(portfolio, 'netDv01', null, 'sumMetricWeight');
    const waDur = this._computeFromRows(portfolio, 'duration', weightKey, 'weightedAvg');
    const waYld = this._computeFromRows(portfolio, 'bvalMidYld', weightKey, 'weightedAvg');
    const waLiq = this._computeFromRows(portfolio, 'liqScoreCombined', weightKey, 'weightedAvg');
    const topSectors = this.getTopConcentrations(portfolio, 'industrySector', grossSize, 3);
    const topCountries = this.getTopConcentrations(portfolio, 'regionCountry', grossSize, 3);
    const bsrPct = this._computeFromRows(portfolio, 'firmAggBsrSize', weightKey, 'percent');
    const wwPct = this._computeFromRows(portfolio, 'firmAggBsiSize', weightKey, 'percent');
    const liqCharges = this._calculateLiquidityCharges(portfolio);
    const fmt = (items) => items.map(i => `${i.name} ${fPct(i.pct, 0)}`).join(' | ');

    return `${fNum(grossSize)} Gross | ${portfolio.length} Lines
-------------------
DV01: ${fNum(netDv01)} | W.A. Dur: ${fNum(waDur, 2)} | W.A. Yld: ${fPct(waYld, 2)} | W.A. Liq: ${fNum(waLiq, 1)}
Top Sectors: ${fmt(topSectors)}
Top Countries: ${fmt(topCountries)}
BSR: ${fPct(bsrPct, 0)} | WW: ${fPct(wwPct, 0)}
Liq. Cost Recs: ${fNum(liqCharges.pxCharge, 2, '', 'c')} (PX) | ${fNum(liqCharges.bpsCharge, 1, '', 'bps')} (Other)`;
}
}

/* ═══════════════════════════════════════════════════════════════════════════
   SUMMARY STRING FORMATTER (module-level, used by getShortString)
   ═══════════════════════════════════════════════════════════════════════════ */

function formatPortfolioSummary(portfolioMeta$) {
    const meta = normalizeMeta(portfolioMeta$);
    const side = (meta.rfqSide || '').toUpperCase();
    const nowMs = Date.now();
    const status = pickStatus(meta);
    const dueIso = meta.dueTimeEt || meta.dueTime || null;
    const clientName = nonEmpty(meta.client) || '-';
    const traderName = nonEmpty(meta.clientTraderName) || nonEmpty(meta.clientTraderUsername) || '-';
    const dirFlags = buildDirFlags(meta);
    const liveStates = new Set(['LIVE', 'PENDING', 'NEW']);
    const statusOrDue = liveStates.has(status) && dueIso ? `[Due ${fmtDue(nowMs, dueIso)}]` : `[${status || '-'}]`;

    const overall = { gs: num(meta.grossSize), ns: num(meta.netSize), gdv: num(meta.grossDv01), ndv: num(meta.netDv01), ct: int(meta.count), liq: preferNumber(meta.liqScoreCombined, meta.liqScoreBase), rating: nonEmpty(meta.creditRating) || '-', dur: preferNumber(meta.duration, 0), signal: num(meta.signalLiveStats) };
    const bid = { gs: num(coalesce(meta.bwicGrossSize, meta.grossSize)), gdv: num(coalesce(meta.bwicGrossDv01, meta.grossDv01)), ct: int(coalesce(meta.bwicCount, meta.count)), liq: preferNumber(meta.bwicLiqScoreCombined, meta.bwicLiqScoreBase), rating: nonEmpty(meta.bwicCreditRating) || nonEmpty(meta.creditRating) || '-', dur: preferNumber(meta.bwicDuration, meta.duration), signal: num(meta.bwicSignalLiveStats) };
    const ask = { gs: num(coalesce(meta.owicGrossSize, meta.grossSize)), gdv: num(coalesce(meta.owicGrossDv01, meta.grossDv01)), ct: int(coalesce(meta.owicCount, meta.count)), liq: preferNumber(meta.owicLiqScoreCombined, meta.owicLiqScoreBase), rating: nonEmpty(meta.owicCreditRating) || nonEmpty(meta.creditRating) || '-', dur: preferNumber(meta.owicDuration, meta.duration), signal: num(meta.owicSignalLiveStats) };

    const headerParts = [clientName, truncateName(traderName), side || '-', statusOrDue].filter(Boolean);

    if (side === 'BOWIC') {
        const line1Metrics = [kv('Gross', fmtSize(overall.gs)), kv('Net', fmtSize(overall.ns)), kv('DV01', fmtK(overall.gdv)), kv(fmtInt(overall.ct), 'bonds'), kv('Liq', fmtLiq(overall.liq)), overall.rating || '', kv('Dur', fmtDur(overall.dur)), kv('Sig', fmtDur(overall.signal))].join(', ');
        const line1 = `<div class="overview-string-line" id=overview-string-line-1-a>${headerParts.join(' | ')}</div><div class="overview-string-line" id=overview-string-line-1-b>${line1Metrics}</div>`;
        return `<div class="overview-string-line" id='overview-string-line-1'>${line1.trim()}</div><div class="overview-string-line" id='overview-string-line-2'>${sideLine('Bid Side', bid).trim()}</div><div class="overview-string-line" id='overview-string-line-3'>${sideLine('Offer Side', ask).trim()}</div>`;
    }
    if (side === 'BWIC') {
        return `<div class="overview-string-line" id='overview-string-line-1'><div class="overview-string-line" id=overview-string-line-1-a>${headerParts.join(' | ')}</div></div><div class="overview-string-line" id='overview-string-line-2'>${sideLine('Bid Side', bid).trim()}</div>`;
    }
    if (side === 'OWIC') {
        return `<div class="overview-string-line" id='overview-string-line-1'><div class="overview-string-line" id=overview-string-line-1-a>${headerParts.join(' | ')}</div></div><div class="overview-string-line" id='overview-string-line-2'>${sideLine('Offer Side', ask).trim()}</div>`;
    }

    const hasBid = int(meta.bwicCount) > 0 || int(meta.count) > 0;
    const hasAsk = int(meta.owicCount) > 0;
    if (hasBid && hasAsk) return `${headerParts.join(' | ')} | ${kv('Gsz', fmtSize(overall.gs))} | ${kv('Nsz', fmtSize(overall.ns))}\n${sideLine('BID Side:', bid)}\n${sideLine('OFFER Side:', ask)}`;
    if (hasBid) return `${headerParts.join(' | ')}\n${sideLine('BID Side:', bid)}`;
    if (hasAsk) return `${headerParts.join(' | ')}\n${sideLine('OFFER Side:', ask)}`;
    return headerParts.join(' | ');
}
