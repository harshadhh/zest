// =============================================================
//  zest-sync.js — Firebase Auth + Firestore sync library
//  The Zest Classes Admin Panel
// =============================================================

window.ZestSync = (function () {

  let _db = null, _storage = null, _auth = null;
  let _ready = false, _initPromise = null, _currentUser = null;

  // ------------------------------------------------------------------
  // Config — reads from firebase-config.js (gitignored)
  // ------------------------------------------------------------------
  function getFirebaseConfig() {
    let cfg = null;
    if (window.ZEST_FIREBASE_CONFIG && window.ZEST_FIREBASE_CONFIG.apiKey) {
      cfg = { ...window.ZEST_FIREBASE_CONFIG };
    } else {
      // Fallback: read config saved from Settings page
      try {
        const stored = JSON.parse(localStorage.getItem('zest_firebase_config') || 'null');
        if (stored && stored.apiKey) cfg = stored;
      } catch (e) {}
    }
    if (!cfg) return null;
    // Fix new Firebase projects using firebasestorage.app instead of appspot.com
    if (cfg.storageBucket && cfg.storageBucket.endsWith('.firebasestorage.app'))
      cfg.storageBucket = cfg.storageBucket.replace('.firebasestorage.app', '.appspot.com');
    return cfg;
  }

  // ------------------------------------------------------------------
  // LocalStorage helpers (cache only — Firestore is source of truth)
  // ------------------------------------------------------------------
  function safeGet(key) {
    try { return JSON.parse(localStorage.getItem(key) || '[]'); }
    catch (e) { return []; }
  }
  function safeSet(key, val) {
    try { localStorage.setItem(key, JSON.stringify(val)); } catch (e) {}
  }
  function safeGetObj(key) {
    try { return JSON.parse(localStorage.getItem(key) || '{}'); }
    catch (e) { return {}; }
  }
  function safeSetObj(key, val) {
    try { localStorage.setItem(key, JSON.stringify(val)); } catch (e) {}
  }

  // ------------------------------------------------------------------
  // Dynamic script loader
  // ------------------------------------------------------------------
  function loadScript(src) {
    return new Promise((resolve, reject) => {
      if (document.querySelector('script[src="' + src + '"]')) { resolve(); return; }
      const s = document.createElement('script');
      s.src = src;
      s.onload = resolve;
      s.onerror = () => reject(new Error('Failed to load: ' + src));
      document.head.appendChild(s);
    });
  }

  // ------------------------------------------------------------------
  // initFirebase — loads SDK, initialises app, waits for auth state
  // ------------------------------------------------------------------
  async function initFirebase() {
    if (_ready && _db) return true;
    if (_initPromise) return _initPromise;

    _initPromise = (async () => {
      const config = getFirebaseConfig();
      if (!config) return false;

      try {
        await loadScript('https://www.gstatic.com/firebasejs/9.23.0/firebase-app-compat.js');
        await loadScript('https://www.gstatic.com/firebasejs/9.23.0/firebase-auth-compat.js');
        await loadScript('https://www.gstatic.com/firebasejs/9.23.0/firebase-firestore-compat.js');
        await loadScript('https://www.gstatic.com/firebasejs/9.23.0/firebase-storage-compat.js');

        if (!window.firebase.apps.length) window.firebase.initializeApp(config);

        _auth    = window.firebase.auth();
        _db      = window.firebase.firestore();
        _storage = window.firebase.storage();

        // Enable offline persistence
        try {
          await _db.enablePersistence({ synchronizeTabs: true });
        } catch (e) { /* non-fatal */ }

        // Wait up to 10s for auth state to restore
        await new Promise(resolve => {
          const unsub = _auth.onAuthStateChanged(user => {
            _currentUser = user;
            unsub();
            resolve();
          });
          setTimeout(resolve, 10000);
        });

        _ready = true;
        _showBadge(_currentUser ? 'live' : 'unauthed');
        return true;

      } catch (e) {
        console.error('[ZestSync] initFirebase error:', e);
        _db = null; _storage = null; _auth = null;
        _ready = false; _initPromise = null;
        _showBadge('offline');
        return false;
      }
    })();

    return _initPromise;
  }

  // ------------------------------------------------------------------
  // Sync status badge
  // ------------------------------------------------------------------
  function _showBadge(mode) {
    const el = document.getElementById('syncStatusBadge');
    if (!el) return;
    if      (mode === 'live')       { el.textContent = '🔥 Firebase Live';     el.style.color = 'rgba(251,146,60,.9)'; }
    else if (mode === 'offline')    { el.textContent = '🔴 Offline';           el.style.color = 'rgba(239,68,68,.8)'; }
    else if (mode === 'unauthed')   { el.textContent = '🔒 Not signed in';     el.style.color = 'rgba(239,68,68,.8)'; }
    else                            { el.textContent = '⏳ Connecting...';     el.style.color = 'rgba(235,168,33,.8)'; }
  }

  function initSyncBadge() {
    _showBadge('connecting');
    initFirebase().then(ok => _showBadge(ok && _currentUser ? 'live' : ok ? 'unauthed' : 'offline'));
  }

  function isLive() {
    return _ready && _db && !!_currentUser;
  }

  // ------------------------------------------------------------------
  // AUTH
  // ------------------------------------------------------------------
  async function signInWithEmail(email, password) {
    const ok = await initFirebase();
    if (!ok || !_auth) return { success: false, error: 'Firebase not available' };
    try {
      const cred = await _auth.signInWithEmailAndPassword(email, password);
      _currentUser = cred.user;
      sessionStorage.setItem('zest_admin_auth', 'true');
      _showBadge('live');
      return { success: true };
    } catch (e) {
      return { success: false, error: e.code };
    }
  }

  async function verifyPassword(pass) {
    const config = getFirebaseConfig();
    if (!config) return false;
    const adminEmail = window.ZEST_ADMIN_EMAIL || 'admin@zestclasses.com';
    const result = await signInWithEmail(adminEmail, pass);
    if (result.success) return true;
    if (['auth/wrong-password', 'auth/invalid-credential', 'auth/user-not-found'].includes(result.error)) return false;
    if (result.error === 'auth/too-many-requests') throw new Error('Too many attempts. Try again later.');
    return false;
  }

  async function logout() {
    try { if (_auth) await _auth.signOut(); } catch (e) {}
    sessionStorage.clear();
    _currentUser = null;
    window.location.href = 'index.html';
  }

  async function changePassword(newPass) {
    // Fall back to _auth.currentUser if _currentUser is null (page reload edge case)
    if (!_currentUser && _auth) _currentUser = _auth.currentUser;
    if (!_currentUser) throw new Error('Please log out and log back in before changing password.');
    await _currentUser.updatePassword(newPass);
  }

  // ------------------------------------------------------------------
  // FIRESTORE DATA OPERATIONS — Firestore is always source of truth
  // ------------------------------------------------------------------

  /**
   * getAll — fetch full collection from Firestore, cache to localStorage.
   * Falls back to localStorage cache if Firestore unreachable.
   *
   * IMPORTANT: We always store records with record.id = our custom ID (e.g. 'S1234').
   * appendRow uses .doc(record.id).set() so Firestore doc ID always matches record.id.
   * getAll merges { id: doc.id, ...doc.data() } — since doc.id === record.id this is consistent.
   * If a record has no custom id (legacy), doc.id is used as fallback.
   */
  async function getAll(collection, localKey) {
    const ok = await initFirebase();
    if (ok && _db) {
      try {
        const snap = await _db.collection(collection).get();
        const rows = [];
        snap.forEach(doc => {
          const data = doc.data();
          // Use our custom id field if present, otherwise fall back to Firestore doc id
          rows.push({ ...data, id: data.id || doc.id });
        });
        safeSet(localKey, rows);
        return rows;
      } catch (e) {
        console.warn('[ZestSync] getAll Firestore error, falling back to cache:', e);
      }
    }
    return safeGet(localKey);
  }

  /**
   * appendRow — write a new record to Firestore using record.id as the doc ID.
   * This ensures Firestore doc ID always matches our custom id field,
   * so deleteRow and updateRow can find the document reliably.
   */
  async function appendRow(collection, localKey, record) {
    const ok = await initFirebase();
    if (ok && _db) {
      if (!record.id) {
        // Safety: assign an id if missing
        record = { ...record, id: collection.charAt(0).toUpperCase() + Date.now() };
      }
      // Use .doc(record.id).set() — NOT .add() — so our ID is the Firestore doc ID
      await _db.collection(collection).doc(record.id).set(record);
      await getAll(collection, localKey);
      // Sync to Google Sheets (fire-and-forget)
      _syncToSheets('append', { collection, row: record, headers: Object.keys(record) });
      return record.id;
    } else {
      // Offline fallback: append to localStorage only
      const id = record.id || ('local_' + Date.now());
      const cache = safeGet(localKey);
      cache.push({ ...record, id });
      safeSet(localKey, cache);
      return id;
    }
  }

  // ------------------------------------------------------------------
  // GOOGLE SHEETS 2-WAY SYNC — fire-and-forget, never blocks Firebase ops
  // ------------------------------------------------------------------

  /**
   * _cleanForSheets — strip photo fields (base64 blobs) before sending to Sheets.
   * Also strips any field whose value is over 40KB to avoid Apps Script payload issues.
   */
  function _cleanForSheets(data) {
    if (!data) return data;
    const d = { ...data };
    if (d.row && typeof d.row === 'object') {
      d.row = { ...d.row };
      delete d.row.photo;
    }
    if (Array.isArray(d.rows)) {
      d.rows = d.rows.map(r => { const c = { ...r }; delete c.photo; return c; });
    }
    if (Array.isArray(d.headers)) {
      d.headers = d.headers.filter(h => h !== 'photo');
    }
    if (d.updates && typeof d.updates === 'object') {
      d.updates = { ...d.updates };
      delete d.updates.photo;
    }
    return d;
  }

  function _syncToSheets(action, data) {
    try {
      const url = localStorage.getItem('zest_sheets_url');
      if (!url) return; // No script URL configured
      const token = sessionStorage.getItem('zest_sheets_token') || '';
      const cleanData = _cleanForSheets(data);
      fetch(url, {
        method: 'POST',
        mode: 'no-cors',
        headers: { 'Content-Type': 'text/plain' },
        body: JSON.stringify({ action, token, data: cleanData }),
      }).catch(() => {}); // Silent — never block UI
    } catch (e) {}
  }

  /**
   * _syncToAttSheet — fire-and-forget POST to the ATTENDANCE Google Sheet web app.
   * Uses the separate 'zest_att_sheets_url' saved in Settings.
   */
  function _syncToAttSheet(action, data) {
    try {
      const url = localStorage.getItem('zest_att_sheets_url');
      if (!url) return; // No attendance script URL configured
      fetch(url, {
        method: 'POST',
        mode: 'no-cors',
        headers: { 'Content-Type': 'text/plain' },
        body: JSON.stringify({ action, data }),
      }).catch(() => {}); // Silent — never block UI
    } catch (e) {}
  }

  /**
   * updateRow — update a single Firestore document, re-fetch to update cache.
   * Also syncs update to Google Sheets if configured.
   */
  async function updateRow(collection, localKey, docId, updates) {
    const ok = await initFirebase();
    if (ok && _db) {
      await _db.collection(collection).doc(docId).update(updates);
      await getAll(collection, localKey);
    } else {
      // Offline fallback
      const cache = safeGet(localKey);
      const idx = cache.findIndex(r => r.id === docId);
      if (idx !== -1) { cache[idx] = { ...cache[idx], ...updates }; safeSet(localKey, cache); }
    }
    // Sync to Google Sheets (fire-and-forget)
    _syncToSheets('update', { collection, idField: 'id', idValue: docId, updates });
  }

  /**
   * deleteRow — delete a single Firestore doc, then re-fetch.
   * Also syncs deletion to Google Sheets if configured.
   * If idField === 'id', delete by doc ID directly.
   * Otherwise, query by field value.
   */
  async function deleteRow(collection, localKey, idField, idValue) {
    const ok = await initFirebase();
    if (ok && _db) {
      if (idField === 'id') {
        await _db.collection(collection).doc(idValue).delete();
      } else {
        const snap = await _db.collection(collection).where(idField, '==', idValue).get();
        const batch = _db.batch();
        snap.forEach(doc => batch.delete(doc.ref));
        await batch.commit();
      }
      await getAll(collection, localKey);
    } else {
      // Offline fallback
      const cache = safeGet(localKey);
      safeSet(localKey, cache.filter(r => r[idField] !== idValue));
    }
    // Sync to Google Sheets (fire-and-forget)
    _syncToSheets('delete', { collection, idField, idValue });
  }

  /**
   * saveAll — delete entire collection + rewrite in chunks of 490.
   * Respects Firestore 500-op batch limit.
   */
  async function saveAll(collection, localKey, rows) {
    const ok = await initFirebase();
    if (ok && _db) {
      // PHASE 1: Delete all existing docs in chunks of 490
      const existingSnap = await _db.collection(collection).get();
      const allDocs = [];
      existingSnap.forEach(doc => allDocs.push(doc.ref));

      for (let i = 0; i < allDocs.length; i += 490) {
        const chunk = allDocs.slice(i, i + 490);
        const batch = _db.batch();
        chunk.forEach(ref => batch.delete(ref));
        await batch.commit();
      }

      // PHASE 2: Write new rows in chunks of 490
      for (let i = 0; i < rows.length; i += 490) {
        const chunk = rows.slice(i, i + 490);
        const batch = _db.batch();
        chunk.forEach(row => {
          const ref = row.id
            ? _db.collection(collection).doc(row.id)
            : _db.collection(collection).doc();
          batch.set(ref, row);
        });
        await batch.commit();
      }

      // Update localStorage cache
      safeSet(localKey, rows);

      // Sync full collection to Google Sheets (fire-and-forget)
      const headers = rows.length ? Object.keys(rows[0]) : [];
      _syncToSheets('set', { collection, rows, headers });
    } else {
      safeSet(localKey, rows);
    }
  }

  // ------------------------------------------------------------------
  // PHOTO UPLOAD
  // ------------------------------------------------------------------

  /**
   * uploadPhoto — upload to Firebase Storage photos/{studentId}.jpg
   * Returns download URL. Has 8s timeout so it doesn't block saves.
   */
  /**
   * _dataUrlToBlob — convert a base64 dataURL string to a Blob.
   * Firebase Storage ref.put() requires a Blob/File, not a string.
   */
  function _dataUrlToBlob(dataUrl) {
    const parts = dataUrl.split(',');
    const mime  = parts[0].match(/:(.*?);/)[1];
    const bStr  = atob(parts[1]);
    const arr   = new Uint8Array(bStr.length);
    for (let i = 0; i < bStr.length; i++) arr[i] = bStr.charCodeAt(i);
    return new Blob([arr], { type: mime });
  }

  async function uploadPhoto(studentId, fileOrDataUrl) {
    const ok = await initFirebase();
    if (!ok || !_storage) throw new Error('Storage not available');

    // Accept either a File/Blob OR a base64 dataURL string
    const blob = (typeof fileOrDataUrl === 'string' && fileOrDataUrl.startsWith('data:'))
      ? _dataUrlToBlob(fileOrDataUrl)
      : fileOrDataUrl;

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Photo upload timed out')), 8000);
      const ref  = _storage.ref('photos/' + studentId + '.jpg');
      const task = ref.put(blob);
      task.then(async () => {
        clearTimeout(timeout);
        try {
          const url = await ref.getDownloadURL();
          resolve(url);
        } catch (e) {
          reject(e);
        }
      }).catch(e => {
        clearTimeout(timeout);
        reject(e);
      });
    });
  }

  // ------------------------------------------------------------------
  // ATTENDANCE — stored in attendance/{YYYY-MM} with day as field key
  // ------------------------------------------------------------------

  /**
   * saveAttendanceToSheet — saves to Firestore `attendance/{YYYY-MM}` document
   * with day number as field key. Also updates localStorage cache.
   * @param {string} date  — "YYYY-MM-DD"
   * @param {object} dayData — { studentId: 'present'|'absent'|'leave', ... }
   */
  async function saveAttendanceToSheet(date, dayData, _studentMap) {
    // _studentMap is accepted for API compatibility but not stored (Firestore doc stores statuses only)
    const [year, month, day] = date.split('-');
    const docId  = year + '-' + month;
    const dayKey = 'day_' + parseInt(day, 10);

    const ok = await initFirebase();
    if (ok && _db) {
      await _db.collection('attendance').doc(docId).set(
        { [dayKey]: dayData },
        { merge: true }
      );

      // Also sync to Attendance Google Sheet (fire-and-forget)
      _syncToAttSheet('saveAttendance', { date, attendance: dayData, studentMap: _studentMap });
    }

    // Update localStorage cache regardless
    const cacheKey = 'zest_attendance_' + docId;
    const cached   = safeGetObj(cacheKey);
    cached[dayKey] = dayData;
    safeSetObj(cacheKey, cached);
  }

  /**
   * getAttendance — fetches from Firestore `attendance/{YYYY-MM}` for a given date.
   * Returns the day object { studentId: status, ... }.
   * Falls back to localStorage cache if offline.
   * @param {string} date — "YYYY-MM-DD"
   */
  async function getAttendance(date) {
    const [year, month, day] = date.split('-');
    const docId   = year + '-' + month;
    const dayKey  = 'day_' + parseInt(day, 10);
    const cacheKey = 'zest_attendance_' + docId;

    const ok = await initFirebase();
    if (ok && _db) {
      try {
        const snap = await _db.collection('attendance').doc(docId).get();
        if (snap.exists) {
          const data = snap.data();
          // Update full month cache
          safeSetObj(cacheKey, data);
          return data[dayKey] || {};
        }
        return {};
      } catch (e) {
        console.warn('[ZestSync] getAttendance Firestore error, falling back to cache:', e);
      }
    }

    // Fallback to localStorage
    const cached = safeGetObj(cacheKey);
    return cached[dayKey] || {};
  }

  // ------------------------------------------------------------------
  // CONNECTION TEST
  // ------------------------------------------------------------------

  /**
   * testConnection — tries write if signed in, tries read if not.
   * permission-denied = connected but not authed.
   */
  async function testConnection() {
    const ok = await initFirebase();
    if (!ok || !_db) return false;
    try {
      if (_currentUser) {
        // Try a write
        await _db.collection('_ping').doc('test').set({ t: Date.now() });
      } else {
        // Try a read
        await _db.collection('students').limit(1).get();
      }
      return true;
    } catch (e) {
      // permission-denied means we can reach Firebase but aren't authed — still connected
      if (e.code === 'permission-denied') return true;
      return false;
    }
  }

  // ------------------------------------------------------------------
  // DATA SYNC UTILITIES (Settings page)
  // ------------------------------------------------------------------

  /** Push all localStorage data to Firestore */
  async function pushAllToFirestore(collections) {
    const results = {};
    for (const { collection, localKey } of collections) {
      try {
        const rows = safeGet(localKey);
        await saveAll(collection, localKey, rows);
        results[collection] = { ok: true, count: rows.length };
      } catch (e) {
        results[collection] = { ok: false, error: e.message };
      }
    }
    return results;
  }

  /** Pull all Firestore data to localStorage */
  async function pullFromFirestore(collections) {
    const results = {};
    for (const { collection, localKey } of collections) {
      try {
        const rows = await getAll(collection, localKey);
        results[collection] = { ok: true, count: rows.length };
      } catch (e) {
        results[collection] = { ok: false, error: e.message };
      }
    }
    return results;
  }

  // ------------------------------------------------------------------
  // HTML escaping utility — prevents XSS when using innerHTML
  // ------------------------------------------------------------------
  const _escMap = {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'};
  function escapeHtml(str) {
    if (str == null) return '';
    return String(str).replace(/[&<>"']/g, c => _escMap[c]);
  }

  // ------------------------------------------------------------------
  // Auth guard — call on every protected page
  // Hides body until auth is confirmed; redirects if not authed.
  // Calls callback once auth is verified.
  // ------------------------------------------------------------------
  async function guardPage(callback) {
    document.body.style.visibility = 'hidden';
    const ok = await initFirebase();
    if (!ok || !_currentUser) {
      sessionStorage.removeItem('zest_admin_auth');
      window.location.href = 'index.html';
      return;
    }
    sessionStorage.setItem('zest_admin_auth', 'true');
    document.body.style.visibility = '';
    if (typeof callback === 'function') callback();
  }

  // ------------------------------------------------------------------
  // Public API
  // ------------------------------------------------------------------
  return {
    // Init
    initFirebase,
    initSyncBadge,
    isLive,

    // Auth
    signInWithEmail,
    verifyPassword,
    logout,
    changePassword,
    guardPage,

    // Data
    getAll,
    appendRow,
    updateRow,
    deleteRow,
    saveAll,

    // Photo
    uploadPhoto,

    // Attendance
    saveAttendanceToSheet,
    getAttendance,

    // Utils
    testConnection,
    pushAllToFirestore,
    pullFromFirestore,
    escapeHtml,

    // Expose db for advanced use (settings page danger zone only)
    getDb: () => _db,
    getAuth: () => _auth,
    getCurrentUser: () => _currentUser,
  };

})();
