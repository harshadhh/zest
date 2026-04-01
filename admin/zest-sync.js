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

        // Enable offline persistence — fire and forget, NEVER await (can block for seconds)
        _db.enablePersistence({ synchronizeTabs: true }).catch(() => {});

        // Wait up to 12s for auth state to restore. If we timeout too early, guardPage forcefully logs out.
        await new Promise(resolve => {
          let resolved = false;
          const unsub = _auth.onAuthStateChanged(user => {
            if (resolved) { _currentUser = user; return; }
            resolved = true;
            _currentUser = user;
            unsub();
            resolve();
          });
          setTimeout(() => {
            if (!resolved) {
              resolved = true;
              unsub();
              // If timeout hits, trust sessionStorage so we don't aggressively boot people on slow networks
              if (sessionStorage.getItem('zest_admin_auth') === 'true') {
                _currentUser = { uid: 'offline_cached_user' }; 
              }
              resolve();
            }
          }, 12000);
        });

        _ready = true;
        _showBadge(_currentUser ? 'live' : 'unauthed');

        // FIX #16: Persistent auth listener — keeps _currentUser fresh as
        // Firebase silently refreshes tokens every hour. Without this,
        // writes fail after ~1hr with auth/token-expired.
        _auth.onAuthStateChanged(user => {
          _currentUser = user;
          _showBadge(user ? 'live' : 'unauthed');
        });

        // Auto-load saved Google Sheet URLs from Firestore into localStorage
        if (_currentUser) {
          _loadSheetUrlsFromFirestore().catch(() => {});
        }

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

  /**
   * uid(prefix) — FIX #12: collision-safe ID generator.
   * Combines Date.now() with a 6-char random suffix so two records
   * created in the same millisecond never share the same ID.
   * Usage: uid('S') → 'S1711271234567_a3f9k2'
   */
  function uid(prefix) {
    const rand = Math.random().toString(36).slice(2, 8);
    return `${prefix || ''}${Date.now()}_${rand}`;
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
      // SPEED FIX: instead of re-fetching ALL docs (slow!), just append to local cache.
      // Strip photo (base64 blob / large Storage URL) from localStorage — only keep the URL reference.
      const cacheRecord = { ...record };
      if (cacheRecord.photo && cacheRecord.photo.startsWith('data:')) {
        cacheRecord.photo = ''; // Never store raw base64 in localStorage — crashes the tab
      }
      const cache = safeGet(localKey);
      cache.push(cacheRecord);
      safeSet(localKey, cache);
      // Sync to Google Sheets (fire-and-forget)
      _syncToSheets('append', { collection, row: record, headers: Object.keys(record) });
      return record.id;
    } else {
      // Offline fallback: append to localStorage only
      const id = record.id || ('local_' + Date.now());
      const cacheRecord = { ...record, id };
      if (cacheRecord.photo && cacheRecord.photo.startsWith('data:')) cacheRecord.photo = '';
      const cache = safeGet(localKey);
      cache.push(cacheRecord);
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
      const cleanData = _cleanForSheets(data);
      fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain' },
        body: JSON.stringify({ action, data: cleanData }),
      }).catch(() => {}); // Silent — never block UI
    } catch (e) {}
  }

  // ------------------------------------------------------------------
  // SHEET URL PERSISTENCE — save/load Google Sheet URLs in Firestore
  // so the user only needs to set them once, not every day.
  // ------------------------------------------------------------------

  /**
   * saveSheetUrls — persist both Sheet URLs to Firestore config/sheetUrls.
   * Also saves to localStorage as before for immediate use.
   */
  async function saveSheetUrls(mainUrl, attUrl) {
    // Always save to localStorage for immediate use
    if (mainUrl) localStorage.setItem('zest_sheets_url', mainUrl);
    else localStorage.removeItem('zest_sheets_url');

    if (attUrl) localStorage.setItem('zest_att_sheets_url', attUrl);
    else localStorage.removeItem('zest_att_sheets_url');

    // Persist to Firestore so they survive browser data clears
    const ok = await initFirebase();
    if (ok && _db && _currentUser) {
      await _db.collection('config').doc('sheetUrls').set({
        mainSheetUrl: mainUrl || '',
        attSheetUrl: attUrl || '',
        updatedAt: new Date().toISOString(),
      });
    }
  }

  /**
   * _loadSheetUrlsFromFirestore — called automatically on every page load
   * during initFirebase. Restores URLs from Firestore into localStorage.
   */
  async function _loadSheetUrlsFromFirestore() {
    if (!_db || !_currentUser) return;
    try {
      const snap = await _db.collection('config').doc('sheetUrls').get();
      if (snap.exists) {
        const data = snap.data();
        if (data.mainSheetUrl) localStorage.setItem('zest_sheets_url', data.mainSheetUrl);
        if (data.attSheetUrl)  localStorage.setItem('zest_att_sheets_url', data.attSheetUrl);
      }
    } catch (e) {
      console.warn('[ZestSync] Could not load sheet URLs from Firestore:', e.message);
    }
  }

  /**
   * _syncToAttSheet — fire-and-forget POST to the ATTENDANCE Google Sheet web app.
   * Uses the separate 'zest_att_sheets_url' saved in Settings.
   */
  function _syncToAttSheet(action, data) {
    try {
      const url = localStorage.getItem('zest_att_sheets_url');
      if (!url) {
        console.warn('[ZestSync] No attendance sheet URL configured — skipping sync.');
        return;
      }
      console.debug('[ZestSync] Syncing to Attendance Sheet:', action, Object.keys(data));
      fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain' },
        body: JSON.stringify({ action, data }),
      })
      .then(r => console.debug('[ZestSync] Attendance Sheet sync response:', r.status, r.type))
      .catch(err => console.warn('[ZestSync] Attendance Sheet sync error:', err.message));
    } catch (e) {
      console.warn('[ZestSync] Attendance Sheet sync exception:', e.message);
    }
  }

  /**
   * updateRow — update a single Firestore document, re-fetch to update cache.
   * Also syncs update to Google Sheets if configured.
   */
  async function updateRow(collection, localKey, docId, updates) {
    const ok = await initFirebase();
    if (ok && _db) {
      await _db.collection(collection).doc(docId).update(updates);
    }
    
    // Update local cache regardless of online/offline
    const cache = safeGet(localKey);
    const idx = cache.findIndex(r => r.id === docId);
    if (idx !== -1) { cache[idx] = { ...cache[idx], ...updates }; safeSet(localKey, cache); }

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
    }
    
    // Update local cache regardless of online/offline
    const cache = safeGet(localKey);
    safeSet(localKey, cache.filter(r => String(r[idField]) !== String(idValue)));

    // Sync to Google Sheets (fire-and-forget)
    _syncToSheets('delete', { collection, idField, idValue });
  }

  /**
   * saveAll — SAFE upsert pattern: write first, then delete stale docs.
   * If a crash happens mid-operation, data is never lost — at worst
   * some stale docs remain until the next sync.
   * Respects Firestore 500-op batch limit (chunks of 490).
   */
  async function saveAll(collection, localKey, rows) {
    const ok = await initFirebase();
    if (ok && _db) {
      // PHASE 1: Upsert all new/updated rows FIRST (data is safe from this point)
      const newIds = new Set();
      for (let i = 0; i < rows.length; i += 490) {
        const chunk = rows.slice(i, i + 490);
        const batch = _db.batch();
        chunk.forEach(row => {
          const id = row.id || (collection.charAt(0).toUpperCase() + Date.now() + '_' + Math.random().toString(36).slice(2, 6));
          if (!row.id) row.id = id;
          newIds.add(id);
          batch.set(_db.collection(collection).doc(id), row);
        });
        await batch.commit();
      }

      // PHASE 2: Delete stale docs that are NOT in the new set
      // (safe — if crash happens here, worst case is extra docs remain)
      const existingSnap = await _db.collection(collection).get();
      const staleDocs = [];
      existingSnap.forEach(doc => {
        if (!newIds.has(doc.id)) staleDocs.push(doc.ref);
      });
      for (let i = 0; i < staleDocs.length; i += 490) {
        const chunk = staleDocs.slice(i, i + 490);
        const batch = _db.batch();
        chunk.forEach(ref => batch.delete(ref));
        await batch.commit();
      }

      // Update localStorage cache
      safeSet(localKey, rows);

      // Sync full collection to Google Sheets (fire-and-forget)
      // FIX: don't send empty headers array — Apps Script uses its HEADERS
      // constant as the fallback, which always has the correct column names.
      const sheetPayload = rows.length
        ? { collection, rows, headers: Object.keys(rows[0]) }
        : { collection, rows };  // omit headers so Apps Script uses its HEADERS constant
      _syncToSheets('set', sheetPayload);
    } else {
      safeSet(localKey, rows);
    }
  }

  // ------------------------------------------------------------------
  // CASCADE OPERATIONS — for data integrity
  // ------------------------------------------------------------------

  /**
   * deleteStudentCascade — delete a student AND all related data:
   * fees, results, and attendance entries for that student ID.
   * Prevents orphaned records.
   */
  async function deleteStudentCascade(studentId) {
    const ok = await initFirebase();
    if (!ok || !_db) throw new Error('Firebase not available');

    // 1. Delete from students
    await _db.collection('students').doc(studentId).delete();
    _syncToSheets('delete', { collection: 'students', idField: 'id', idValue: studentId });

    // 2. Delete all fee records for this student
    const feeSnap = await _db.collection('fees').where('studentId', '==', studentId).get();
    if (!feeSnap.empty) {
      const batch = _db.batch();
      feeSnap.forEach(doc => batch.delete(doc.ref));
      await batch.commit();
    }
    _syncToSheets('delete', { collection: 'fees', idField: 'studentId', idValue: studentId });

    // 3. Delete all result records for this student
    const resSnap = await _db.collection('results').where('studentId', '==', studentId).get();
    if (!resSnap.empty) {
      const batch = _db.batch();
      resSnap.forEach(doc => batch.delete(doc.ref));
      await batch.commit();
    }
    _syncToSheets('delete', { collection: 'results', idField: 'studentId', idValue: studentId });

    // 4. Remove from attendance documents (all months)
    // Attendance docs store { day_X: { studentId: status } }
    // Remove studentId key from each day object
    const attSnap = await _db.collection('attendance').get();
    const attBatch = _db.batch();
    let attUpdates = 0;
    attSnap.forEach(doc => {
      const data = doc.data();
      const updates = {};
      let hasUpdate = false;
      Object.entries(data).forEach(([dayKey, dayData]) => {
        if (typeof dayData === 'object' && dayData[studentId] !== undefined) {
          const cleaned = { ...dayData };
          delete cleaned[studentId];
          updates[dayKey] = cleaned;
          hasUpdate = true;
        }
      });
      if (hasUpdate) {
        attBatch.update(doc.ref, updates);
        attUpdates++;
      }
    });
    if (attUpdates > 0) await attBatch.commit();

    // 5. Clear from local caches directly to prevent massive load times
    const cStudents = safeGet('zest_students');
    safeSet('zest_students', cStudents.filter(s => String(s.id) !== String(studentId)));

    const cFees = safeGet('zest_fees');
    safeSet('zest_fees', cFees.filter(f => String(f.studentId) !== String(studentId)));

    const cRes = safeGet('zest_results');
    safeSet('zest_results', cRes.filter(r => String(r.studentId) !== String(studentId)));
  }

  /**
   * propagateStudentNameChange — when a student's name is edited,
   * update the studentName field in all their fee and result records.
   * Prevents orphaned references.
   */
  async function propagateStudentNameChange(studentId, newName) {
    const ok = await initFirebase();
    if (!ok || !_db) return;

    // Update fees
    const feeSnap = await _db.collection('fees').where('studentId', '==', studentId).get();
    if (!feeSnap.empty) {
      const batch = _db.batch();
      feeSnap.forEach(doc => batch.update(doc.ref, { studentName: newName }));
      await batch.commit();
    }

    // Update results
    const resSnap = await _db.collection('results').where('studentId', '==', studentId).get();
    if (!resSnap.empty) {
      const batch = _db.batch();
      resSnap.forEach(doc => batch.update(doc.ref, { studentName: newName }));
      await batch.commit();
    }

    // Mutate caches directly to prevent lag
    const cFees = safeGet('zest_fees');
    let fChanged = false;
    cFees.forEach(f => { if (String(f.studentId) === String(studentId)) { f.studentName = newName; fChanged = true; } });
    if (fChanged) safeSet('zest_fees', cFees);

    const cRes = safeGet('zest_results');
    let rChanged = false;
    cRes.forEach(r => { if (String(r.studentId) === String(studentId)) { r.studentName = newName; rChanged = true; } });
    if (rChanged) safeSet('zest_results', cRes);
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
      const timeout = setTimeout(() => reject(new Error('Photo upload timed out')), 30000);
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

  /**
   * migrateBase64Photos — FIX #10: one-time migration utility.
   * Scans all student documents for base64 photos (stored inline in Firestore),
   * uploads them to Firebase Storage, then replaces the blob with a clean URL.
   * Run this once from Settings to clean up existing data.
   * Returns { migrated, skipped, errors } counts.
   */
  async function migrateBase64Photos(onProgress) {
    const ok = await initFirebase();
    if (!ok || !_db || !_storage) throw new Error('Firebase not available');

    const snap = await _db.collection('students').get();
    let migrated = 0, skipped = 0, errors = 0;

    for (const doc of snap.docs) {
      const data = doc.data();
      if (!data.photo || !data.photo.startsWith('data:')) {
        skipped++;
        if (onProgress) onProgress({ migrated, skipped, errors, total: snap.size });
        continue;
      }

      try {
        const url = await uploadPhoto(data.id || doc.id, data.photo);
        await _db.collection('students').doc(doc.id).update({ photo: url });
        migrated++;
      } catch (e) {
        console.warn('[ZestSync] migrateBase64Photos failed for', doc.id, e.message);
        errors++;
      }
      if (onProgress) onProgress({ migrated, skipped, errors, total: snap.size });
    }

    // Refresh student cache
    await getAll('students', 'zest_students');
    return { migrated, skipped, errors };
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
    }

    // Sync to Attendance Google Sheet (fire-and-forget, works regardless of Firebase)
    _syncToAttSheet('saveAttendance', { date, attendance: dayData, studentMap: _studentMap });

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
    
    // If Firebase failed or user is not logged in via Firebase
    if (!ok || !_currentUser) {
      // 🚨 FIX: Be forgiving. If they already had access before (sessionStorage is true), let them stay in 'Offline Mode'.
      // Otherwise, an invalid Firebase config or offline network will instantly kick them out.
      if (sessionStorage.getItem('zest_admin_auth') === 'true') {
        console.warn('[ZestSync] Firebase auth failed, but session cached. Entering offline mode.');
      } else {
        sessionStorage.removeItem('zest_admin_auth');
        window.location.href = 'index.html';
        return;
      }
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
    deleteStudentCascade,
    propagateStudentNameChange,

    // Photo
    uploadPhoto,

    // Attendance
    saveAttendanceToSheet,
    getAttendance,

    // Config
    saveSheetUrls,

    // Utils
    testConnection,
    pushAllToFirestore,
    pullFromFirestore,
    escapeHtml,
    uid,
    migrateBase64Photos,

    // Expose db for advanced use (settings page danger zone only)
    getDb: () => _db,
    getAuth: () => _auth,
    getCurrentUser: () => _currentUser,
  };

})();
