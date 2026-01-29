// we are leet haxors
// disabled to test the built in functionality

function codelang() {
  console.log('[code-lang] Script initialized');

  const STORAGE_KEY = 'chroma-docs-lang-preference';
  const VALID_LANGS = ['python', 'typescript'];

  // Save preference when hash changes
  function savePreference() {
    const hash = window.location.hash.slice(1).toLowerCase();
    console.log('[code-lang] Hash changed to:', hash);
    if (VALID_LANGS.includes(hash)) {
      localStorage.setItem(STORAGE_KEY, hash);
      console.log('[code-lang] Saved preference:', hash);
    } else {
      console.log('[code-lang] Hash not a valid lang, not saving');
    }
  }

  // Restore preference on page load
  function restorePreference() {
    const saved = localStorage.getItem(STORAGE_KEY);
    const currentHash = window.location.hash.slice(1).toLowerCase();
    console.log('[code-lang] Restoring preference. Saved:', saved, 'Current hash:', currentHash);

    if (saved && VALID_LANGS.includes(saved)) {
      // Only set if current hash is empty or not a valid lang
      if (!VALID_LANGS.includes(currentHash)) {
        console.log('[code-lang] Setting hash to saved preference:', saved);
        history.replaceState(null, '', '#' + saved);
      } else {
        console.log('[code-lang] Current hash is valid, keeping it');
      }
    } else {
      console.log('[code-lang] No valid saved preference found');
    }
  }

  // Monkey-patch replaceState to detect tab switches
  const originalReplaceState = history.replaceState;
  history.replaceState = function (state, title, url) {
    console.log('[code-lang] history.replaceState called with url:', url);
    const result = originalReplaceState.apply(this, arguments);
    savePreference();
    return result;
  };
  console.log('[code-lang] history.replaceState patched');

  // Monkey-patch pushState to inject saved language hash into navigations
  const originalPushState = history.pushState;
  history.pushState = function (state, title, url) {
    const saved = localStorage.getItem(STORAGE_KEY);
    console.log('[code-lang] history.pushState called with url:', url, 'saved preference:', saved);
    if (saved && VALID_LANGS.includes(saved) && url) {
      const newUrl = url + '#' + saved;
      console.log('[code-lang] Injecting hash, new url:', newUrl);
      return originalPushState.apply(this, [state, title, newUrl]);
    }
    return originalPushState.apply(this, arguments);
  };
  console.log('[code-lang] history.pushState patched');

  // Restore on load
  restorePreference();
}
