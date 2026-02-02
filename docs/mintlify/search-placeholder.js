const SUFFIX = 'powered by Chroma';

function modifyPlaceholder(element) {
  const placeholder = element.placeholder || '';

  // Avoid duplicate appends
  if (placeholder.includes(SUFFIX)) return;

  element.placeholder = placeholder ? `${placeholder} ${SUFFIX}` : SUFFIX;
  console.log('Modified placeholder:', element.placeholder);
}

function watchForSearchInput() {
  const observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      for (const node of mutation.addedNodes) {
        if (node.nodeType !== Node.ELEMENT_NODE) continue;

        // Check if the added node is an input or textarea
        if (node.tagName === 'INPUT' || node.tagName === 'TEXTAREA') {
          modifyPlaceholder(node);
        }

        // Check for input/textarea elements within the added node
        const elements = node.querySelectorAll?.('input, textarea');
        if (elements) {
          for (const el of elements) {
            modifyPlaceholder(el);
          }
        }
      }
    }
  });

  observer.observe(document.body, {
    childList: true,
    subtree: true
  });
}

watchForSearchInput();
