function watchForSearchInput() {
  const observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      for (const node of mutation.addedNodes) {
        if (node.nodeType !== Node.ELEMENT_NODE) continue;

        // Check if the added node is an input
        if (node.tagName === 'INPUT') {
          console.log('Input placeholder:', node.placeholder);
        }

        // Check for input elements within the added node
        const inputs = node.querySelectorAll?.('input');
        if (inputs) {
          for (const input of inputs) {
            console.log('Input placeholder:', input.placeholder);
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
