let activeTabId = -1;
const backendPort = browser.runtime.connectNative('p2pbackend');
backendPort.onDisconnect.addListener((p) => {
    console.log('Disconnected');
    if (p.error) {
      console.log(`Disconnected due to an error: ${p.error.message}`);
    }
  });
backendPort.onMessage.addListener(async response => {
    console.log('From nm host:' + response);
    if (typeof response === 'object') {
        console.log(response['ResourceAvailable']);
    }
    if (response['ResourceAvailable'] === 'index.html') {
        console.log('Redirecting...');
        await browser.tabs.update(activeTabId, {url: 'http://localhost/'});
    }
});

browser.tabs.onUpdated.addListener(async (tabId, changeInfo) => {
    if (changeInfo.status === 'complete') {
        console.log('Loading complete');
    }
});
browser.omnibox.onInputChanged.addListener((_text, addSuggestions) => {
    addSuggestions([{ 'content': 'https://hello.p2p/', 'description': 'Visit "hello" on the p2p network!' }]);
});
browser.omnibox.onInputEntered.addListener(async (url, _disposition) => {
    activeTabId = (await browser.tabs.create({url: '/my_page.html?id=' + url})).id;
    console.log('Active tab id: ' + activeTabId);
    console.log('Sending search request...');
    backendPort.postMessage('{"SearchRequest": "index.html"}');
});