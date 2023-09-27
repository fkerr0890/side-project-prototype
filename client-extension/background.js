let activeTabId = -1;
let pendingRequests = {};
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
    if (!response['ResourceAvailable']) {
        return;
    }
    if (response['ResourceAvailable'] === 'index.html' && Object.keys(pendingRequests).length === 0) {
        console.log('Redirecting to home...');
        await browser.tabs.update(activeTabId, {url: 'http://localhost/'});
    }
    else if (pendingRequests[response['ResourceAvailable']]) {
        console.log('Redirecting...');
        pendingRequests[response['ResourceAvailable']]({redirectUrl: 'http://localhost/' + response['ResourceAvailable']});
    }
});

function inteceptRequestAsync(requestDetails) {
    const parts = requestDetails.url.split('/');
    let resourcePath = parts.slice(3, parts.length).join('/');
    if (resourcePath === '') {
        resourcePath = 'index.html';
    }
    if (pendingRequests[resourcePath]) {
        pendingRequests[resourcePath] = null;
        return {cancel: false};
    }
    console.log('Request intercepted: ' + JSON.stringify(requestDetails));
    return new Promise((resolve, reject) => {
        console.log('Sending search request for ' + resourcePath);
        backendPort.postMessage(`{"SearchRequest": "${resourcePath}"}`);
        pendingRequests[resourcePath] = resolve;
        setTimeout(() => {
            reject('Request timed out');
        }, 100000);
    });
}

browser.webRequest.onBeforeRequest.addListener(inteceptRequestAsync, {urls: ['<all_urls>']}, ['blocking']);

// browser.tabs.onUpdated.addListener(async (tabId, changeInfo) => {
//     if (changeInfo.status === 'complete') {
//         console.log('Loading complete');
//     }
// });

browser.omnibox.onInputChanged.addListener((_text, addSuggestions) => {
    addSuggestions([{ 'content': 'https://hello.p2p/', 'description': 'Visit "hello" on the p2p network!' }]);
});
browser.omnibox.onInputEntered.addListener(async (_url, _disposition) => {
    activeTabId = (await browser.tabs.create({url: 'http://localhost/'})).id;
});