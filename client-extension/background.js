let activeTabId = -1;
let pendingRequests = {};
// const backendPort = browser.runtime.connectNative('p2pbackend');

// backendPort.onDisconnect.addListener((p) => {
//     console.log('Disconnected');
//     if (p.error) {
//         console.log(`Disconnected due to an error: ${p.error.message}`);
//     }
// });

// backendPort.onMessage.addListener(async response => {
//     console.log('From nm host:' + response);
//     if (typeof response === 'object') {
//         console.log(response['ResourceAvailable']);
//     }
//     if (!response['ResourceAvailable']) {
//         return;
//     }
//     if (response['ResourceAvailable'] === 'index.html' && Object.keys(pendingRequests).length === 0) {
//         console.log('Redirecting to home...');
//         await browser.tabs.update(activeTabId, {url: 'http://localhost/'});
//     }
//     else if (pendingRequests[response['ResourceAvailable']]) {
//         console.log('Redirecting...');
//         pendingRequests[response['ResourceAvailable']]();
//     }
// });

function inteceptRequestAsync(requestDetails) {
    if (requestDetails.url.includes('~')) {
        return {};
    }
    console.log('request intercepted');
    const parts = requestDetails.url.split('/');
    parts.splice(3, 0, '~example');
    console.log('original url: ' + requestDetails.url + ', new url: ' + parts.join('/'))
    return { redirectUrl: parts.join('/') }
}

browser.webRequest.onBeforeRequest.addListener(inteceptRequestAsync, {urls: ["<all_urls>"]}, ['blocking']);

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