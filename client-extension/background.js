let activeApps = new Map();

function inteceptRequest(requestDetails) {
    const parts = requestDetails.url.split('/');
    if (parts[3][0] == '~') {
        activeApps.set(requestDetails.tabId, parts[3].slice(1));
        return {};
    }
    const activeApp = activeApps.get(requestDetails.tabId);
    if (!activeApp) {
        return {};
    }
    console.log('request intercepted');
    parts.splice(3, 0, '~' + activeApp);
    console.log('original url: ' + requestDetails.url + ', new url: ' + parts.join('/'))
    return { redirectUrl: parts.join('/') }
}

function addHeader(headerDetails) {
    headerDetails.requestHeaders.push({name: 'timestamp', value: headerDetails.timeStamp.toString()});
    return { requestHeaders: headerDetails.requestHeaders };
}

browser.webRequest.onBeforeRequest.addListener(inteceptRequest, {urls: ["<all_urls>"]}, ['blocking']);
browser.webRequest.onBeforeSendHeaders.addListener(addHeader, {urls: ["<all_urls>"]}, ['blocking', 'requestHeaders'])

browser.omnibox.onInputChanged.addListener((_text, addSuggestions) => {
    addSuggestions([{ 'content': 'https://hello.p2p/', 'description': 'Visit "hello" on the p2p network!' }]);
});
browser.omnibox.onInputEntered.addListener(async (_url, _disposition) => {
    activeTabId = (await browser.tabs.create({url: 'http://localhost/'})).id;
});