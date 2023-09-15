const backendPort = browser.runtime.connectNative('p2pbackend');
backendPort.onDisconnect.addListener((p) => {
    console.log('Disconnected');
    if (p.error) {
      console.log(`Disconnected due to an error: ${p.error.message}`);
    }
  });
backendPort.onMessage.addListener(response => {
    console.log(`Received: ${response}`)
});

browser.tabs.onUpdated.addListener(async (tabId, changeInfo) => {
    if (changeInfo.status === "complete") {
        console.log("Loading complete");
    }
});
browser.omnibox.onInputChanged.addListener((_text, addSuggestions) => {
    addSuggestions([{ 'content': 'https://hello.p2p/', 'description': 'Visit "hello" on the p2p network!' }]);
});
browser.omnibox.onInputEntered.addListener(async (url, _disposition) => {
    browser.tabs.create({url: '/my_page.html?id=' + url});
    // for (const _ of Array(5).keys()) {
    //     console.log("Sending:  ping");
    //     let sending = browser.runtime.sendNativeMessage("p2pbackend", "ping");
    //     sending.then(onResponse, onError);
    //     await new Promise(r => setTimeout(r, 2000));
    // }
    console.log("Sending:  ping");
    browser.runtime.postMessage("ping");
});

async function onResponse(response) {
    // console.log('Received document');
    // const parser = new DOMParser();
    // const newDocument = parser.parseFromString(response, "text/html");
    // const activeTab = await browser.tabs.query({ currentWindow: true, active: true });
    // await browser.scripting.executeScript({
    //     target: {
    //       tabId: activeTab.id,
    //     },
    //     func: () => {
    //       document = newDocument;
    //     },
    // });
    console.log(`Received: ${response}`)
}
  
function onError(error) {
    console.log(`Error: ${error}`);
}