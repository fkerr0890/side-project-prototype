use std::{collections::HashMap, str::FromStr, sync::Arc, convert::Infallible, net::SocketAddr};

use hyper::{Request, Response, Body, body, HeaderMap, Version, StatusCode, header::{HeaderName, HeaderValue}, service::{make_service_fn, service_fn}, Server, Client, Method, Uri};
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, Mutex};

use crate::message::Message;

#[derive(Serialize, Deserialize, Clone)]
pub struct SerdeHttpRequest {
    method: String,
    uri: String,
    version: String,
    headers: HashMap<String, Vec<String>>,
    body: Vec<u8>
}

impl SerdeHttpRequest {
    async fn from_hyper_request(request: Request<Body>) -> Self {
        let (parts, body) = request.into_parts();
        Self {
            method: parts.method.to_string(),
            uri: parts.uri.to_string(),
            version: version_to_string(parts.version),
            headers: drain_headers(parts.headers),
            body: body::to_bytes(body).await.unwrap().to_vec()
        }
    }

    fn to_hyper_request(self, uri_prefix: String) -> Request<Body> {
        let mut request = Request::new(Body::from(self.body));
        *request.method_mut() = Method::from_str(&self.method).unwrap();
        *request.uri_mut() = Uri::from_str(&(uri_prefix + &self.uri)).unwrap();
        *request.version_mut() = string_to_version(self.version);
        *request.headers_mut() = reconstruct_header_map(self.headers);
        request
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SerdeHttpResponse {
    pub body: Vec<u8>,
    status_code: u16,
    version: String,
    headers: HashMap<String, Vec<String>>,
}

impl SerdeHttpResponse {
    async fn from_hyper_response(response: Response<Body>) -> Self {
        let (parts, body) = response.into_parts();
        Self {
            status_code: parts.status.as_u16(),
            version: version_to_string(parts.version),
            headers: drain_headers(parts.headers),
            body: body::to_bytes(body).await.unwrap().to_vec()
        }
    }

    pub fn new(status_code: u16, version: String, headers: HashMap<String, Vec<String>>, body: Vec<u8>) -> Self {
        Self {
            body,
            status_code,
            version,
            headers,
        }
    }

    fn to_hyper_response(self) -> Response<Body> {
        let mut response = Response::new(Body::from(self.body));
        *response.status_mut() = StatusCode::from_u16(self.status_code).unwrap();
        *response.version_mut() = string_to_version(self.version);
        *response.headers_mut() = reconstruct_header_map(self.headers);
        response
    }

    pub fn into_parts(self) -> (u16, String, HashMap<String, Vec<String>>, Vec<u8>) {
        return (self.status_code, self.version, self.headers, self.body)
    }
}

fn version_to_string(version: Version) -> String {
    match version {
        Version::HTTP_09 => String::from("HTTP/0.9"),
        Version::HTTP_10 => String::from("HTTP/1.0"),
        Version::HTTP_11 => String::from("HTTP/1.1"),
        Version::HTTP_2 => String::from("HTTP/2"),
        Version::HTTP_3 => String::from("HTTP/3.0"),
        _ => panic!("Http version not supported")
    }
}

fn string_to_version(version: String) -> Version {
    match version.as_str() {
        "HTTP/0.9" => Version::HTTP_09,
        "HTTP/1.0" => Version::HTTP_10,
        "HTTP/1.1" => Version::HTTP_11,
        "HTTP/2" => Version::HTTP_2,
        "HTTP/3.0" => Version::HTTP_3,
        _ => panic!("Http version not supported")
    }
}

fn drain_headers(mut header_map: HeaderMap) -> HashMap<String, Vec<String>> {
    let mut headers: HashMap<String, Vec<String>> = HashMap::with_capacity(header_map.len());
    let mut curr_header_name = String::new();
    for (header_name, header_value) in header_map.drain() {
        if let Some(header_name) = header_name {
            curr_header_name = header_name.to_string();
            headers.insert(curr_header_name.clone(), Vec::new());
        }
        headers.get_mut(&curr_header_name).unwrap().push(header_value.to_str().unwrap().to_owned());
    }
    headers
}

fn reconstruct_header_map(headers: HashMap<String, Vec<String>>) -> HeaderMap {
    let mut header_map = HeaderMap::new();
    for (header_name, header_values) in headers {
        for header_value in header_values {
            header_map.append(HeaderName::from_str(&header_name).unwrap(), HeaderValue::from_str(&header_value).unwrap());
        }
    }
    header_map
}

#[derive(Clone)]
pub struct ServerContext {
    egress: mpsc::UnboundedSender<Message>,
    from_outbound_gateway: Arc<Mutex<mpsc::UnboundedReceiver<SerdeHttpResponse>>>
}

impl ServerContext {
    pub fn new(egress: &mpsc::UnboundedSender<Message>, from_outbound_gateway: Arc<Mutex<mpsc::UnboundedReceiver<SerdeHttpResponse>>>) -> Self { Self { egress: egress.clone(), from_outbound_gateway } }
}

async fn handle_request(context: ServerContext, request: Request<Body>) -> Result<Response<Body>, Infallible> {     
    let serde_request = SerdeHttpRequest::from_hyper_request(request).await;
    for message in Message::initial_http_request(String::from("example"), serde_request).chunked() {
        context.egress.send(message).unwrap();
    }
    let mut rx = context.from_outbound_gateway.lock().await;
    let response = rx.recv().await.unwrap();
    Ok(response.to_hyper_response())
}

pub async fn tcp_listen(socket: SocketAddr, server_context: ServerContext) {
    let context = server_context.clone();
    let make_service = make_service_fn(move |_| {
        let context = context.clone();
        let service = service_fn(move |req| {
            handle_request(context.clone(), req)
        });
        async move { Ok::<_, Infallible>(service) }
    });
    Server::bind(&socket).serve(make_service).await.unwrap();
}

pub async fn make_request(request: SerdeHttpRequest, socket: String) -> SerdeHttpResponse {
    let client = Client::new();
    let request = request.to_hyper_request(socket);
    let response = client.request(request).await.unwrap();
    let serde_response = SerdeHttpResponse::from_hyper_response(response).await;
    serde_response
}