use std::{collections::HashMap, str::FromStr, convert::Infallible, net::SocketAddr};

use hyper::{Request, Response, Body, body, HeaderMap, Version, StatusCode, header::{HeaderName, HeaderValue}, service::{make_service_fn, service_fn}, Server, Client, Method, Uri};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use tracing::{debug, error, Level};
use uuid::Uuid;

use crate::{message::{Message, MessageDirection, MetadataKind, NumId, Peer, StreamMetadata, StreamPayloadKind}, time};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SerdeHttpRequest {
    method: String,
    uri: String,
    version: String,
    headers: HashMap<String, Vec<String>>,
    body: Vec<u8>,
    for_testing: bool
}

impl SerdeHttpRequest {
    async fn from_hyper_request(request: Request<Body>, for_testing: bool) -> Result<Self, String> {
        let (parts, body) = request.into_parts();
        Ok(Self {
            method: parts.method.to_string(),
            uri: parts.uri.to_string(),
            version: version_to_string(parts.version),
            headers: drain_headers(parts.headers)?,
            body: body::to_bytes(body).await.map_err(|e| { e.to_string() })?.to_vec(),
            for_testing
        })
    }

    fn into_hyper_request(self, uri_prefix: String) -> Result<Request<Body>, String> {
        let mut request = Request::new(Body::from(self.body));
        *request.method_mut() = Method::from_str(&self.method).map_err(|e| e.to_string())?;
        *request.uri_mut() = Uri::from_str(&(uri_prefix + &self.uri)).map_err(|e| e.to_string())?;
        *request.version_mut() = string_to_version(self.version);
        *request.headers_mut() = reconstruct_header_map(self.headers)?;
        Ok(request)
    }

    pub fn method(&self) -> &str { &self.method }
    pub fn uri(&self) -> &str { &self.uri }
    pub fn body(&self) -> &[u8] { &self.body }
    pub fn set_uri(&mut self, uri: String) { self.uri = uri; }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SerdeHttpResponse {
    body: Vec<u8>,
    status_code: u16,
    version: String,
    headers: HashMap<String, Vec<String>>,
}

impl SerdeHttpResponse {
    async fn from_hyper_response(response: Response<Body>) -> Result<Self, String> {
        let (parts, body) = response.into_parts();
        Ok(Self {
            status_code: parts.status.as_u16(),
            version: version_to_string(parts.version),
            headers: drain_headers(parts.headers)?,
            body: body::to_bytes(body).await.map_err(|e| { e.to_string() })?.to_vec()
        })
    }

    fn into_hyper_response(self) -> Response<Body> {
        let Self { body, status_code, version, headers } = self;
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = match StatusCode::from_u16(status_code) {
            Ok(status_code) => status_code,
            Err(e) => return construct_error_response(e.to_string(), version).into_hyper_response()
        };
        *response.headers_mut() = match reconstruct_header_map(headers) {
            Ok(headers) => headers,
            Err(e) => return construct_error_response(e.to_string(), version).into_hyper_response()
        };
        *response.version_mut() = string_to_version(version);
        response
    }

    fn builder(status_code: u16, version: String) -> Self {
        Self {
            body: Vec::with_capacity(0),
            status_code,
            version,
            headers: HashMap::new()
        }
    }

    fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(String::from(key), vec![String::from(value)]);
        self
    }

    fn body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
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

fn drain_headers(mut header_map: HeaderMap) -> Result<HashMap<String, Vec<String>>, String> {
    let mut headers: HashMap<String, Vec<String>> = HashMap::with_capacity(header_map.len());
    let mut curr_header_name = String::new();
    for (header_name, header_value) in header_map.drain() {
        if let Some(header_name) = header_name {
            curr_header_name = header_name.to_string();
            headers.insert(curr_header_name.clone(), Vec::new());
        }
        headers.get_mut(&curr_header_name).unwrap().push(header_value.to_str().map_err(|e| { e.to_string() })?.to_owned());
    }
    Ok(headers)
}

fn reconstruct_header_map(headers: HashMap<String, Vec<String>>) -> Result<HeaderMap, String> {
    let mut header_map = HeaderMap::new();
    for (header_name, header_values) in headers {
        for header_value in header_values {
            header_map.append(HeaderName::from_str(&header_name).map_err(|e| { e.to_string() })?, HeaderValue::from_str(&header_value).map_err(|e| { e.to_string() })?);
        }
    }
    Ok(header_map)
}

#[derive(Clone)]
pub struct ServerContext {
    pub to_staging: mpsc::UnboundedSender<(Message, mpsc::UnboundedSender<SerdeHttpResponse>)>
}

impl ServerContext {
    pub fn new(to_staging: mpsc::UnboundedSender<(Message, mpsc::UnboundedSender<SerdeHttpResponse>)>) -> Self {
        Self { to_staging }
    }
}

pub async fn handle_request(context: ServerContext, request: Request<Body>, testing: bool) -> Result<Response<Body>, Infallible> {
    // let epoch_now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
    // let timestamp = u128::from_str(request.headers().get("timestamp").unwrap().to_str().unwrap()).unwrap();
    // debug!(elapsed = epoch_now - timestamp, "Request latency millis");
    time!({
    let request_version = request.version();
    let mut request = match time!( { SerdeHttpRequest::from_hyper_request(request, testing).await }, Some(Level::DEBUG)) {
        Ok(request) => request,
        Err(e) => return Ok(construct_hyper_error_response(e, request_version, 400))
    };
    debug!(uri = request.uri());
    let (host_name, path) = get_host_name_and_path(request.uri());
    if host_name.trim_end().is_empty() {
        return Ok(construct_hyper_error_response(String::from("Invalid host name"), request_version, 400));
    }
    request.set_uri(path);
    let sm = Message::new(
        Peer::default(),
        NumId(Uuid::new_v4().as_u128()),
        None,
        MetadataKind::Stream(StreamMetadata::new(StreamPayloadKind::Request(request), host_name.clone())),
        MessageDirection::OneHop);
    let (tx, mut rx) = mpsc::unbounded_channel();
    context.to_staging.send((sm, tx)).ok();
    let response = match rx.recv().await {
        Some(response) => response,
        None => {
            debug!("SMP to http channel closed");
            return Ok(construct_hyper_error_response(String::from("Request timed out/p2p daemon terminated"), request_version, 500))
        }
    };
    Ok(response.into_hyper_response())
    }, Some(Level::DEBUG))
}

pub async fn tcp_listen(socket: SocketAddr, server_context: ServerContext) {
    let context = server_context.clone();
    let make_service = make_service_fn(move |_| {
        let context = context.clone();
        let service = service_fn(move |req| {
            handle_request(context.clone(), req, false)
        });
        async move { Ok::<_, Infallible>(service) }
    });
    Server::bind(&socket).serve(make_service).await.unwrap();
}

pub async fn make_request(request: SerdeHttpRequest, socket: &str) -> SerdeHttpResponse {
    let client = Client::new();
    let (request_version, for_testing) = (request.version.clone(), request.for_testing);
    let hyper_request = match request.into_hyper_request(String::from("http://") + socket) { Ok(request) => request, Err(e) => return construct_error_response(e.to_string(), request_version) };
    debug!("{:?}", hyper_request);
    if for_testing { 
        return SerdeHttpResponse::builder(200, request_version).body(vec![128u8; 1023])
    }
    time!({ let response = match client.request(hyper_request).await { Ok(response) => response, Err(e) => return construct_error_response(e.to_string(), request_version) };
    match SerdeHttpResponse::from_hyper_response(response).await {
        Ok(response) => response,
        Err(e) => construct_error_response(e, request_version)
    }
    }, Some(Level::DEBUG))
}

pub fn construct_error_response(error: String, request_version: String) -> SerdeHttpResponse {
    error!(error);
    let body = format!("<h1>{}</h1>", error);
    SerdeHttpResponse::builder(500, request_version)
        .header("Content-Type", "text/html")
        .header("Content-Length", &body.len().to_string())
        .body(body.into_bytes())
}

pub fn construct_hyper_error_response(error: String, request_version: Version, status_code: u16) -> Response<Body> {
    Response::builder()
        .status(status_code)
        .version(request_version)
        .header("Content-Type", "text/plain")
        .header("Content-Length", &error.len().to_string())
        .body(Body::from(error))
        .unwrap()
}

fn get_host_name_and_path(uri: &str) -> (String, String) {
    let mut path = String::new();
    let mut host_name = String::new();
    for (i, part) in uri.split('/').enumerate() {
        if let Some(c) = part.chars().next() {
            if c == '~' && i == 1 {
                host_name = part.to_owned().drain(1..).collect();
                continue;
            }
        }
        path += part;
        path += "/";
    }
    path.pop();
    (host_name, path)
}