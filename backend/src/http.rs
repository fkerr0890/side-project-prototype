use std::{collections::HashMap, str::FromStr, convert::Infallible, net::SocketAddr};

use hyper::{Request, Response, Body, body, HeaderMap, Version, StatusCode, header::{HeaderName, HeaderValue}, service::{make_service_fn, service_fn}, Server, Client, Method, Uri};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::{message::{Message, SearchMessage, StreamMessage, StreamMessageInnerKind, StreamMessageKind}, message_processing::stream::StreamResponseType, node::EndpointPair};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SerdeHttpRequest {
    method: String,
    uri: String,
    version: String,
    headers: HashMap<String, Vec<String>>,
    body: Vec<u8>
}

impl SerdeHttpRequest {
    async fn from_hyper_request(request: Request<Body>) -> Result<Self, String> {
        let (parts, body) = request.into_parts();
        Ok(Self {
            method: parts.method.to_string(),
            uri: parts.uri.to_string(),
            version: version_to_string(parts.version),
            headers: drain_headers(parts.headers)?,
            body: body::to_bytes(body).await.map_err(|e| { e.to_string() })?.to_vec()
        })
    }

    fn to_hyper_request(self, uri_prefix: String) -> Result<Request<Body>, String> {
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

    fn to_hyper_response(self) -> Response<Body> {
        let mut response = Response::new(Body::from(self.body));
        *response.status_mut() = match StatusCode::from_u16(self.status_code) {
            Ok(status_code) => status_code,
            Err(e) => return construct_error_response(e.to_string(), self.version.clone()).to_hyper_response()
        };
        *response.version_mut() = string_to_version(self.version.clone());
        *response.headers_mut() = match reconstruct_header_map(self.headers) {
            Ok(headers) => headers,
            Err(e) => return construct_error_response(e.to_string(), self.version).to_hyper_response()
        };
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
    pub to_srp: mpsc::UnboundedSender<SearchMessage>,
    pub to_smp: mpsc::UnboundedSender<StreamMessage>,
    pub tx_to_smp: mpsc::UnboundedSender<mpsc::UnboundedSender<StreamResponseType>>
}

impl ServerContext {
    pub fn new(to_srp: mpsc::UnboundedSender<SearchMessage>, to_smp: mpsc::UnboundedSender<StreamMessage>, tx_to_smp: mpsc::UnboundedSender<mpsc::UnboundedSender<StreamResponseType>>) -> Self {
        Self {
            to_srp,
            to_smp,
            tx_to_smp
        }
    }
}

async fn handle_request(context: ServerContext, request: Request<Body>) -> Result<Response<Body>, Infallible> {
    let request_version = request.version().clone();
    let mut request = match SerdeHttpRequest::from_hyper_request(request).await {
        Ok(request) => request,
        Err(e) => {
            return Ok(Response::builder()
                .status(400)
                .version(request_version)
                .header("Content-Type", "text/plain")
                .header("Content-Length", e.len().to_string())
                .body(Body::from(e))
                .unwrap())
        }
    };
    debug!(uri = request.uri());
    let (host_name, path) = get_host_name_and_path(request.uri());
    request.set_uri(path);
    let search_request = SearchMessage::initial_search_request(host_name.to_owned(), true, None);
    let payload = match bincode::serialize(&request) {
        Ok(request) => request,
        Err(e) => {
            let e = (*e).to_string();
            return Ok(Response::builder()
                .status(400)
                .version(request_version)
                .header("Content-Type", "text/plain")
                .header("Content-Length", e.len().to_string())
                .body(Body::from(e))
                .unwrap())
        }
    };
    let mut sm = StreamMessage::new(
        host_name.to_owned(),
        search_request.id(),
        StreamMessageKind::Resource(StreamMessageInnerKind::Request),
        payload);
    sm.set_sender(EndpointPair::default_socket());
    let (tx, mut rx) = mpsc::unbounded_channel();
    context.tx_to_smp.send(tx).ok();
    context.to_smp.send(sm).ok();
    context.to_srp.send(search_request).ok();
    let response = match rx.recv().await {
        Some(response) => response,
        None => {
            error!("SMP to http channel closed");
            return Ok(Response::builder()
                .status(500)
                .version(request_version)
                .header("Content-Type", "text/plain")
                .header("Content-Length", "21")
                .body(Body::from("p2p daemon terminated"))
                .unwrap())
        }
    };
    Ok(response.unwrap_http().to_hyper_response())
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

pub async fn make_request(request: SerdeHttpRequest, socket: &str) -> SerdeHttpResponse {
    let client = Client::new();
    let request_version = request.version.clone();
    let hyper_request = match request.to_hyper_request(String::from("http://") + socket) { Ok(request) => request, Err(e) => return construct_error_response(e.to_string(), request_version) };
    debug!("{:?}", hyper_request);
    let response = match client.request(hyper_request).await { Ok(response) => response, Err(e) => return construct_error_response(e.to_string(), request_version) };
    match SerdeHttpResponse::from_hyper_response(response).await {
        Ok(response) => response,
        Err(e) => construct_error_response(e, request_version)
    }
}

pub fn construct_error_response(error: String, request_version: String) -> SerdeHttpResponse {
    error!(error);
    let body = format!("<h1>{}</h1>", error);
    SerdeHttpResponse::builder(500, request_version)
        .header("Content-Type", "text/html")
        .header("Content-Length", &body.len().to_string())
        .body(body.into_bytes())
}

fn get_host_name_and_path(uri: &str) -> (String, String) {
    let mut path = String::new();
    let mut host_name = String::new();
    for (i, part) in uri.split("/").enumerate() {
        if let Some(c) = part.chars().nth(0) {
            if c == '~' && i == 1 {
                host_name = part.to_owned().drain(1..).collect();
                continue;
            }
        }
        path += part;
        path += "/";
    }
    path.pop();
    return (host_name, path)
}