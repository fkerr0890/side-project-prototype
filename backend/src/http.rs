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
        *response.status_mut() = StatusCode::from_u16(self.status_code).unwrap();
        *response.version_mut() = string_to_version(self.version);
        *response.headers_mut() = reconstruct_header_map(self.headers);
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
    let request_version = request.version().clone();
    let serde_request = match SerdeHttpRequest::from_hyper_request(request).await {
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
    for message in Message::initial_http_request(String::from("example"), serde_request).chunked() {
        context.egress.send(message).ok();
    }
    let mut rx = context.from_outbound_gateway.lock().await;
    let response = match rx.recv().await {
        Some(response) => response,
        None => {
            return Ok(Response::builder()
                .status(500)
                .version(request_version)
                .header("Content-Type", "text/plain")
                .header("Content-Length", "21")
                .body(Body::from("p2p daemon terminated"))
                .unwrap())
        }
    };
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
    let request_version = request.version.clone();
    let hyper_request = request.to_hyper_request(socket);
    let response = client.request(hyper_request).await.unwrap();
    match SerdeHttpResponse::from_hyper_response(response).await {
        Ok(response) => response,
        Err(e) => {
            let body = e.to_string();
            SerdeHttpResponse::builder(500, request_version)
                .header("Content-Type", "text/plain")
                .header("Content-Length", &body.len().to_string())
                .body(format!("<h1>{}</h1>", body).into_bytes())
        }
    }
}