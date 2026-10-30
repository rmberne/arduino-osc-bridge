use crate::osc_manager::ProjectState;
use axum::response::sse::{Event, KeepAlive};
use axum::{
    extract::State,
    response::{Html, Sse},
    routing::get,
    Router,
};
use serde::Serialize;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tracing::info;

const INDEX_HTML: &str = include_str!("../static/index.html");

#[derive(Serialize)]
struct DisplayStateDTO<'a> {
    name: &'a str,
    bpm: f32,
    is_playing: bool,
}

impl<'a> From<&'a ProjectState> for DisplayStateDTO<'a> {
    fn from(state: &'a ProjectState) -> Self {
        Self {
            name: &state.name,
            bpm: state.bpm,
            is_playing: state.is_playing,
        }
    }
}

async fn ping() -> &'static str {
    "pong"
}

async fn index_html() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn sse_handler(
    State(rx): State<watch::Receiver<ProjectState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = WatchStream::new(rx).map(|state| {
        let dto = DisplayStateDTO::from(&state);
        let json = serde_json::to_string(&dto).unwrap_or_default();
        Ok(Event::default().data(json))
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

fn create_app(state: watch::Receiver<ProjectState>) -> Router {
    Router::new()
        .route("/", get(index_html))
        .route("/events", get(sse_handler))
        .route("/ping", get(ping))
        .with_state(state)
}

pub async fn run_web_server(mut shutdown: watch::Receiver<bool>) -> std::io::Result<()> {
    let (_state_tx, state_rx) = watch::channel(ProjectState::default());
    let app = create_app(state_rx);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %addr, "Web server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
            info!("Stopping web server");
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{header, Request, StatusCode},
    };
    use tower::ServiceExt; // Provides the `oneshot` method on `Router`

    #[tokio::test]
    async fn serves_index_html() {
        let (_, state_rx) = watch::channel(ProjectState::default());
        let app = create_app(state_rx);

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(body_str.contains("<title>Stage Monitor</title>"));
    }

    #[tokio::test]
    async fn ping_returns_200_and_correct_body() {
        let (_, state_rx) = watch::channel(ProjectState::default());
        let app = create_app(state_rx);

        // Build a mock HTTP GET request to "/ping"
        let response = app
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();

        // Assert HTTP status code
        assert_eq!(response.status(), StatusCode::OK);

        // Convert the response body to bytes and assert string content
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        assert_eq!(&body_bytes[..], b"pong");
    }

    #[tokio::test]
    async fn events_streams_initial_and_updated_project_state() {
        let (state_tx, state_rx) = watch::channel(ProjectState::default());
        let app = create_app(state_rx);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/event-stream"
        );

        let mut body = response.into_body().into_data_stream();
        let initial = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let initial = String::from_utf8(initial.to_vec()).unwrap();
        assert!(initial.contains(r#""name":"Untitled Project""#));
        assert!(initial.contains(r#""bpm":120.0"#));
        assert!(initial.contains(r#""is_playing":false"#));

        state_tx
            .send(ProjectState {
                name: "Updated Project".to_string(),
                bpm: 128.0,
                is_playing: true,
            })
            .unwrap();

        let updated = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let updated = String::from_utf8(updated.to_vec()).unwrap();
        assert!(updated.contains(r#""name":"Updated Project""#));
        assert!(updated.contains(r#""bpm":128.0"#));
        assert!(updated.contains(r#""is_playing":true"#));
    }

    #[tokio::test]
    async fn sse_handler_streams_exact_events_and_ends_when_sender_closes() {
        let (state_tx, state_rx) = watch::channel(ProjectState::default());
        let response =
            axum::response::IntoResponse::into_response(sse_handler(State(state_rx)).await);
        let mut body = response.into_body().into_data_stream();

        let initial = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            initial.as_ref(),
            b"data: {\"name\":\"Untitled Project\",\"bpm\":120.0,\"is_playing\":false}\n\n"
        );

        state_tx
            .send(ProjectState {
                name: "Studio \"A\"".to_string(),
                bpm: 128.0,
                is_playing: true,
            })
            .unwrap();

        let updated = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            updated.as_ref(),
            b"data: {\"name\":\"Studio \\\"A\\\"\",\"bpm\":128.0,\"is_playing\":true}\n\n"
        );

        drop(state_tx);
        let stream_end = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .unwrap();
        assert!(stream_end.is_none());
    }
}
