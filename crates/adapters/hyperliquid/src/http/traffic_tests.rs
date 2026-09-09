use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test]
async fn shared_constructors_and_retry_attempts_pay_full_weight() {
    let first = HyperliquidRawHttpClient::new(HyperliquidEnvironment::Testnet, 10, None).unwrap();
    let second = HyperliquidRawHttpClient::new(HyperliquidEnvironment::Testnet, 10, None).unwrap();
    assert!(Arc::ptr_eq(&first.rest_limiter, &second.rest_limiter));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for status in ["429 Too Many Requests", "503 Service Unavailable", "200 OK"] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 8192];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 {status}\r\nRetry-After: 2\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let mut client = first;
    client.set_base_info_url(format!("http://{address}/info"));
    client.rest_limiter = Arc::new(WeightedLimiter::with_burst(1, 200, 10));
    let started = std::time::Instant::now();
    client
        .send_info_request_raw(&InfoRequest::meta())
        .await
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "Retry-After must survive header extraction"
    );
    assert_eq!(
        client.rest_limiter_snapshot().await.tokens,
        140,
        "three attempts each cost 20"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn surcharge_debt_reserve_and_shared_cooldown_are_preserved() {
    let limiter = Arc::new(WeightedLimiter::with_burst(600, 80, 10));
    assert!(limiter.acquire_bounded(70, false).await);
    let blocked = tokio::spawn({
        let limiter = limiter.clone();
        async move { limiter.acquire_bounded(20, false).await }
    });
    let started = std::time::Instant::now();
    assert!(limiter.acquire_bounded(1, true).await);
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "protective action must not queue behind ordinary traffic"
    );
    assert!(!blocked.is_finished());
    blocked.abort();
    limiter.debit_extra(100).await;
    assert_eq!(limiter.snapshot().await.tokens, 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), limiter.acquire_bounded(1, true))
            .await
            .expect("read debt must preserve protective capacity")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), limiter.acquire(1))
            .await
            .is_err(),
        "negative debt must not be discarded"
    );
    let limiter = WeightedLimiter::with_burst(600, 80, 10);
    limiter.cool_down(Duration::from_millis(100)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), limiter.acquire_bounded(1, true))
            .await
            .is_err()
    );
}
