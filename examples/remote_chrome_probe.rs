use gpt2omo::orca::{BrowserDriverConfig, BrowserDriverKind};
use gpt2omo::{BrowserPool, BrowserReachability, LegacyAccountConfig};
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn isolated_roots() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let root = tempfile::tempdir().expect("failed to create tempdir");
    let bridge = root.path().join("bridge");
    let mount = root.path().join("mount");
    fs::create_dir_all(&bridge).expect("failed to create bridge dir");
    fs::create_dir_all(&mount).expect("failed to create mount dir");
    (root, bridge, mount)
}

async fn fetch_cdp_targets(endpoint: &str) -> anyhow::Result<Vec<Value>> {
    let client = reqwest::Client::new();
    let url = format!("{}/json/list", endpoint.trim_end_matches('/'));
    let response = client.get(&url).send().await?;
    let targets: Vec<Value> = response.json().await?;
    Ok(targets)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = "http://127.0.0.1:9333";
    println!("=== Remote Chrome Probe ===");
    println!("Connecting to CDP endpoint: {}", endpoint);

    // 1. Setup isolated bridge and mount directories
    let (_root, bridge, mount) = isolated_roots();
    let accounts_json = r#"{
  "version": 1,
  "accounts": [{
    "id": "remote-chrome",
    "browser": {
      "driver": "chrome",
      "instance": "remote-chrome",
      "launch_mode": "attach_only",
      "cdp_endpoint": "http://127.0.0.1:9333"
    }
  }]
}"#;
    fs::write(bridge.join("accounts.json"), accounts_json)?;

    // 2. Fetch /json/list (before)
    let targets_before = fetch_cdp_targets(endpoint).await?;
    let urls_before: Vec<String> = targets_before
        .iter()
        .filter_map(|t| t.get("url").and_then(Value::as_str))
        .map(String::from)
        .collect();
    let ids_before: HashSet<String> = targets_before
        .iter()
        .filter_map(|t| t.get("id").and_then(Value::as_str))
        .map(String::from)
        .collect();

    println!("\n[CDP /json/list before] Total targets: {}", targets_before.len());
    for url in &urls_before {
        println!("  - {}", url);
    }

    // Save evidence pages-before.json
    let evidence_dir = PathBuf::from(".omo/evidence/remote-chrome-probe");
    fs::create_dir_all(&evidence_dir)?;
    fs::write(
        evidence_dir.join("pages-before.json"),
        serde_json::to_string_pretty(&targets_before)?,
    )?;

    // 3. Build BrowserPool
    let pool = BrowserPool::new(
        &bridge,
        &mount,
        LegacyAccountConfig::default(),
        BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Chrome), None, "active", None),
    );

    // 4. Call pool.health("remote-chrome")
    let health = pool.health("remote-chrome").await;
    println!("\n[BrowserPool Health]");
    println!("  Account ID: {}", health.account_id);
    println!("  Instance: {}", health.instance);
    println!("  Reachability: {:?}", health.reachability);
    println!("  Login state: {:?}", health.login_state);
    let health_detail = health.detail.clone().unwrap_or_else(|| "none".to_string());
    println!("  Detail: {}", health_detail);

    assert_eq!(
        health.reachability,
        BrowserReachability::Reachable,
        "Browser reachability must be Reachable"
    );

    // 5. Open ChatGPT login page via BrowserPool
    println!("\n[Opening ChatGPT Login Page via BrowserPool]");
    let handle = pool.open_chatgpt_login_page("remote-chrome").await?;
    let created_page_id = handle.page_id.clone();
    println!("  Successfully created page id: {}", created_page_id);

    // 6. Fetch /json/list (after)
    let targets_after = fetch_cdp_targets(endpoint).await?;
    let urls_after: Vec<String> = targets_after
        .iter()
        .filter_map(|t| t.get("url").and_then(Value::as_str))
        .map(String::from)
        .collect();

    println!("\n[CDP /json/list after] Total targets: {}", targets_after.len());
    for url in &urls_after {
        println!("  - {}", url);
    }

    // Save evidence pages-after.json
    fs::write(
        evidence_dir.join("pages-after.json"),
        serde_json::to_string_pretty(&targets_after)?,
    )?;

    // Find the new page(s)
    let new_targets: Vec<&Value> = targets_after
        .iter()
        .filter(|t| {
            t.get("id")
                .and_then(Value::as_str)
                .map(|id| !ids_before.contains(id))
                .unwrap_or(false)
        })
        .collect();

    println!("\n[Delta / New Target(s)] Count: {}", new_targets.len());
    for target in &new_targets {
        let id = target.get("id").and_then(Value::as_str).unwrap_or("");
        let url = target.get("url").and_then(Value::as_str).unwrap_or("");
        println!("  - ID: {}, URL: {}", id, url);
    }

    assert_eq!(
        new_targets.len(),
        1,
        "Expected exactly one new target created by open_chatgpt_login_page"
    );
    let new_url = new_targets[0]
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(
        new_url.contains("chatgpt.com"),
        "Expected new target URL to contain chatgpt.com, got '{}'",
        new_url
    );

    // 7. Close ONLY the page created by this probe via pool.close()
    println!("\n[Closing Created Page]");
    println!("  Closing page binding: {:?}", handle.binding());
    pool.close(&handle.binding()).await?;
    println!("  Page close request sent.");

    // 8. Await removal of the closed target from /json/list and print final count
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut targets_final;
    loop {
        targets_final = fetch_cdp_targets(endpoint).await?;
        let still_present = targets_final
            .iter()
            .any(|t| t.get("id").and_then(Value::as_str) == Some(&created_page_id));
        if !still_present {
            break;
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for target {} to close", created_page_id);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = pool.close(&handle.binding()).await;
    }

    println!(
        "\n[CDP /json/list final] Final target count: {} (initial was {})",
        targets_final.len(),
        targets_before.len()
    );
    assert_eq!(
        targets_final.len(),
        targets_before.len(),
        "Target count after closing should match initial target count"
    );

    println!("\nProbe completed successfully!");
    Ok(())
}
