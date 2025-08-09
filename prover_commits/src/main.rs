use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, env, fs, path::PathBuf};

// ---- Config ----
#[derive(Debug, Clone)]
struct Config {
    owner: String,
    repo: String,
    base_path: String, // e.g. "prover/data/historical_data"
    branch: String,    // e.g. "main"
    out_path: PathBuf, // e.g. "commitments.json"
    parallel: usize,
}

#[derive(Debug, Serialize)]
struct OutputItem {
    value: String,
    url: String,
    description: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            owner: "matter-labs".into(),
            repo: "zksync-era".into(),
            base_path: "prover/data/historical_data".into(),
            branch: "main".into(),
            out_path: PathBuf::from("commitments.json"),
            parallel: 16,
        }
    }
}

// ---- GitHub API types ----
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")] // type: "file" | "dir"
enum GhItemType {
    File,
    Dir,
}

#[derive(Debug, Deserialize)]
struct GhContentItem {
    name: String,
    #[serde(rename = "type")]
    kind: GhItemType,
}

static HASH_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^0x[0-9a-fA-F]{64}$").unwrap());

#[tokio::main]
async fn main() -> Result<()> {
    let mut cfg = Config::default();
    // Very light CLI parsing to keep deps small
    // Usage: zk-wtf-gh [--owner O] [--repo R] [--base-path P] [--branch B] [--out FILE]
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--owner" => cfg.owner = args.next().context("--owner requires value")?,
            "--repo" => cfg.repo = args.next().context("--repo requires value")?,
            "--base-path" => cfg.base_path = args.next().context("--base-path requires value")?,
            "--branch" => cfg.branch = args.next().context("--branch requires value")?,
            "--out" => cfg.out_path = PathBuf::from(args.next().context("--out requires value")?),
            "--parallel" => {
                cfg.parallel = args
                    .next()
                    .context("--parallel requires value")?
                    .parse()
                    .context("--parallel must be usize")?
            }
            _ => eprintln!("Unknown arg: {arg}"),
        }
    }

    let client = reqwest::Client::builder()
        .user_agent(format!(
            "zk-wtf-gh (+https://github.com/{}/{})",
            cfg.owner, cfg.repo
        ))
        .build()?;

    // 1) List subdirectories under base_path
    let subdirs = list_subdirs(&client, &cfg).await?;
    println!("Found {} subdirectories", subdirs.len());

    // 2) For each subdir, fetch commitment.json (if present) and extract hashes
    let mut futs = FuturesUnordered::new();
    for dir in subdirs {
        let client = client.clone();
        let cfg = cfg.clone();
        futs.push(tokio::spawn(async move {
            let res = fetch_commitment_and_extract(&client, &cfg, &dir).await;
            (dir, res)
        }));
    }

    let mut all_entries = HashMap::new();
    while let Some((dir, res)) = futs.next().await.transpose()? {
        match res {
            Ok(hs) if !hs.is_empty() => all_entries.extend(hs),
            Ok(_) => { /* empty commitment.json or no hashes */ }
            Err(e) => {
                eprintln!("[warn] {dir}: {e:#}");
            }
        }
    }

    let mut sorted: Vec<_> = all_entries.into_iter().collect();
    sorted.sort_by(|(a, _), (b, _)| a.cmp(b));
    let sorted = sorted
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();

    // sort entries by directory name

    let out = json!({
        "source": format!("{}/{}/{}/{}", cfg.owner, cfg.repo, cfg.base_path, cfg.branch),
        "fetched_at": Utc::now(),
        "items": sorted,
    });

    // Write
    if let Some(parent) = cfg.out_path.parent() {
        fs::create_dir_all(parent).ok();
    }
    fs::write(&cfg.out_path, serde_json::to_vec_pretty(&out)?)
        .with_context(|| format!("writing {}", cfg.out_path.display()))?;

    println!("Wrote {}", cfg.out_path.display());
    Ok(())
}

async fn list_subdirs(client: &reqwest::Client, cfg: &Config) -> Result<Vec<String>> {
    // GitHub contents API for the base_path
    // GET /repos/{owner}/{repo}/contents/{path}?ref={branch}
    let url = format!(
        "https://api.github.com/repos/{}/{}/contents/{}?ref={}",
        cfg.owner, cfg.repo, cfg.base_path, cfg.branch
    );
    let mut req = client
        .get(&url)
        .header(ACCEPT, "application/vnd.github+json");
    if let Ok(token) = env::var("GITHUB_TOKEN") {
        if !token.trim().is_empty() {
            req = req.header(AUTHORIZATION, format!("Bearer {}", token));
        }
    }
    let resp = req.send().await?.error_for_status()?;
    let items: Vec<GhContentItem> = resp.json().await?;
    let mut out = Vec::new();
    for it in items.into_iter() {
        if let GhItemType::Dir = it.kind {
            out.push(it.name);
        }
    }
    if out.is_empty() {
        return Err(anyhow!(
            "No subdirectories under {}/{}",
            cfg.repo,
            cfg.base_path
        ));
    }
    Ok(out)
}

async fn fetch_commitment_and_extract(
    client: &reqwest::Client,
    cfg: &Config,
    dir: &str,
) -> Result<HashMap<String, OutputItem>> {
    // Check directory contents for commitment.json to avoid 404s
    let list_url = format!(
        "https://api.github.com/repos/{}/{}/contents/{}/{}?ref={}",
        cfg.owner, cfg.repo, cfg.base_path, dir, cfg.branch
    );
    let mut req = client
        .get(&list_url)
        .header(ACCEPT, "application/vnd.github+json");
    if let Ok(token) = env::var("GITHUB_TOKEN") {
        if !token.trim().is_empty() {
            req = req.header(AUTHORIZATION, format!("Bearer {}", token));
        }
    }
    let resp = req.send().await?.error_for_status()?;
    let items: Vec<GhContentItem> = resp.json().await?;

    let has_commitment = items
        .iter()
        .any(|i| matches!(i.kind, GhItemType::File) && i.name == "commitments.json");
    if !has_commitment {
        return Ok(Default::default()); // No commitment.json in this dir
    }

    // Fetch raw commitment.json directly (faster than content API b64 decoding)
    let raw_url = format!(
        "https://raw.githubusercontent.com/{}/{}/{}/{}/{}/commitments.json",
        cfg.owner, cfg.repo, cfg.branch, cfg.base_path, dir
    );
    let user_url = format!(
        "https://github.com/{}/{}/blob/{}/{}/{}/commitments.json",
        cfg.owner, cfg.repo, cfg.branch, cfg.base_path, dir
    );
    let mut req = client.get(&raw_url);
    if let Ok(token) = env::var("GITHUB_TOKEN") {
        if !token.trim().is_empty() {
            req = req.header(AUTHORIZATION, format!("Bearer {}", token));
        }
    }
    let text = req.send().await?.error_for_status()?.text().await?;

    let val: Value =
        serde_json::from_str(&text).with_context(|| format!("{}: invalid JSON", raw_url))?;

    let mut hashes = HashMap::new();
    collect_hashes(dir, &val, &mut hashes);

    let result = hashes
        .into_iter()
        .map(|(k, v)| {
            (
                k.clone(),
                OutputItem {
                    value: v,
                    url: user_url.clone(),
                    description: format!("Boojum Hash for {} version {} in {}", k, dir, cfg.repo),
                },
            )
        })
        .collect();

    Ok(result)
}

fn collect_hashes(prefix: &str, v: &Value, out: &mut HashMap<String, String>) {
    match v {
        Value::String(s) => {
            if HASH_RE.is_match(s) {
                out.insert(prefix.to_string(), s.clone());
            }
        }
        Value::Array(arr) => {
            for (i, x) in arr.iter().enumerate() {
                collect_hashes(&format!("{}[{}]", prefix, i), x, out);
            }
        }
        Value::Object(map) => {
            for (k, x) in map {
                let new_prefix = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                collect_hashes(&new_prefix, x, out);
            }
        }
        _ => {}
    }
}
