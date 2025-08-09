use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::header::{ACCEPT, AUTHORIZATION};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    env, fs,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
struct Config {
    owner: String,
    repo: String,
    subpath: String,     // e.g. tools/verifier
    tags_prefix: String, // e.g. "v"
    out_path: PathBuf,
    parallel: usize,
    max_tags: Option<usize>, // optional limit for testing
}

impl Default for Config {
    fn default() -> Self {
        Self {
            owner: "matter-labs".into(),
            repo: "zksync-airbender".into(),
            subpath: "tools/verifier".into(),
            tags_prefix: "v".into(),
            out_path: PathBuf::from("airbender_verifier_index.json"),
            parallel: 16,
            max_tags: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct TagItem {
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum GhItemType {
    File,
    Dir,
}

#[derive(Debug, Deserialize)]
struct GhContentItem {
    name: String,
    path: String,
    #[serde(rename = "type")]
    kind: GhItemType,
}

#[derive(Debug, Serialize)]
struct Output {
    source: String,
    fetched_at: chrono::DateTime<chrono::Utc>,
    items: BTreeMap<String, OutputItem>,
}

#[derive(Debug, Serialize)]
struct OutputItem {
    value: String,
    url: String,
    description: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut cfg = Config::default();
    // Minimal CLI parsing
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--owner" => cfg.owner = args.next().context("--owner value")?,
            "--repo" => cfg.repo = args.next().context("--repo value")?,
            "--subpath" => cfg.subpath = args.next().context("--subpath value")?,
            "--prefix" => cfg.tags_prefix = args.next().context("--prefix value")?,
            "--out" => cfg.out_path = PathBuf::from(args.next().context("--out value")?),
            "--parallel" => {
                cfg.parallel = args
                    .next()
                    .context("--parallel value")?
                    .parse()
                    .context("--parallel usize")?
            }
            "--max-tags" => {
                cfg.max_tags = Some(
                    args.next()
                        .context("--max-tags value")?
                        .parse()
                        .context("--max-tags usize")?,
                )
            }
            _ => eprintln!("Unknown arg: {a}"),
        }
    }

    let client = reqwest::Client::builder()
        .user_agent(format!(
            "zk-wtf-airbender (+https://github.com/{}/{})",
            cfg.owner, cfg.repo
        ))
        .build()?;

    let tags = list_tags(&client, &cfg).await?;
    println!("Scanning {} tags", tags.len());

    // For each tag, list JSON files in subpath, then fetch & parse each
    let sem = Arc::new(Semaphore::new(cfg.parallel.max(1)));
    let mut tag_entries: HashMap<String, OutputItem> = HashMap::new();

    for tag in tags {
        let json_files = list_json_files_for_tag(&client, &cfg, &tag).await?;
        if json_files.is_empty() {
            // Some tags may not have the path yet — that's fine.
            continue;
        }

        let mut tasks = FuturesUnordered::new();
        for item in json_files {
            let client = client.clone();
            let cfg = cfg.clone();
            let tag_clone = tag.clone();
            let sem = sem.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore");
                fetch_and_extract(&client, &cfg, &tag_clone, &item).await
            }));
        }

        let mut entries: HashMap<String, OutputItem> = HashMap::new();
        while let Some(res) = tasks.next().await {
            match res? {
                Ok(entry) => entries.extend(entry),
                Err(e) => eprintln!("[warn] {tag}: {e:#}"),
            }
        }

        if !entries.is_empty() {
            tag_entries.extend(entries);
        }
    }
    let mut sorted: Vec<_> = tag_entries.into_iter().collect();
    sorted.sort_by(|(a, _), (b, _)| a.cmp(b));
    let sorted = sorted
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();

    let out = Output {
        source: format!("{}/{}/{}", cfg.owner, cfg.repo, cfg.subpath),
        fetched_at: Utc::now(),
        items: sorted,
    };

    if let Some(parent) = cfg.out_path.parent() {
        fs::create_dir_all(parent).ok();
    }
    fs::write(&cfg.out_path, serde_json::to_vec_pretty(&out)?)
        .with_context(|| format!("writing {}", cfg.out_path.display()))?;

    println!("Wrote {}", cfg.out_path.display());
    Ok(())
}

async fn list_tags(client: &reqwest::Client, cfg: &Config) -> Result<Vec<String>> {
    // GET /repos/{owner}/{repo}/tags?per_page=100&page=N
    let mut page = 1usize;
    let per_page = 100usize;
    let mut out: Vec<String> = Vec::new();

    loop {
        let url = format!(
            "https://api.github.com/repos/{}/{}/tags?per_page={}&page={}",
            cfg.owner, cfg.repo, per_page, page
        );
        let mut req = client
            .get(&url)
            .header(ACCEPT, "application/vnd.github+json");
        if let Ok(token) = env::var("GITHUB_TOKEN") {
            if !token.is_empty() {
                req = req.header(AUTHORIZATION, format!("Bearer {}", token));
            }
        }
        let resp = req.send().await?.error_for_status()?;
        let batch: Vec<TagItem> = resp.json().await?;
        if batch.is_empty() {
            break;
        }
        for t in batch {
            if t.name.starts_with(&cfg.tags_prefix) {
                out.push(t.name);
            }
        }
        if let Some(max) = cfg.max_tags {
            if out.len() >= max {
                out.truncate(max);
                break;
            }
        }
        page += 1;
    }

    if out.is_empty() {
        return Err(anyhow!("No tags with prefix '{}'", cfg.tags_prefix));
    }
    Ok(out)
}

async fn list_json_files_for_tag(
    client: &reqwest::Client,
    cfg: &Config,
    tag: &str,
) -> Result<Vec<GhContentItem>> {
    // GET /repos/{owner}/{repo}/contents/{path}?ref={tag}
    let url = format!(
        "https://api.github.com/repos/{}/{}/contents/{}?ref={}",
        cfg.owner, cfg.repo, cfg.subpath, tag
    );
    let mut req = client
        .get(&url)
        .header(ACCEPT, "application/vnd.github+json");
    if let Ok(token) = env::var("GITHUB_TOKEN") {
        if !token.is_empty() {
            req = req.header(AUTHORIZATION, format!("Bearer {}", token));
        }
    }
    let resp = req.send().await?;
    if resp.status().as_u16() == 404 {
        return Ok(vec![]);
    } // path may not exist in this tag
    let items: Vec<GhContentItem> = resp.error_for_status()?.json().await?;
    Ok(items
        .into_iter()
        .filter(|i| matches!(i.kind, GhItemType::File) && i.name.ends_with(".json"))
        .collect())
}

async fn fetch_and_extract(
    client: &reqwest::Client,
    cfg: &Config,
    tag: &str,
    item: &GhContentItem,
) -> Result<HashMap<String, OutputItem>> {
    let raw_url = format!(
        "https://raw.githubusercontent.com/{}/{}/{}/{}",
        cfg.owner, cfg.repo, tag, item.path
    );
    let user_url = format!(
        "https://github.com/{}/{}/blob/{}/{}",
        cfg.owner, cfg.repo, tag, item.path
    );
    let mut req = client.get(&raw_url);
    if let Ok(token) = env::var("GITHUB_TOKEN") {
        if !token.is_empty() {
            req = req.header(AUTHORIZATION, format!("Bearer {}", token));
        }
    }
    let text = req.send().await?.error_for_status()?.text().await?;

    let val: Value =
        serde_json::from_str(&text).with_context(|| format!("{}: invalid JSON", item.path))?;

    let bytecode = find_string_by_key(&val, "bytecode_hash_hex");
    let params = find_string_by_key(&val, "params_hex");

    let key = format!("{}/{}", tag, item.name);

    let mut result = HashMap::new();
    if let Some(bytecode) = &bytecode {
        result.insert(
            format!("{}/bytecode", key),
            OutputItem {
                value: bytecode.clone(),
                url: user_url.clone(),
                description: format!(
                    "Bytecode hash for {} for tag {} in {}",
                    item.name, tag, cfg.repo
                ),
            },
        );
    }
    if let Some(params) = &params {
        result.insert(
            format!("{}/params", key),
            OutputItem {
                value: params.clone(),
                url: user_url.clone(),
                description: format!(
                    "Verification params hash for {} for tag {} in {}",
                    item.name, tag, cfg.repo
                ),
            },
        );
    }
    Ok(result)
}

fn find_string_by_key(v: &Value, key: &str) -> Option<String> {
    match v {
        Value::Object(map) => {
            for (k, vv) in map {
                if k == key {
                    if let Value::String(s) = vv {
                        return Some(s.clone());
                    }
                }
                if let Some(found) = find_string_by_key(vv, key) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(arr) => {
            for x in arr {
                if let Some(found) = find_string_by_key(x, key) {
                    return Some(found);
                }
            }
            None
        }
        _ => None,
    }
}
