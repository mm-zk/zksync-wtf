use alloy::{primitives::Address, providers::ProviderBuilder, sol};
use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::{env, fs, path::PathBuf, str::FromStr};

sol! {
    #[sol(rpc)]
    interface IBridgehub {
        function getAllZKChains() external view returns (address[]);
        function getAllZKChainChainIDs() external view returns (uint256[]);
        function getZKChain(uint256 _chainId) external view returns (address);

        function sharedBridge() external view returns (address);

        function admin() external view returns (address);

        function assetRouter() external view returns (address);



    }
}

#[derive(Debug, Clone)]
struct Config {
    out: PathBuf,
}

#[derive(Debug, Clone)]
struct Ecosystem {
    name: String,
    rpc: String,
    bridgehub: Address,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            out: PathBuf::from("contract_addresses.json"),
        }
    }
}

#[derive(Debug, Serialize)]
struct ChainItem {
    chain_id: String,
    zk_chain_address: String,
}

#[derive(Debug, Serialize)]
struct OutputItem {
    value: String,
    url: String,
    description: String,
}

#[derive(Debug, Serialize)]
struct Output {
    source: String,
    fetched_at: chrono::DateTime<chrono::Utc>,
    items: BTreeMap<String, OutputItem>,
}

async fn fetch_bridgehub_chains(
    rpc_url: &str,
    ecosystem: &str,
    bridgehub: Address,
    chain_mapping: &HashMap<String, String>,
) -> Result<HashMap<String, OutputItem>> {
    let provider = ProviderBuilder::new().on_http(rpc_url.parse()?);
    let hub = IBridgehub::new(bridgehub, provider.clone());

    // Try to get chain IDs first
    let chain_ids = hub.getAllZKChainChainIDs().call().await?;

    // Fetch each chain address
    let mut items = Vec::with_capacity(chain_ids._0.len());
    for id in chain_ids._0 {
        match hub.getZKChain(id).call().await {
            Ok(addr) => items.push(ChainItem {
                chain_id: id.to_string(),
                zk_chain_address: format!("0x{:x}", addr._0),
            }),
            Err(e) => {
                eprintln!("[warn] getZKChain({id}) failed: {e:#}");
            }
        }
    }

    let mut result: HashMap<String, OutputItem> = items
        .into_iter()
        .map(|item| {
            let chain_name = chain_mapping
                .get(&item.chain_id)
                .cloned()
                .unwrap_or_else(|| format!("chain_{}", item.chain_id));

            (
                format!(
                    "{} DiamondProxy - {} {}",
                    ecosystem, item.chain_id, chain_name
                ),
                OutputItem {
                    value: item.zk_chain_address.clone(),
                    url: format!("https://etherscan.io/address/{}", item.zk_chain_address),
                    description: format!("Diamond Proxy for {}", item.chain_id),
                },
            )
        })
        .collect();

    // insert bridgehub address as well
    result.insert(
        format!("{} Bridgehub", ecosystem),
        OutputItem {
            value: format!("0x{:x}", bridgehub),
            url: format!("https://etherscan.io/address/{}", bridgehub),
            description: "Bridgehub contract address".to_string(),
        },
    );
    let shared_bridge = hub.sharedBridge().call().await?;
    result.insert(
        format!("{} SharedBridge", ecosystem),
        OutputItem {
            value: format!("0x{:x}", shared_bridge._0),
            url: format!("https://etherscan.io/address/{}", shared_bridge._0),
            description: "Shared Bridge contract address".to_string(),
        },
    );

    let admin = hub.admin().call().await?;
    result.insert(
        format!("{} Admin", ecosystem),
        OutputItem {
            value: format!("0x{:x}", admin._0),
            url: format!("https://etherscan.io/address/{}", admin._0),
            description: "Admin contract address".to_string(),
        },
    );

    let asset_router = hub.assetRouter().call().await?;
    result.insert(
        format!("{} AssetRouter", ecosystem),
        OutputItem {
            value: format!("0x{:x}", asset_router._0),
            url: format!("https://etherscan.io/address/{}", asset_router._0),
            description: "Asset Router contract address".to_string(),
        },
    );

    Ok(result)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut cfg = Config::default();

    // Minimal CLI parsing
    // Usage: --rpc <URL> --address <0x...> --out <file> --label <name>
    let mut args = env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--out" => cfg.out = PathBuf::from(args.next().context("--out requires value")?),
            other => eprintln!("Unknown arg: {other}"),
        }
    }

    // Load chain mapping.
    let chain_mapping: HashMap<String, String> = {
        let file_path = "../data/chains.json";
        let data =
            fs::read_to_string(file_path).with_context(|| format!("reading file {}", file_path))?;
        let json: serde_json::Value =
            serde_json::from_str(&data).with_context(|| format!("parsing file {}", file_path))?;
        let items = json
            .get("items")
            .and_then(|v| v.as_object())
            .context("expected 'items' to be a JSON object")?;

        let mut chain_mapping = HashMap::new();
        for (key, value) in items {
            if let Some(name) = key.strip_prefix("chain_id for") {
                if let Some(id) = value.get("value").unwrap().as_str() {
                    chain_mapping.insert(id.to_owned(), name.trim().to_string());
                } else {
                    eprintln!("Warning: value for key '{}' is not an integer", key);
                }
            }
        }

        println!("Chain mapping: {:#?}", chain_mapping);
        chain_mapping
    };

    let configs = vec![
        Ecosystem {
            rpc: "https://rpc.era-gateway-stage.zksync.dev/".into(),
            bridgehub: Address::from_str("0x0000000000000000000000000000000000010002").unwrap(),
            name: "stage gateway".into(),
        },
        Ecosystem {
            rpc: "https://rpc.era-gateway-testnet.zksync.dev/".into(),
            bridgehub: Address::from_str("0x0000000000000000000000000000000000010002").unwrap(),
            name: "testnet gateway".into(),
        },
        Ecosystem {
            rpc: "https://rpc.era-gateway-mainnet.zksync.dev/".into(),
            bridgehub: Address::from_str("0x0000000000000000000000000000000000010002").unwrap(),
            name: "mainnet gateway".into(),
        },
        Ecosystem {
            rpc: "https://ethereum-sepolia-rpc.publicnode.com".into(),
            bridgehub: Address::from_str("0x236D1c3Ff32Bd0Ca26b72Af287E895627c0478cE").unwrap(),
            name: "stage".into(),
        },
        Ecosystem {
            rpc: "https://ethereum-sepolia-rpc.publicnode.com".into(),
            bridgehub: Address::from_str("0x35A54c8C757806eB6820629bc82d90E056394C92").unwrap(),
            name: "testnet".into(),
        },
        Ecosystem {
            rpc: "https://ethereum.publicnode.com".into(),
            bridgehub: Address::from_str("0x303a465B659cBB0ab36eE643eA362c509EEb5213").unwrap(),
            name: "mainnet".into(),
        },
    ];

    let mut all_items = HashMap::new();

    for cfg in configs {
        println!("Processing {}", cfg.name);
        let chain_items =
            fetch_bridgehub_chains(&cfg.rpc, &cfg.name, cfg.bridgehub, &chain_mapping).await?;
        all_items.extend(chain_items);
    }

    let mut sorted: Vec<_> = all_items.into_iter().collect();
    sorted.sort_by(|(a, _), (b, _)| a.cmp(b));
    let sorted = sorted
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();

    let out = Output {
        source: "bridgehub".to_string(),
        fetched_at: Utc::now(),
        items: sorted,
    };

    if let Some(parent) = cfg.out.parent() {
        fs::create_dir_all(parent).ok();
    }
    fs::write(&cfg.out, serde_json::to_vec_pretty(&out)?)
        .with_context(|| format!("writing {}", cfg.out.display()))?;

    println!("Wrote {}", cfg.out.display());
    Ok(())
}
