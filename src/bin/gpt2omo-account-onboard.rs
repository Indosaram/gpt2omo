use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use gpt2omo::cli::default_mount_root;
use gpt2omo::orca::OrcaConfig;
use gpt2omo::{
    activate_pending_accounts_config, default_bridge_base_dir, default_scope_dir,
    load_pending_accounts_config, pending_account_health, pending_accounts_path,
    prepare_pending_accounts_config, BrowserPool, LegacyAccountConfig, WorkspaceMux,
};
use serde::Serialize;
use std::path::PathBuf;
use tokio::time::{sleep, Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "gpt2omo-account-onboard",
    version,
    about = "Prepare, verify, and safely activate isolated gpt2omo ChatGPT accounts"
)]
struct Cli {
    /// Filesystem mount root used by the bridge daemon. Browser profiles must live outside it.
    #[arg(long, default_value_os_t = default_mount_root())]
    mount_root: PathBuf,

    /// Bridge control directory containing pending and active account configurations.
    #[arg(long, env = "OMO_BRIDGE_HOME")]
    bridge_dir: Option<PathBuf>,

    /// Scope directory used by the bridge daemon to protect retained sessions.
    #[arg(long, env = "OMO_SCOPE_DIR")]
    scope_dir: Option<PathBuf>,

    /// Bridge port used only to derive the default scope directory.
    #[arg(long, default_value_t = 18800)]
    port: u16,

    /// Browser workspace selector for legacy fallback and non-CDP operation.
    #[arg(long, default_value = "active")]
    worktree: String,

    /// Browser executable used when no account-specific browser binding exists.
    #[arg(long, default_value = "orca")]
    orca_bin: String,

    /// Emit compact JSON.
    #[arg(long)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create a pending isolated configuration and open one ChatGPT login page per account.
    Prepare {
        /// Stable account identifier. Repeat once per ChatGPT account.
        #[arg(long, required = true, value_name = "ID")]
        account: Vec<String>,

        /// First loopback CDP port; subsequent accounts use consecutive ports.
        #[arg(long, default_value_t = 9223)]
        cdp_start_port: u16,

        /// Replace an unactivated pending onboarding configuration.
        #[arg(long)]
        replace_pending: bool,
    },
    /// Show pending-account browser reachability and login readiness.
    Status,
    /// Wait for every pending account to become login-ready without activating routing.
    Wait {
        /// Maximum number of seconds to wait for operator login.
        #[arg(long, default_value_t = 600)]
        timeout_seconds: u64,

        /// Number of seconds between readiness checks.
        #[arg(long, default_value_t = 2)]
        poll_seconds: u64,
    },
    /// Promote a fully ready pending configuration to accounts.json.
    Activate {
        /// Required acknowledgement that activation changes routing for future work.
        #[arg(long)]
        confirm: bool,
    },
}

#[derive(Serialize)]
struct PreparedAccount {
    account_id: String,
    browser_instance: String,
    browser_page_id: String,
    login_url: &'static str,
}

#[derive(Serialize)]
struct PrepareResult {
    pending_config: String,
    accounts: Vec<PreparedAccount>,
    next_step: &'static str,
}

#[derive(Serialize)]
struct StatusResult {
    pending_config: String,
    accounts: Vec<gpt2omo::BrowserHealth>,
    ready_for_activation: bool,
}

#[derive(Serialize)]
struct ActivateResult {
    active_config: String,
    account_ids: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    gpt2omo::load_dotenv_if_present();
    let cli = Cli::parse();
    let bridge_dir = cli.bridge_dir.unwrap_or_else(default_bridge_base_dir);
    let scope_dir = cli.scope_dir.unwrap_or_else(|| default_scope_dir(cli.port));
    let legacy = LegacyAccountConfig::default();
    let legacy_driver = OrcaConfig::new(cli.worktree.clone(), None, cli.orca_bin);
    let mux = WorkspaceMux::new(&cli.mount_root, &scope_dir)?;

    match cli.command {
        Command::Prepare {
            account,
            cdp_start_port,
            replace_pending,
        } => {
            let config = prepare_pending_accounts_config(
                &bridge_dir,
                &cli.mount_root,
                &account,
                cdp_start_port,
                &cli.worktree,
                replace_pending,
            )?;
            let browsers = BrowserPool::with_config_path(
                &bridge_dir,
                &cli.mount_root,
                legacy,
                legacy_driver,
                pending_accounts_path(&bridge_dir),
            );
            browsers.provision_profiles()?;
            let mut accounts = Vec::with_capacity(config.accounts.len());
            for account in &config.accounts {
                let page = browsers.open_chatgpt_login_page(&account.id).await?;
                accounts.push(PreparedAccount {
                    account_id: account.id.clone(),
                    browser_instance: page.target.instance,
                    browser_page_id: page.page_id,
                    login_url: "https://chatgpt.com",
                });
            }
            print_result(
                cli.json,
                &PrepareResult {
                    pending_config: pending_accounts_path(&bridge_dir).display().to_string(),
                    accounts,
                    next_step: "Log into the intended ChatGPT account in every opened page, then run status.",
                },
            )?;
        }
        Command::Status => print_result(
            cli.json,
            &pending_status(&bridge_dir, &cli.mount_root, legacy, legacy_driver).await?,
        )?,
        Command::Wait {
            timeout_seconds,
            poll_seconds,
        } => {
            let (timeout, poll_interval) = wait_durations(timeout_seconds, poll_seconds)?;
            let deadline = Instant::now() + timeout;
            loop {
                let status = pending_status(
                    &bridge_dir,
                    &cli.mount_root,
                    legacy.clone(),
                    legacy_driver.clone(),
                )
                .await?;
                if status.ready_for_activation {
                    print_result(cli.json, &status)?;
                    break;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(anyhow!(
                        "timed out waiting for every pending account to become login-ready"
                    ));
                }
                sleep(remaining.min(poll_interval)).await;
            }
        }
        Command::Activate { confirm } => {
            if !confirm {
                return Err(anyhow!(
                    "activation changes routing for future workers; pass --confirm after status reports every account ready"
                ));
            }
            let config =
                load_pending_accounts_config(&bridge_dir, &cli.mount_root, legacy.clone())?;
            let browsers = BrowserPool::with_config_path(
                &bridge_dir,
                &cli.mount_root,
                legacy.clone(),
                legacy_driver,
                pending_accounts_path(&bridge_dir),
            );
            let health = pending_account_health(&config, &browsers).await;
            let activated = activate_pending_accounts_config(
                &bridge_dir,
                &cli.mount_root,
                &mux,
                legacy,
                &health,
            )?;
            print_result(
                cli.json,
                &ActivateResult {
                    active_config: bridge_dir.join("accounts.json").display().to_string(),
                    account_ids: activated
                        .accounts
                        .into_iter()
                        .map(|account| account.id)
                        .collect(),
                },
            )?;
        }
    }
    Ok(())
}

async fn pending_status(
    bridge_dir: &PathBuf,
    mount_root: &PathBuf,
    legacy: LegacyAccountConfig,
    legacy_driver: OrcaConfig,
) -> Result<StatusResult> {
    let config = load_pending_accounts_config(bridge_dir, mount_root, legacy.clone())?;
    let browsers = BrowserPool::with_config_path(
        bridge_dir,
        mount_root,
        legacy,
        legacy_driver,
        pending_accounts_path(bridge_dir),
    );
    let accounts = pending_account_health(&config, &browsers).await;
    let ready_for_activation = accounts
        .iter()
        .all(|account| account.login_state == gpt2omo::BrowserLoginState::Ready);
    Ok(StatusResult {
        pending_config: pending_accounts_path(bridge_dir).display().to_string(),
        accounts,
        ready_for_activation,
    })
}

fn wait_durations(timeout_seconds: u64, poll_seconds: u64) -> Result<(Duration, Duration)> {
    if timeout_seconds == 0 || poll_seconds == 0 {
        return Err(anyhow!(
            "--timeout-seconds and --poll-seconds must both be greater than zero"
        ));
    }
    Ok((
        Duration::from_secs(timeout_seconds),
        Duration::from_secs(poll_seconds),
    ))
}

fn print_result<T: Serialize>(json: bool, result: &T) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(result)?);
    } else {
        println!("{}", serde_json::to_string_pretty(result)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activate_requires_explicit_confirmation() {
        let cli = Cli::try_parse_from(["gpt2omo-account-onboard", "activate"]).unwrap();
        assert!(matches!(cli.command, Command::Activate { confirm: false }));
    }

    #[test]
    fn prepare_accepts_multiple_account_identifiers() {
        let cli = Cli::try_parse_from([
            "gpt2omo-account-onboard",
            "prepare",
            "--account",
            "primary",
            "--account",
            "secondary",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::Prepare { account, .. } if account == ["primary", "secondary"]
        ));
    }

    #[test]
    fn wait_rejects_zero_timeout_or_poll_interval() {
        assert!(wait_durations(0, 1).is_err());
        assert!(wait_durations(1, 0).is_err());
        assert_eq!(
            wait_durations(5, 2).unwrap(),
            (Duration::from_secs(5), Duration::from_secs(2))
        );
    }
}
