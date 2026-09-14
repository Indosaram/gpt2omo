//! Regression coverage for DB-2 / WF-2 / OT-2: the ChatGPT Web prompt carries
//! the per-scope `capability_secret`, so it must never be observable in the
//! process table or in a diagnostic string.

use gpt2omo::orca::{chatgpt_prompt_invocation, BrowserDriverKind};

/// Stands in for a refreshed per-scope capability secret.
const CAPABILITY_SECRET: &str = "cap-sentinel-7f3a9d2b4e6c8a1509d4";

fn delegation_prompt() -> String {
    format!(
        "[GPT2OMO DELEGATION]\n\
SCOPE_ID: 44444444-4444-4444-8444-444444444444\n\
CAPABILITY_SECRET: {CAPABILITY_SECRET}\n\
WORKSPACE: /tmp/project\n\
GENERATION: 2\n\n\
TASK:\nfix the failing tests"
    )
}

#[test]
fn prompt_invocation_never_puts_the_capability_secret_in_argv() {
    let prompt = delegation_prompt();
    let invocation = chatgpt_prompt_invocation(BrowserDriverKind::AgentBrowser, "session-1", &prompt)
        .expect("agent-browser must be able to deliver a capability-bearing prompt");

    for (index, arg) in invocation.args.iter().enumerate() {
        assert!(
            !arg.contains(CAPABILITY_SECRET),
            "capability secret leaked into process argument {index}: {arg}"
        );
    }

    let stdin = invocation
        .stdin
        .expect("capability-bearing payload must travel over stdin, not argv");
    assert!(
        stdin.contains(CAPABILITY_SECRET),
        "the child must still receive the exact payload over the private channel"
    );
}

#[test]
fn drivers_without_a_secret_safe_transport_are_refused_without_echoing_the_secret() {
    let prompt = delegation_prompt();
    for kind in [
        BrowserDriverKind::Orca,
        BrowserDriverKind::Cmux,
        BrowserDriverKind::Maho,
        BrowserDriverKind::Aside,
        BrowserDriverKind::Chrome,
    ] {
        let Err(error) = chatgpt_prompt_invocation(kind, "page-1", &prompt) else {
            panic!("driver {kind} has no stdin transport, so it must refuse the prompt instead of passing it in argv");
        };
        let message = error.to_string();
        assert!(
            !message.contains(CAPABILITY_SECRET),
            "driver {kind} refusal diagnostic interpolated the capability secret: {message}"
        );
    }
}
