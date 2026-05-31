#![allow(dead_code)]
use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use reqwest::{
    Url,
    header::{HeaderName, HeaderValue},
};

pub const INTERNAL_TOKEN_HEADER: &str = "x-buso-internal-token";

const DEFAULT_INTERNAL_TOKEN_FILE: &str = "/shared/.internal-token";
const DESCRIPTOR_URL_ENV: &str = "ALPHA_DESCRIPTOR_URL";
const INTERNAL_TOKEN_ENV: &str = "ALPHA_INTERNAL_TOKEN";
const INTERNAL_TOKEN_FILE_ENV: &str = "ALPHA_INTERNAL_TOKEN_FILE";

fn trimmed_nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

pub fn descriptor_url_from_env() -> Option<String> {
    trimmed_nonempty(env::var(DESCRIPTOR_URL_ENV).ok())
}

fn descriptor_origin() -> Result<Option<String>> {
    let Some(url) = descriptor_url_from_env() else {
        return Ok(None);
    };
    let parsed = Url::parse(&url)
        .with_context(|| format!("{DESCRIPTOR_URL_ENV} must be a valid absolute URL"))?;
    Ok(Some(parsed.origin().ascii_serialization()))
}

pub fn internal_token_file_path() -> PathBuf {
    trimmed_nonempty(env::var(INTERNAL_TOKEN_FILE_ENV).ok())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_INTERNAL_TOKEN_FILE))
}

pub fn load_internal_token() -> Result<Option<String>> {
    if let Some(token) = trimmed_nonempty(env::var(INTERNAL_TOKEN_ENV).ok()) {
        return Ok(Some(token));
    }

    let token_path = internal_token_file_path();
    if !token_path.exists() {
        return Ok(None);
    }

    let token = fs::read_to_string(&token_path).with_context(|| {
        format!(
            "failed to read internal token file {}",
            token_path.display()
        )
    })?;
    Ok(trimmed_nonempty(Some(token)))
}

fn should_attach_internal_token(url: &str) -> Result<bool> {
    let Some(expected_origin) = descriptor_origin()? else {
        return Ok(false);
    };
    let parsed = Url::parse(url)
        .with_context(|| format!("invalid URL for internal token attachment: {url}"))?;
    Ok(parsed.origin().ascii_serialization() == expected_origin)
}

fn internal_header_value_for_url(url: &str) -> Result<Option<HeaderValue>> {
    if !should_attach_internal_token(url)? {
        return Ok(None);
    }

    let Some(token) = load_internal_token()? else {
        bail!(
            "internal token is required for descriptor-scoped request to {url}, but no token is configured"
        );
    };

    let header_value = HeaderValue::from_str(&token)
        .context("internal token contains invalid HTTP header characters")?;
    Ok(Some(header_value))
}

pub fn matches_internal_token(header_value: Option<&str>) -> Result<bool> {
    let Some(expected_token) = load_internal_token()? else {
        return Ok(false);
    };

    Ok(header_value
        .map(str::trim)
        .is_some_and(|candidate| candidate == expected_token))
}

pub fn maybe_add_internal_token_async(
    builder: reqwest::RequestBuilder,
    url: &str,
) -> Result<reqwest::RequestBuilder> {
    let Some(header_value) = internal_header_value_for_url(url)? else {
        return Ok(builder);
    };

    Ok(builder.header(
        HeaderName::from_static(INTERNAL_TOKEN_HEADER),
        header_value,
    ))
}

pub fn maybe_add_internal_token_blocking(
    builder: reqwest::blocking::RequestBuilder,
    url: &str,
) -> Result<reqwest::blocking::RequestBuilder> {
    let Some(header_value) = internal_header_value_for_url(url)? else {
        return Ok(builder);
    };

    Ok(builder.header(
        HeaderName::from_static(INTERNAL_TOKEN_HEADER),
        header_value,
    ))
}