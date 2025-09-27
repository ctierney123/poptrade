use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use serde_json::json;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use chrono::Utc;
use tokio::sync::{Mutex, mpsc};
use std::sync::Arc;
use crossterm::{
    cursor::{MoveTo, Hide},
    terminal::{Clear, ClearType},
    execute,
};
use std::io;
use colored::*;
use serde::{Deserialize, Serialize};
use reqwest;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;


// --- Parquet Exporting Module ---
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::Write;
use arrow2::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::Timelike;

/// A simple struct for trade/tick data
#[derive(Debug, Clone)]
struct TickRecord {
    timestamp: i64,     // unix nanos
    token: String,      // which coin
    price: f64,
    size: f64,
    side: String,       // "buy" or "sell"
    age_seconds: Option<u64>, // age of coin in seconds, None if unknown
    // Advanced features
    price_change: f64,  // price change from previous tick
    price_change_pct: f64, // percentage change
    volume_imbalance: f64, // signed volume (positive for buys, negative for sells)
    vwap: f64,          // volume weighted average price
    rolling_volatility: f64, // rolling standard deviation
    momentum: f64,      // momentum over last N ticks
    skewness: f64,      // skewness of trade sizes
    kurtosis: f64,      // kurtosis of trade sizes
    trade_intensity: f64, // trades per second
    reliability_score: f64, // data quality score 0-1
}

/// Rolling metrics tracker for real-time feature engineering
#[derive(Debug, Clone)]
struct RollingMetrics {
    // Price tracking
    last_price: f64,
    price_sum: f64,
    price_squared_sum: f64,
    price_count: u32,
    
    // Volume tracking
    volume_sum: f64,
    volume_squared_sum: f64,
    signed_volume_sum: f64, // positive for buys, negative for sells
    vwap_numerator: f64,    // sum(price * volume)
    vwap_denominator: f64,  // sum(volume)
    
    // Trade size tracking for skewness/kurtosis
    trade_sizes: Vec<f64>,
    trade_size_sum: f64,
    trade_size_squared_sum: f64,
    trade_size_cubed_sum: f64,
    trade_size_quartic_sum: f64,
    
    // Time tracking
    last_timestamp: i64,
    trade_count: u32,
    time_window_seconds: u64,
    
    // Momentum tracking
    price_changes: Vec<f64>,
    momentum_sum: f64,
    
    // Reliability tracking
    data_points: u32,
    missing_data_count: u32,
}

impl RollingMetrics {
    fn new() -> Self {
        Self {
            last_price: 0.0,
            price_sum: 0.0,
            price_squared_sum: 0.0,
            price_count: 0,
            volume_sum: 0.0,
            volume_squared_sum: 0.0,
            signed_volume_sum: 0.0,
            vwap_numerator: 0.0,
            vwap_denominator: 0.0,
            trade_sizes: Vec::new(),
            trade_size_sum: 0.0,
            trade_size_squared_sum: 0.0,
            trade_size_cubed_sum: 0.0,
            trade_size_quartic_sum: 0.0,
            last_timestamp: 0,
            trade_count: 0,
            time_window_seconds: 300, // 5 minutes
            price_changes: Vec::new(),
            momentum_sum: 0.0,
            data_points: 0,
            missing_data_count: 0,
        }
    }
    
    fn update(&mut self, price: f64, volume: f64, side: &str, timestamp: i64) {
        let is_buy = side == "buy";
        let signed_volume = if is_buy { volume } else { -volume };
        
        // Price tracking
        if self.last_price > 0.0 {
            let price_change = price - self.last_price;
            let price_change_pct = if self.last_price > 0.0 { price_change / self.last_price } else { 0.0 };
            
            self.price_sum += price;
            self.price_squared_sum += price * price;
            self.price_count += 1;
            
            // Rolling variance calculation (Welford's algorithm)
            if self.price_count > 1 {
                let mean = self.price_sum / self.price_count as f64;
                let _variance = (self.price_squared_sum / self.price_count as f64) - (mean * mean);
                // Store rolling volatility as standard deviation
            }
            
            // Momentum tracking
            self.price_changes.push(price_change_pct);
            if self.price_changes.len() > 20 { // Keep last 20 price changes
                self.price_changes.remove(0);
            }
            self.momentum_sum = self.price_changes.iter().sum();
        }
        
        self.last_price = price;
        
        // Volume tracking
        self.volume_sum += volume;
        self.volume_squared_sum += volume * volume;
        self.signed_volume_sum += signed_volume;
        
        // VWAP calculation
        self.vwap_numerator += price * volume;
        self.vwap_denominator += volume;
        
        // Trade size tracking for skewness/kurtosis
        self.trade_sizes.push(volume);
        if self.trade_sizes.len() > 100 { // Keep last 100 trade sizes
            self.trade_sizes.remove(0);
        }
        
        self.trade_size_sum += volume;
        self.trade_size_squared_sum += volume * volume;
        self.trade_size_cubed_sum += volume * volume * volume;
        self.trade_size_quartic_sum += volume * volume * volume * volume;
        
        // Time tracking
        self.last_timestamp = timestamp;
        self.trade_count += 1;
        self.data_points += 1;
    }
    
    fn get_rolling_volatility(&self) -> f64 {
        if self.price_count < 2 {
            return 0.0;
        }
        let mean = self.price_sum / self.price_count as f64;
        let variance = (self.price_squared_sum / self.price_count as f64) - (mean * mean);
        variance.sqrt()
    }
    
    fn get_vwap(&self) -> f64 {
        if self.vwap_denominator > 0.0 {
            self.vwap_numerator / self.vwap_denominator
        } else {
            0.0
        }
    }
    
    fn get_momentum(&self) -> f64 {
        if self.price_changes.is_empty() {
            return 0.0;
        }
        self.momentum_sum / self.price_changes.len() as f64
    }
    
    fn get_skewness(&self) -> f64 {
        if self.trade_sizes.len() < 3 {
            return 0.0;
        }
        let n = self.trade_sizes.len() as f64;
        let mean = self.trade_size_sum / n;
        let variance = (self.trade_size_squared_sum / n) - (mean * mean);
        if variance <= 0.0 {
            return 0.0;
        }
        let std_dev = variance.sqrt();
        let skewness = (self.trade_size_cubed_sum / n - 3.0 * mean * variance - mean * mean * mean) / (std_dev * std_dev * std_dev);
        skewness
    }
    
    fn get_kurtosis(&self) -> f64 {
        if self.trade_sizes.len() < 4 {
            return 0.0;
        }
        let n = self.trade_sizes.len() as f64;
        let mean = self.trade_size_sum / n;
        let variance = (self.trade_size_squared_sum / n) - (mean * mean);
        if variance <= 0.0 {
            return 0.0;
        }
        let std_dev = variance.sqrt();
        let kurtosis = (self.trade_size_quartic_sum / n - 4.0 * mean * (self.trade_size_cubed_sum / n) + 6.0 * mean * mean * variance + 3.0 * mean * mean * mean * mean) / (std_dev * std_dev * std_dev * std_dev) - 3.0;
        kurtosis
    }
    
    fn get_trade_intensity(&self) -> f64 {
        if self.last_timestamp > 0 {
            let time_elapsed = (self.last_timestamp - (self.last_timestamp - 1000000000)) as f64 / 1_000_000_000.0; // 1 second window
            if time_elapsed > 0.0 {
                self.trade_count as f64 / time_elapsed
            } else {
                0.0
            }
        } else {
            0.0
        }
    }
    
    fn get_reliability_score(&self) -> f64 {
        if self.data_points == 0 {
            return 0.0;
        }
        let completeness = 1.0 - (self.missing_data_count as f64 / self.data_points as f64);
        let recency = if self.last_timestamp > 0 {
            let time_since_last = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64 - self.last_timestamp) as f64 / 1_000_000_000.0;
            if time_since_last < 60.0 { 1.0 } else { 0.5 }
        } else {
            0.0
        };
        (completeness + recency) / 2.0
    }
}

/// Writer context that manages hourly files
struct DataWriter {
    base_path: PathBuf,
    buffer: Vec<TickRecord>,
    schema: Arc<Schema>,
}

impl DataWriter {
    fn new(base_path: &str) -> Self {
        let fields = vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("token", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("size", DataType::Float64, false),
            Field::new("side", DataType::Utf8, false),
        ];
        let schema = Arc::new(Schema::from(fields));
        Self {
            base_path: PathBuf::from(base_path),
            buffer: Vec::new(),
            schema,
        }
    }

    /// Append a record into memory
    fn push(&mut self, rec: TickRecord) {
        self.buffer.push(rec);
        if self.buffer.len() >= 10 {  // Flush every 10 records instead of 10,000
            self.flush().unwrap();
        }
    }

    /// Compute current hourly file path
    fn current_file_path(&self) -> PathBuf {
        let now = chrono::Utc::now();
        let dir = self.base_path.join(now.format("%Y-%m-%d").to_string());
        fs::create_dir_all(&dir).unwrap();
        dir.join(format!("hour_{:02}.parquet", now.hour()))
    }

    /// Flush buffer to parquet file
    fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // For now, just write to CSV format as a fallback
        // This ensures the data export works while we can improve Parquet later
        let file_path = self.current_file_path().with_extension("csv");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(file_path)?;

        for record in &self.buffer {
            let age_str = match record.age_seconds {
                Some(age) => age.to_string(),
                None => "null".to_string(),
            };
            writeln!(file, "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}", 
                record.timestamp, 
                record.token, 
                record.price, 
                record.size, 
                record.side,
                age_str,
                record.price_change,
                record.price_change_pct,
                record.volume_imbalance,
                record.vwap,
                record.rolling_volatility,
                record.momentum,
                record.skewness,
                record.kurtosis,
                record.trade_intensity,
                record.reliability_score
            )?;
        }

        self.buffer.clear();
        Ok(())
    }
}

/// Handle message and extract trade data for Parquet export
fn handle_message_for_export(msg: &str, writer: &Arc<Mutex<DataWriter>>, tokens: &Arc<Mutex<HashMap<String, TokenInfo>>>) {
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(msg) {
        if let Some(tx_type) = json["txType"].as_str() {
            // Check if this is a trade event (any txType that's not "create")
            if tx_type != "create" {
                // Extract trade data - use the same fields as the main handler
                if let (Some(sol_amount), Some(mint)) = (
                    json["solAmount"].as_f64(),
                    json["mint"].as_str()
                ) {
                    // Calculate price from bonding curve data (same as main handler)
                    let v_sol = json["vSolInBondingCurve"].as_f64().unwrap_or(0.0);
                    let v_tokens = json["vTokensInBondingCurve"].as_f64().unwrap_or(0.0);
                    let price = if v_tokens > 0.0 { v_sol / v_tokens } else { 0.0 };
                    
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH).unwrap()
                        .as_nanos() as i64;
                    
                    // Calculate coin age if we have subscription start time
                    let age_seconds = if let Ok(tokens_guard) = tokens.try_lock() {
                        if let Some(token) = tokens_guard.get(mint) {
                            if let Some(subscription_start) = token.subscription_start_time {
                                let age = Instant::now().duration_since(subscription_start).as_secs();
                                Some(age)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    
                    // Update rolling metrics and calculate advanced features
                    let (price_change, price_change_pct, volume_imbalance, vwap, rolling_volatility, momentum, skewness, kurtosis, trade_intensity, reliability_score) = 
                        if let Ok(mut tokens_guard) = tokens.try_lock() {
                            if let Some(token) = tokens_guard.get_mut(mint) {
                                // Update rolling metrics
                                token.rolling_metrics.update(price, sol_amount, tx_type, timestamp);
                                
                                // Calculate features
                                let price_change = if token.rolling_metrics.last_price > 0.0 {
                                    price - token.rolling_metrics.last_price
                                } else { 0.0 };
                                
                                let price_change_pct = if token.rolling_metrics.last_price > 0.0 {
                                    price_change / token.rolling_metrics.last_price
                                } else { 0.0 };
                                
                                let volume_imbalance = if tx_type == "buy" { sol_amount } else { -sol_amount };
                                let vwap = token.rolling_metrics.get_vwap();
                                let rolling_volatility = token.rolling_metrics.get_rolling_volatility();
                                let momentum = token.rolling_metrics.get_momentum();
                                let skewness = token.rolling_metrics.get_skewness();
                                let kurtosis = token.rolling_metrics.get_kurtosis();
                                let trade_intensity = token.rolling_metrics.get_trade_intensity();
                                let reliability_score = token.rolling_metrics.get_reliability_score();
                                
                                (price_change, price_change_pct, volume_imbalance, vwap, rolling_volatility, momentum, skewness, kurtosis, trade_intensity, reliability_score)
                            } else {
                                (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                            }
                        } else {
                            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                        };
                    
                    let record = TickRecord {
                        timestamp,
                        token: mint.to_string(),
                        price,
                        size: sol_amount,
                        side: tx_type.to_string(),
                        age_seconds,
                        // Advanced features
                        price_change,
                        price_change_pct,
                        volume_imbalance,
                        vwap,
                        rolling_volatility,
                        momentum,
                        skewness,
                        kurtosis,
                        trade_intensity,
                        reliability_score,
                    };

                    // thread-safe push
                    if let Ok(mut w) = writer.try_lock() {
                        w.push(record);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct TokenInfo {
    name: String,
    symbol: String,
    mint: String,
    price_sol: f64,
    price_usd: f64,
    market_cap_sol: f64,
    market_cap_usd: f64,
    volume_24h_sol: f64,
    volume_24h_usd: f64,
    trades_24h: u32,
    last_trade_time: Instant,
    price_change_24h: f64,
    // Activity tracking for delisting
    total_trades: u32,
    creation_time: Instant,
    last_activity_time: Instant,
    is_subscribed: bool,
    // Price tracking
    initial_price_usd: f64,
    price_history: Vec<(Instant, f64)>, // (timestamp, price_usd)
    // Age tracking
    subscription_start_time: Option<Instant>, // when we first subscribed to this token
    // Rolling metrics for feature engineering
    rolling_metrics: RollingMetrics,
}

#[derive(Debug, Deserialize)]
struct SolPriceResponse {
    solana: SolPriceData,
}

#[derive(Debug, Deserialize)]
struct SolPriceData {
    usd: f64,
}

// API Response structures
#[derive(Debug, Clone, Serialize)]
struct ApiTokenInfo {
    name: String,
    symbol: String,
    mint: String,
    price_sol: f64,
    price_usd: f64,
    market_cap_sol: f64,
    market_cap_usd: f64,
    volume_24h_sol: f64,
    volume_24h_usd: f64,
    trades_24h: u32,
    price_change_24h: f64,
    total_trades: u32,
    age_seconds: u64,
    is_subscribed: bool,
    initial_price_usd: f64,
    // Rolling metrics
    vwap: f64,
    rolling_volatility: f64,
    momentum: f64,
    skewness: f64,
    kurtosis: f64,
    trade_intensity: f64,
    reliability_score: f64,
}

#[derive(Debug, Serialize)]
struct ApiCoinsList {
    coins: Vec<String>,
    total_count: usize,
}

#[derive(Debug, Serialize)]
struct ApiCoinPrices {
    prices: Vec<ApiCoinPrice>,
    sol_price_usd: f64,
}

#[derive(Debug, Serialize)]
struct ApiCoinPrice {
    mint: String,
    symbol: String,
    price_sol: f64,
    price_usd: f64,
    price_change_24h: f64,
}

#[derive(Debug, Serialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<String>,
    timestamp: i64,
}

// Application state for API
#[derive(Clone)]
struct AppState {
    tokens: Arc<Mutex<HashMap<String, TokenInfo>>>,
    terminal: Arc<Mutex<TerminalInterface>>,
}

#[derive(Debug, Clone)]
struct TradeUpdate {
    mint: String,
    sol_amount: f64,
    market_cap: f64,
    tx_type: String,
    timestamp: Instant,
}

#[derive(Debug, Clone)]
struct TransactionLog {
    symbol: String,
    tx_type: String,
    sol_amount: f64,
    market_cap: f64,
    timestamp: Instant,
}

struct TerminalInterface {
    terminal_size: (u16, u16),
    table_start_row: u16,
    transaction_log_start_row: u16,
    transaction_log: Vec<TransactionLog>,
    max_transaction_log_size: usize,
    sol_price_usd: f64,
}

async fn fetch_sol_price() -> Result<f64, Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    
    let response = client
        .get("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd")
        .header("User-Agent", "PumpTrade/1.0")
        .send()
        .await?;
    
    if !response.status().is_success() {
        eprintln!("⚠️ SOL price API returned status: {}", response.status());
        return Ok(100.0); // Fallback price
    }
    
    let price_data: SolPriceResponse = response.json().await?;
    Ok(price_data.solana.usd)
}

impl TerminalInterface {
    fn new() -> Self {
        let terminal_size = crossterm::terminal::size().unwrap_or((80, 24));
        Self {
            terminal_size,
            table_start_row: 3,
            transaction_log_start_row: 20,
            transaction_log: Vec::new(),
            max_transaction_log_size: 10,
            sol_price_usd: 100.0, // Default fallback price
        }
    }

    async fn update_sol_price(&mut self) {
        if let Ok(price) = fetch_sol_price().await {
            self.sol_price_usd = price;
        }
    }

    fn start_price_monitor(terminal_arc: Arc<Mutex<TerminalInterface>>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60)); // Reduced to 1 minute
            let mut consecutive_failures = 0;
            
            loop {
                interval.tick().await;
                {
                    let mut terminal = terminal_arc.lock().await;
                    let old_price = terminal.sol_price_usd;
                    terminal.update_sol_price().await;
                    
                    // Check if price update failed
                    if terminal.sol_price_usd == old_price && old_price == 100.0 {
                        consecutive_failures += 1;
                        if consecutive_failures >= 3 {
                            eprintln!("⚠️ SOL price API may be rate limited, using fallback price");
                            // Increase interval to reduce API calls
                            interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes
                        }
                    } else {
                        consecutive_failures = 0;
                    }
                }
            }
        });
    }

    fn add_transaction(&mut self, symbol: String, tx_type: String, sol_amount: f64, market_cap: f64) {
        let transaction = TransactionLog {
            symbol,
            tx_type,
            sol_amount,
            market_cap,
            timestamp: Instant::now(),
        };
        
        self.transaction_log.push(transaction);
        if self.transaction_log.len() > self.max_transaction_log_size {
            self.transaction_log.remove(0);
        }
    }

    fn render_header(&self) {
        let mut stdout = io::stdout();
        execute!(stdout, MoveTo(0, 0)).unwrap();
        println!("{}", "=".repeat(80).bright_blue());
        println!("{}", "PUMP FUN TOKEN TRACKER - Live Prices & Stats".bright_white().bold());
        println!("{}", "=".repeat(80).bright_blue());
    }

    fn render_table(&self, tokens: &HashMap<String, TokenInfo>, message_count: u32) {
        let mut stdout = io::stdout();
        
        // Move to table start position
        execute!(stdout, MoveTo(0, self.table_start_row)).unwrap();
        
        // Clear the table area
        for i in 0..15 {
            execute!(stdout, MoveTo(0, self.table_start_row + i)).unwrap();
            print!("{}", " ".repeat(140));
        }
        
        // Render table header
        execute!(stdout, MoveTo(0, self.table_start_row)).unwrap();
        println!("{:<10} {:<12} {:<12} {:<12} {:<8} {:<8} {:<8} {:<8} {:<8} {:<8} {:<8} {:<8} {:<8} {:<8}", 
                 "SYMBOL".bright_white().bold(), 
                 "PRICE".bright_white().bold(), 
                 "MKT CAP".bright_white().bold(), 
                 "VOL 24H".bright_white().bold(), 
                 "TRADES".bright_white().bold(), 
                 "15s".bright_green().bold(),
                 "30s".bright_green().bold(),
                 "45s".bright_green().bold(),
                 "1m".bright_green().bold(),
                 "2m".bright_green().bold(),
                 "3m".bright_green().bold(),
                 "4m".bright_green().bold(),
                 "5m".bright_green().bold(),
                 "ROLL".bright_green().bold());
        
        execute!(stdout, MoveTo(0, self.table_start_row + 1)).unwrap();
        println!("{}", "-".repeat(140).bright_blue());
        
        if tokens.is_empty() {
            execute!(stdout, MoveTo(0, self.table_start_row + 2)).unwrap();
            println!("{}", "Waiting for new tokens...".bright_yellow());
            return;
        }
        
        // Sort tokens by market cap (highest first)
        let mut sorted_tokens: Vec<_> = tokens.values().collect();
        sorted_tokens.sort_by(|a, b| b.market_cap_usd.partial_cmp(&a.market_cap_usd).unwrap());
        
        // Show top 15 tokens
        for (i, token) in sorted_tokens.iter().take(15).enumerate() {
            execute!(stdout, MoveTo(0, self.table_start_row + 2 + i as u16)).unwrap();
            
            // Calculate token age in seconds
            let token_age_seconds = token.creation_time.elapsed().as_secs();
            
            // Calculate price changes for different timeframes
            let change_15s = if token_age_seconds >= 15 { self.calculate_price_change(&token.price_history, 15) } else { 0.0 };
            let change_30s = if token_age_seconds >= 30 { self.calculate_price_change(&token.price_history, 30) } else { 0.0 };
            let change_45s = if token_age_seconds >= 45 { self.calculate_price_change(&token.price_history, 45) } else { 0.0 };
            let change_1m = if token_age_seconds >= 60 { self.calculate_price_change(&token.price_history, 60) } else { 0.0 };
            let change_2m = if token_age_seconds >= 120 { self.calculate_price_change(&token.price_history, 120) } else { 0.0 };
            let change_3m = if token_age_seconds >= 180 { self.calculate_price_change(&token.price_history, 180) } else { 0.0 };
            let change_4m = if token_age_seconds >= 240 { self.calculate_price_change(&token.price_history, 240) } else { 0.0 };
            let change_5m = if token_age_seconds >= 300 { self.calculate_price_change(&token.price_history, 300) } else { 0.0 };
            
            // Rolling change (overall change since creation)
            let rolling_change = if token.initial_price_usd > 0.0 {
                ((token.price_usd - token.initial_price_usd) / token.initial_price_usd) * 100.0
            } else {
                0.0
            };
            
            // Helper function to create placeholder with separate underscore and text parts
            let create_placeholder_parts = |text: &str, width: usize| -> (String, String) {
                let text_len = text.len();
                let padding = if text_len < width { width - text_len } else { 0 };
                
                let mut underscores = String::new();
                for _ in 0..padding {
                    underscores.push('_');
                }
                
                let display_text = if text_len > width {
                    text[..width].to_string()
                } else {
                    text.to_string()
                };
                
                (underscores, display_text)
            };
            
            // Helper function to format change with underscore placeholders
            let format_change = |change: f64, age_required: u64| -> (String, bool) {
                if token_age_seconds < age_required {
                    ("_____".to_string(), false) // 5 underscores, not applicable
                } else if change > 0.0 {
                    (format!("+{:.1}%", change), true)
                } else if change < 0.0 {
                    (format!("{:.1}%", change), true)
                } else {
                    (format!("{:.1}%", change), true)
                }
            };
            
            // Create placeholders for each field
            let (symbol_underscores, symbol_text) = create_placeholder_parts(&token.symbol[..std::cmp::min(10, token.symbol.len())], 10);
            let (price_underscores, price_text) = create_placeholder_parts(&format!("${:.6}", token.price_usd), 12);
            let (market_cap_underscores, market_cap_text) = create_placeholder_parts(&format!("${:.0}", token.market_cap_usd), 12);
            let (volume_underscores, volume_text) = create_placeholder_parts(&format!("${:.0}", token.volume_24h_usd), 12);
            let (trades_underscores, trades_text) = create_placeholder_parts(&token.trades_24h.to_string(), 8);
            
            // Format change columns with placeholders
            let (change_15s_text, change_15s_applicable) = format_change(change_15s, 15);
            let (change_15s_underscores, change_15s_display) = create_placeholder_parts(&change_15s_text, 8);
            
            let (change_30s_text, change_30s_applicable) = format_change(change_30s, 30);
            let (change_30s_underscores, change_30s_display) = create_placeholder_parts(&change_30s_text, 8);
            
            let (change_45s_text, change_45s_applicable) = format_change(change_45s, 45);
            let (change_45s_underscores, change_45s_display) = create_placeholder_parts(&change_45s_text, 8);
            
            let (change_1m_text, change_1m_applicable) = format_change(change_1m, 60);
            let (change_1m_underscores, change_1m_display) = create_placeholder_parts(&change_1m_text, 8);
            
            let (change_2m_text, change_2m_applicable) = format_change(change_2m, 120);
            let (change_2m_underscores, change_2m_display) = create_placeholder_parts(&change_2m_text, 8);
            
            let (change_3m_text, change_3m_applicable) = format_change(change_3m, 180);
            let (change_3m_underscores, change_3m_display) = create_placeholder_parts(&change_3m_text, 8);
            
            let (change_4m_text, change_4m_applicable) = format_change(change_4m, 240);
            let (change_4m_underscores, change_4m_display) = create_placeholder_parts(&change_4m_text, 8);
            
            let (change_5m_text, change_5m_applicable) = format_change(change_5m, 300);
            let (change_5m_underscores, change_5m_display) = create_placeholder_parts(&change_5m_text, 8);
            
            let (rolling_text, _) = format_change(rolling_change, 0);
            let (rolling_underscores, rolling_display) = create_placeholder_parts(&rolling_text, 8);
            
            // Print with mixed color coding: black underscores + colored text
            print!("{}{}", symbol_underscores.black(), symbol_text.bright_cyan());
            print!("{}{}", price_underscores.black(), price_text.bright_yellow());
            print!("{}{}", market_cap_underscores.black(), market_cap_text.bright_magenta());
            print!("{}{}", volume_underscores.black(), volume_text.bright_blue());
            print!("{}{}", trades_underscores.black(), trades_text.bright_white());
            
            // Helper function for gradient color based on percentage magnitude
            let get_gradient_color = |change: f64, text: &str| -> String {
                let abs_change = change.abs();
                if change > 0.0 {
                    // Green gradient: white at 0%, bright green at 20%+
                    if abs_change <= 1.0 {
                        text.white().to_string()
                    } else if abs_change <= 5.0 {
                        text.green().to_string()
                    } else if abs_change <= 10.0 {
                        text.bright_green().to_string()
                    } else if abs_change <= 20.0 {
                        text.bright_green().to_string()
                    } else {
                        text.bright_green().to_string()
                    }
                } else if change < 0.0 {
                    // Red gradient: white at 0%, bright red at 20%+
                    if abs_change <= 1.0 {
                        text.white().to_string()
                    } else if abs_change <= 5.0 {
                        text.red().to_string()
                    } else if abs_change <= 10.0 {
                        text.bright_red().to_string()
                    } else if abs_change <= 20.0 {
                        text.bright_red().to_string()
                    } else {
                        text.bright_red().to_string()
                    }
                } else {
                    text.white().to_string()
                }
            };
            
            // Color change columns: black underscores, gradient color for text
            if change_15s_applicable {
                let text_color = get_gradient_color(change_15s, &change_15s_display);
                print!("{}{}{}", change_15s_underscores.black(), text_color, "_".black());
            } else {
                print!("{}{}{}", change_15s_underscores.black(), change_15s_display.black(), "_".black());
            }
            
            if change_30s_applicable {
                let text_color = get_gradient_color(change_30s, &change_30s_display);
                print!("{}{}{}", change_30s_underscores.black(), text_color, "_".black());
            } else {
                print!("{}{}{}", change_30s_underscores.black(), change_30s_display.black(), "_".black());
            }
            
            if change_45s_applicable {
                let text_color = get_gradient_color(change_45s, &change_45s_display);
                print!("{}{}{}", change_45s_underscores.black(), text_color, "_".black());
            } else {
                print!("{}{}{}", change_45s_underscores.black(), change_45s_display.black(), "_".black());
            }
            
            if change_1m_applicable {
                let text_color = get_gradient_color(change_1m, &change_1m_display);
                print!("{}{}{}", change_1m_underscores.black(), text_color, "_".black());
            } else {
                print!("{}{}{}", change_1m_underscores.black(), change_1m_display.black(), "_".black());
            }
            
            if change_2m_applicable {
                let text_color = get_gradient_color(change_2m, &change_2m_display);
                print!("{}{}{}", change_2m_underscores.black(), text_color, "_".black());
            } else {
                print!("{}{}{}", change_2m_underscores.black(), change_2m_display.black(), "_".black());
            }
            
            if change_3m_applicable {
                let text_color = get_gradient_color(change_3m, &change_3m_display);
                print!("{}{}{}", change_3m_underscores.black(), text_color, "_".black());
            } else {
                print!("{}{}{}", change_3m_underscores.black(), change_3m_display.black(), "_".black());
            }
            
            if change_4m_applicable {
                let text_color = get_gradient_color(change_4m, &change_4m_display);
                print!("{}{}{}", change_4m_underscores.black(), text_color, "_".black());
            } else {
                print!("{}{}{}", change_4m_underscores.black(), change_4m_display.black(), "_".black());
            }
            
            if change_5m_applicable {
                let text_color = get_gradient_color(change_5m, &change_5m_display);
                print!("{}{}{}", change_5m_underscores.black(), text_color, "_".black());
            } else {
                print!("{}{}{}", change_5m_underscores.black(), change_5m_display.black(), "_".black());
            }
            
            let rolling_text_color = get_gradient_color(rolling_change, &rolling_display);
            print!("{}{}{}", rolling_underscores.black(), rolling_text_color, "_".black());
            println!(); // New line
        }
        
        // Render footer
        execute!(stdout, MoveTo(0, self.table_start_row + 17)).unwrap();
        println!("{}", "-".repeat(80).bright_blue());
        execute!(stdout, MoveTo(0, self.table_start_row + 18)).unwrap();
        println!("Last updated: {} | Total tokens: {} | Messages: {} | SOL: ${:.2}", 
                 Utc::now().format("%H:%M:%S").to_string().bright_green(),
                 tokens.len().to_string().bright_cyan(),
                 message_count.to_string().bright_cyan(),
                 self.sol_price_usd.to_string().bright_yellow());
    }

    fn calculate_price_change(&self, price_history: &[(Instant, f64)], timeframe_seconds: u64) -> f64 {
        let now = Instant::now();
        let cutoff_time = now - Duration::from_secs(timeframe_seconds);
        
        // Find the price at the cutoff time
        if let Some((_, old_price)) = price_history.iter()
            .filter(|(timestamp, _)| *timestamp >= cutoff_time)
            .min_by(|a, b| a.0.cmp(&b.0)) {
            
            if let Some((_, current_price)) = price_history.last() {
                if *old_price > 0.0 {
                    return ((current_price - old_price) / old_price) * 100.0;
                }
            }
        }
        0.0
    }

    fn render_transaction_log(&self) {
        let mut stdout = io::stdout();
        
        // Clear transaction log area
        for i in 0..self.max_transaction_log_size {
            execute!(stdout, MoveTo(0, self.transaction_log_start_row + i as u16)).unwrap();
            print!("{}", " ".repeat(80));
        }
        
        // Render transaction log header
        execute!(stdout, MoveTo(0, self.transaction_log_start_row)).unwrap();
        println!("{}", "LIVE TRANSACTIONS".bright_white().bold());
        execute!(stdout, MoveTo(0, self.transaction_log_start_row + 1)).unwrap();
        println!("{}", "-".repeat(80).bright_blue());
        
        // Render transactions
        for (i, transaction) in self.transaction_log.iter().enumerate() {
            execute!(stdout, MoveTo(0, self.transaction_log_start_row + 2 + i as u16)).unwrap();
            
            let tx_indicator = if transaction.tx_type == "buy" { 
                "[BUY]".bright_green()
            } else if transaction.tx_type == "sell" { 
                "[SELL]".bright_red()
            } else { 
                "[TRADE]".bright_yellow()
            };
            
            let usd_amount = transaction.sol_amount * self.sol_price_usd;
            let usd_market_cap = transaction.market_cap * self.sol_price_usd;
            
            println!("{} {} | ${:.2} | Market Cap: ${:.2} | {}s ago", 
                     tx_indicator,
                     transaction.symbol.bright_cyan(),
                     usd_amount,
                     usd_market_cap,
                     transaction.timestamp.elapsed().as_secs().to_string().bright_black());
        }
    }
}

// Token cleanup manager for delisting inactive tokens
struct TokenCleanupManager {
    tokens_arc: Arc<Mutex<HashMap<String, TokenInfo>>>,
    cleanup_sender: mpsc::UnboundedSender<String>,
}

impl TokenCleanupManager {
    fn new(tokens_arc: Arc<Mutex<HashMap<String, TokenInfo>>>, cleanup_sender: mpsc::UnboundedSender<String>) -> Self {
        Self {
            tokens_arc,
            cleanup_sender,
        }
    }
    
    fn start_cleanup(&self) {
        let tokens_arc = self.tokens_arc.clone();
        let cleanup_sender = self.cleanup_sender.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                
                let mut tokens_to_remove = Vec::new();
                let mut unsubscribe_requests = Vec::new();
                
                {
                    let mut tokens = tokens_arc.lock().await;
                    let now = Instant::now();
                    
                    for (mint, token) in tokens.iter_mut() {
                        let time_since_creation = now.duration_since(token.creation_time).as_secs();
                        let time_since_activity = now.duration_since(token.last_activity_time).as_secs();
                        
            // Rule 1: Delist if 0 or 1 trades in 20 seconds
            if time_since_creation >= 20 && token.total_trades <= 1 {
                tokens_to_remove.push(mint.clone());
                if token.is_subscribed {
                    unsubscribe_requests.push(mint.clone());
                }
                println!("🗑️ Delisting {} ({} trades in {}s)", token.symbol, token.total_trades, time_since_creation);
            }
            // Rule 2: Delist if 2+ trades but no activity for 30 seconds
            else if token.total_trades >= 2 && time_since_activity >= 30 {
                tokens_to_remove.push(mint.clone());
                if token.is_subscribed {
                    unsubscribe_requests.push(mint.clone());
                }
                println!("🗑️ Delisting {} (no activity for {}s)", token.symbol, time_since_activity);
            }
            // Rule 3: Delist if showing "???" (invalid/missing data)
            else if token.symbol == "???" || token.name == "???" {
                tokens_to_remove.push(mint.clone());
                if token.is_subscribed {
                    unsubscribe_requests.push(mint.clone());
                }
                println!("🗑️ Delisting {} (invalid data - ???)", token.symbol);
            }
                    }
                    
                    // Remove delisted tokens
                    for mint in &tokens_to_remove {
                        tokens.remove(mint);
                    }
                }
                
                // Send unsubscribe requests
                for mint in unsubscribe_requests {
                    if let Err(e) = cleanup_sender.send(mint) {
                        eprintln!("❌ Failed to send unsubscribe request: {}", e);
                    }
                }
            }
        });
    }
}

// This function is no longer needed - we'll use a single WebSocket connection

async fn run_websocket_connection(tokens: HashMap<String, TokenInfo>) -> Result<HashMap<String, TokenInfo>, Box<dyn std::error::Error>> {
    let url = "wss://pumpportal.fun/api/data";
    let (ws_stream, response) = connect_async(url).await?;
    println!("✅ Connected to PumpPortal websocket");
    println!("📡 Response status: {}", response.status());

    let (mut write, mut read) = ws_stream.split();
    
    // Create channel for trade updates
    let (trade_sender, mut trade_receiver) = mpsc::unbounded_channel::<TradeUpdate>();
    
    // Create a channel for sending subscription requests
    let (_subscription_sender, mut subscription_receiver) = mpsc::unbounded_channel::<String>();
    
    // Initialize terminal interface
    let mut terminal = TerminalInterface::new();
    
    // Initialize Parquet data writer
    let data_writer = Arc::new(Mutex::new(DataWriter::new("./data/pumpfun")));
    
    // Fetch SOL price
    terminal.update_sol_price().await;
    
    // Hide cursor and clear screen
    execute!(io::stdout(), Hide, Clear(ClearType::All)).unwrap();
    terminal.render_header();
    
    // Start background price monitoring
    let terminal_arc = Arc::new(Mutex::new(terminal));
    TerminalInterface::start_price_monitor(terminal_arc.clone());

    // Subscribe to new token creations
    let new_token_sub = json!({
        "method": "subscribeNewToken"
    });

    // Subscribe to all token trades using the correct format
    let trade_sub = json!({
        "method": "pumpFunTradeSubscribe",
        "params": {
            "coinAddress": "all",
            "referenceId": "REF#1"
        }
    });

    // Also try the original subscription method
    let trade_sub_alt = json!({
        "method": "subscribeTokenTrade",
        "keys": []
    });

    // Create tokens_arc early so it can be used in resubscription logic
    let tokens_arc = Arc::new(Mutex::new(tokens));
    
    println!("📤 Sending subscription messages...");
    write.send(Message::Text(new_token_sub.to_string())).await.expect("failed to send new token subscribe msg");
    write.send(Message::Text(trade_sub.to_string())).await.expect("failed to send trade subscribe msg");
    write.send(Message::Text(trade_sub_alt.to_string())).await.expect("failed to send trade subscribe alt msg");
    
    // Resubscribe to existing tokens if this is a reconnection
    {
        let tokens_guard = tokens_arc.lock().await;
        if !tokens_guard.is_empty() {
            println!("🔄 Resubscribing to {} existing tokens...", tokens_guard.len());
            for (mint, token) in tokens_guard.iter() {
                if token.is_subscribed {
                    let token_sub = json!({
                        "method": "subscribeTokenTrade",
                        "keys": [mint]
                    });
                    if let Err(e) = write.send(Message::Text(token_sub.to_string())).await {
                        eprintln!("❌ Failed to resubscribe to {}: {}", token.symbol, e);
                    } else {
                        println!("✅ Resubscribed to {}", token.symbol);
                        // Update subscription start time for reconnection
                        {
                            let mut tokens_guard = tokens_arc.lock().await;
                            if let Some(token) = tokens_guard.get_mut(mint) {
                                token.subscription_start_time = Some(Instant::now());
                            }
                        }
                    }
                }
            }
        }
    }
    
    println!("✅ Subscriptions sent successfully");

    println!("🔍 Monitoring Pump Fun activity via PumpPortal...");
    println!("Press Ctrl+C to exit\n");

    let mut last_display = Instant::now();
    let mut message_count = 0;
    let tokens_for_trades = tokens_arc.clone();
    
    // Create channel for cleanup manager to send unsubscribe messages
    let (cleanup_sender, mut cleanup_receiver) = mpsc::unbounded_channel::<String>();
    
    // Create cleanup manager for inactive tokens
    let cleanup_manager = TokenCleanupManager::new(tokens_arc.clone(), cleanup_sender);
    cleanup_manager.start_cleanup();
    
    // Start background token price updater (reduced frequency to avoid rate limiting)
    let tokens_arc_for_updater = tokens_arc.clone();
    let terminal_arc_for_updater = terminal_arc.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10)); // Reduced from 5 to 10 seconds
        loop {
            interval.tick().await;
            {
                let mut tokens = tokens_arc_for_updater.lock().await;
                let terminal = terminal_arc_for_updater.lock().await;
                let sol_price = terminal.sol_price_usd;
                
                // Update all token USD values with current SOL price
                for token in tokens.values_mut() {
                    token.price_usd = token.price_sol * sol_price;
                    token.market_cap_usd = token.market_cap_sol * sol_price;
                    token.volume_24h_usd = token.volume_24h_sol * sol_price;
                }
            }
        }
    });
    
    let terminal_for_trades = terminal_arc.clone();
    
    tokio::spawn(async move {
        while let Some(trade_update) = trade_receiver.recv().await {
            let mut tokens = tokens_for_trades.lock().await;
            if let Some(token) = tokens.get_mut(&trade_update.mint) {
                let _old_price = token.price_usd;
                token.market_cap_sol = trade_update.market_cap;
                token.volume_24h_sol += trade_update.sol_amount;
                token.trades_24h += 1;
                token.last_trade_time = trade_update.timestamp;
                
                // Get SOL price from terminal
                let sol_price = {
                    let terminal_guard = terminal_for_trades.lock().await;
                    terminal_guard.sol_price_usd
                };
                
                // Price and price change are already updated in the main message handler
                // Just update volume here
                token.volume_24h_sol += trade_update.sol_amount;
                token.volume_24h_usd += trade_update.sol_amount * sol_price;
                
                // Add to transaction log
                {
                    let mut terminal = terminal_for_trades.lock().await;
                    terminal.add_transaction(
                        token.symbol.clone(),
                        trade_update.tx_type.clone(),
                        trade_update.sol_amount,
                        trade_update.market_cap
                    );
                }
            }
        }
    });
    
    // We'll handle subscriptions in the main loop instead of a separate task
    
    // Handle subscriptions and messages in the main loop
    loop {
        tokio::select! {
            // Handle incoming WebSocket messages
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        message_count += 1;
                        let data: serde_json::Value = match serde_json::from_str(&text) {
                            Ok(parsed) => parsed,
                            Err(e) => {
                                eprintln!("❌ JSON parse error: {}", e);
                                continue;
                            }
                        };
                        
                        // Skip subscription confirmations
                        if data.get("message").is_some() {
                            continue;
                        }
                        
                        // Export trade data to Parquet
                        handle_message_for_export(&text, &data_writer, &tokens_arc);
                        
                        // Handle new token creation events
                        if data.get("txType") == Some(&serde_json::Value::String("create".to_string())) {
                            // Debug logging removed - using bonding curve calculation
                            
                            let name = data["name"].as_str().unwrap_or("Unknown");
                            let symbol = data["symbol"].as_str().unwrap_or("???");
                            let mint = data["mint"].as_str().unwrap_or("unknown");
                            let market_cap = data["marketCapSol"].as_f64().unwrap_or(0.0);
                            let sol_amount = data["solAmount"].as_f64().unwrap_or(0.0);
                            
                            // Calculate price from bonding curve data
                            let v_sol = data["vSolInBondingCurve"].as_f64().unwrap_or(0.0);
                            let v_tokens = data["vTokensInBondingCurve"].as_f64().unwrap_or(0.0);
                            
                            let price_sol = if v_tokens > 0.0 {
                                v_sol / v_tokens
                            } else {
                                0.0
                            };
                            
                            // Get SOL price from terminal
                            let sol_price = {
                                let terminal_guard = terminal_arc.lock().await;
                                terminal_guard.sol_price_usd
                            };
                            
                            let price_usd = price_sol * sol_price;
                            let market_cap_usd = market_cap * sol_price;
                            let volume_24h_usd = sol_amount * sol_price;
                            
                            let now = Instant::now();
                        let token_info = TokenInfo {
                            name: name.to_string(),
                            symbol: symbol.to_string(),
                            mint: mint.to_string(),
                                price_sol: price_sol,
                                price_usd: price_usd,
                                market_cap_sol: market_cap,
                                market_cap_usd: market_cap_usd,
                                volume_24h_sol: sol_amount,
                                volume_24h_usd: volume_24h_usd,
                                trades_24h: 1,
                                last_trade_time: now,
                            price_change_24h: 0.0,
                                // Activity tracking
                                total_trades: 1,
                                creation_time: now,
                                last_activity_time: now,
                                is_subscribed: false, // Will be set to true after subscription
                                // Price tracking
                                initial_price_usd: price_usd,
                                price_history: vec![(now, price_usd)],
                                // Age tracking
                                subscription_start_time: None, // Will be set when we subscribe
                                // Rolling metrics
                                rolling_metrics: RollingMetrics::new(),
                            };
                            
                            {
                                let mut tokens_guard = tokens_arc.lock().await;
                                tokens_guard.insert(mint.to_string(), token_info);
                            }
                            
                            // Subscribe to trades for this token on the same connection
                            let trade_sub = json!({
                                "method": "subscribeTokenTrade",
                                "keys": [mint]
                            });
                            
                            if let Err(e) = write.send(Message::Text(trade_sub.to_string())).await {
                                eprintln!("❌ Failed to send subscription: {}", e);
                            } else {
                                println!("🔍 Subscribed to trades for token: {}", &mint[..8]);
                                
                                // Mark token as subscribed and record subscription start time
                                {
                                    let mut tokens_guard = tokens_arc.lock().await;
                                    if let Some(token) = tokens_guard.get_mut(mint) {
                                        token.is_subscribed = true;
                                        token.subscription_start_time = Some(Instant::now());
                                    }
                                }
                            }
                            
                            // Add to transaction log as new token creation
                            {
                                let mut terminal = terminal_arc.lock().await;
                                terminal.add_transaction(
                                    symbol.to_string(),
                                    "CREATE".to_string(),
                                    sol_amount,
                                    market_cap
                                );
                            }
                        }
                        
                        // Handle trade events (buy/sell) - check for any transaction that's not create
                        else if data.get("txType").is_some() && 
                                data.get("txType") != Some(&serde_json::Value::String("create".to_string())) {
                            
                            // Debug logging removed - using bonding curve calculation
                            
                            let mint = data["mint"].as_str().unwrap_or("unknown");
                            let sol_amount = data["solAmount"].as_f64().unwrap_or(0.0);
                            let market_cap = data["marketCapSol"].as_f64().unwrap_or(0.0);
                            let tx_type = data["txType"].as_str().unwrap_or("unknown");
                            
                            // Debug logging removed
                            
                            // Update token activity
                            {
                                let mut tokens_guard = tokens_arc.lock().await;
                                if let Some(token) = tokens_guard.get_mut(mint) {
                                    token.total_trades += 1;
                                    token.last_activity_time = Instant::now();
                            token.trades_24h += 1;
                            token.last_trade_time = Instant::now();
                                    
                                    // Update price and market cap from trade data
                                    // Calculate price from bonding curve data
                                    let v_sol = data["vSolInBondingCurve"].as_f64().unwrap_or(0.0);
                                    let v_tokens = data["vTokensInBondingCurve"].as_f64().unwrap_or(0.0);
                                    
                                    // Store old price for change calculation
                                    let old_price_usd = token.price_usd;
                                    
                                    if v_tokens > 0.0 {
                                        token.price_sol = v_sol / v_tokens;
                                    }
                                    token.market_cap_sol = market_cap;
                                    
                                    // Update USD values
                                    let sol_price = {
                                        let terminal_guard = terminal_arc.lock().await;
                                        terminal_guard.sol_price_usd
                                    };
                                    token.price_usd = token.price_sol * sol_price;
                                    token.market_cap_usd = token.market_cap_sol * sol_price;
                                    token.volume_24h_usd = token.volume_24h_sol * sol_price;
                                    
                                    // Add to price history
                                    token.price_history.push((Instant::now(), token.price_usd));
                                    
                                    // Keep only last 5 minutes of history to prevent memory bloat
                                    let cutoff_time = Instant::now() - Duration::from_secs(300);
                                    token.price_history.retain(|(timestamp, _)| *timestamp >= cutoff_time);
                                    
                                    // Calculate price change (compare to previous price)
                                    if old_price_usd > 0.0 {
                                        token.price_change_24h = ((token.price_usd - old_price_usd) / old_price_usd) * 100.0;
                                    } else if token.initial_price_usd > 0.0 {
                                        // Fallback: compare to initial price
                                        token.price_change_24h = ((token.price_usd - token.initial_price_usd) / token.initial_price_usd) * 100.0;
                                    }
                                }
                            }
                            
                            // Send trade update
                            let trade_update = TradeUpdate {
                                mint: mint.to_string(),
                                sol_amount: sol_amount,
                                market_cap: market_cap,
                                tx_type: tx_type.to_string(),
                                timestamp: Instant::now(),
                            };
                            
                            if let Err(_) = trade_sender.send(trade_update) {
                                eprintln!("❌ Failed to send trade update");
                            }
                        }
                        
                        // Display table every 250ms for more frequent updates
                        if last_display.elapsed() >= Duration::from_millis(250) {
                            let tokens_guard = tokens_arc.lock().await;
                            let terminal_guard = terminal_arc.lock().await;
                            
                            terminal_guard.render_table(&*tokens_guard, message_count);
                            terminal_guard.render_transaction_log();
                            
                            last_display = Instant::now();
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        println!("🔍 Binary message #{}: {} bytes", message_count, data.len());
                    }
                    Some(Ok(Message::Ping(data))) => {
                        println!("🔍 Ping message #{}: {} bytes", message_count, data.len());
                    }
                    Some(Ok(Message::Pong(data))) => {
                        println!("🔍 Pong message #{}: {} bytes", message_count, data.len());
                    }
                    Some(Ok(Message::Close(_))) => {
                        println!("🔍 Close message #{}", message_count);
                    }
                    Some(Ok(Message::Frame(_))) => {
                        println!("🔍 Frame message #{}", message_count);
                    }
                    Some(Err(e)) => {
                        eprintln!("❌ WebSocket error: {}", e);
                        return Err(e.into());
                    }
                    None => {
                        println!("🔍 WebSocket connection closed by server");
                        return Err("WebSocket connection closed by server".into());
                    }
                }
            }
            // Handle subscription requests
            mint = subscription_receiver.recv() => {
                match mint {
                    Some(mint) => {
                        let trade_sub = json!({
                            "method": "subscribeTokenTrade",
                            "keys": [mint]
                        });
                        
                        if let Err(e) = write.send(Message::Text(trade_sub.to_string())).await {
                            eprintln!("❌ Failed to send subscription: {}", e);
                        } else {
                            println!("🔍 Subscribed to trades for token: {}", &mint[..8]);
                        }
                    }
                    None => break,
                }
            }
            // Handle cleanup unsubscribe requests
            mint = cleanup_receiver.recv() => {
                match mint {
                    Some(mint) => {
                        let unsubscribe_msg = json!({
                            "method": "unsubscribeTokenTrade",
                            "keys": [mint]
                        });
                        
                        if let Err(e) = write.send(Message::Text(unsubscribe_msg.to_string())).await {
                            eprintln!("❌ Failed to send unsubscribe: {}", e);
                        } else {
                            println!("🔍 Unsubscribed from token: {}", &mint[..8]);
                        }
                    }
                    None => break,
                }
            }
        }
    }
    
    // Return the current token state for reconnection
    let tokens = tokens_arc.lock().await;
    Ok(tokens.clone())
}

// API Handler Functions
impl From<&TokenInfo> for ApiTokenInfo {
    fn from(token: &TokenInfo) -> Self {
        let age_seconds = token.creation_time.elapsed().as_secs();
        
        ApiTokenInfo {
            name: token.name.clone(),
            symbol: token.symbol.clone(),
            mint: token.mint.clone(),
            price_sol: token.price_sol,
            price_usd: token.price_usd,
            market_cap_sol: token.market_cap_sol,
            market_cap_usd: token.market_cap_usd,
            volume_24h_sol: token.volume_24h_sol,
            volume_24h_usd: token.volume_24h_usd,
            trades_24h: token.trades_24h,
            price_change_24h: token.price_change_24h,
            total_trades: token.total_trades,
            age_seconds,
            is_subscribed: token.is_subscribed,
            initial_price_usd: token.initial_price_usd,
            vwap: token.rolling_metrics.get_vwap(),
            rolling_volatility: token.rolling_metrics.get_rolling_volatility(),
            momentum: token.rolling_metrics.get_momentum(),
            skewness: token.rolling_metrics.get_skewness(),
            kurtosis: token.rolling_metrics.get_kurtosis(),
            trade_intensity: token.rolling_metrics.get_trade_intensity(),
            reliability_score: token.rolling_metrics.get_reliability_score(),
        }
    }
}

// GET /api/coins - Get all tracked coin data
async fn get_all_coins(State(state): State<AppState>) -> Result<Json<ApiResponse<Vec<ApiTokenInfo>>>, StatusCode> {
    let tokens = state.tokens.lock().await;
    let api_tokens: Vec<ApiTokenInfo> = tokens.values().map(|token| token.into()).collect();
    
    let response = ApiResponse {
        success: true,
        data: Some(api_tokens),
        error: None,
        timestamp: chrono::Utc::now().timestamp(),
    };
    
    Ok(Json(response))
}

// GET /api/coins/list - Get current tracked coins list
async fn get_coins_list(State(state): State<AppState>) -> Result<Json<ApiResponse<ApiCoinsList>>, StatusCode> {
    let tokens = state.tokens.lock().await;
    let coins: Vec<String> = tokens.keys().cloned().collect();
    let total_count = coins.len();
    
    let coins_list = ApiCoinsList {
        coins,
        total_count,
    };
    
    let response = ApiResponse {
        success: true,
        data: Some(coins_list),
        error: None,
        timestamp: chrono::Utc::now().timestamp(),
    };
    
    Ok(Json(response))
}

// GET /api/coins/prices - Get current coin prices
async fn get_coin_prices(State(state): State<AppState>) -> Result<Json<ApiResponse<ApiCoinPrices>>, StatusCode> {
    let tokens = state.tokens.lock().await;
    let terminal = state.terminal.lock().await;
    
    let prices: Vec<ApiCoinPrice> = tokens.values().map(|token| ApiCoinPrice {
        mint: token.mint.clone(),
        symbol: token.symbol.clone(),
        price_sol: token.price_sol,
        price_usd: token.price_usd,
        price_change_24h: token.price_change_24h,
    }).collect();
    
    let coin_prices = ApiCoinPrices {
        prices,
        sol_price_usd: terminal.sol_price_usd,
    };
    
    let response = ApiResponse {
        success: true,
        data: Some(coin_prices),
        error: None,
        timestamp: chrono::Utc::now().timestamp(),
    };
    
    Ok(Json(response))
}

// GET /api/coins/{mint} - Get specific coin data
async fn get_coin_by_mint(
    Path(mint): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<ApiTokenInfo>>, StatusCode> {
    let tokens = state.tokens.lock().await;
    
    if let Some(token) = tokens.get(&mint) {
        let api_token: ApiTokenInfo = token.into();
        
        let response = ApiResponse {
            success: true,
            data: Some(api_token),
            error: None,
            timestamp: chrono::Utc::now().timestamp(),
        };
        
        Ok(Json(response))
    } else {
        let response = ApiResponse {
            success: false,
            data: None,
            error: Some(format!("Token with mint '{}' not found", mint)),
            timestamp: chrono::Utc::now().timestamp(),
        };
        
        Ok(Json(response))
    }
}

// Create the API router
fn create_api_router(state: AppState) -> Router {
    Router::new()
        .route("/api/coins", get(get_all_coins))
        .route("/api/coins/list", get(get_coins_list))
        .route("/api/coins/prices", get(get_coin_prices))
        .route("/api/coins/:mint", get(get_coin_by_mint))
        .layer(
            ServiceBuilder::new()
                .layer(CorsLayer::permissive())
        )
        .with_state(state)
}

// Start the HTTP API server
async fn start_api_server(state: AppState) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_api_router(state);
    
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("🌐 API server running on http://0.0.0.0:3000");
    println!("📡 Available endpoints:");
    println!("  GET /api/coins - Get all tracked coin data");
    println!("  GET /api/coins/list - Get current tracked coins list");
    println!("  GET /api/coins/prices - Get current coin prices");
    println!("  GET /api/coins/{{mint}} - Get specific coin data");
    
    axum::serve(listener, app).await?;
    Ok(())
}

// Modified WebSocket connection function that uses shared state for API
async fn run_websocket_connection_with_api(
    tokens: HashMap<String, TokenInfo>,
    tokens_arc: Arc<Mutex<HashMap<String, TokenInfo>>>,
    terminal_arc: Arc<Mutex<TerminalInterface>>,
) -> Result<HashMap<String, TokenInfo>, Box<dyn std::error::Error>> {
    let url = "wss://pumpportal.fun/api/data";
    let (ws_stream, response) = connect_async(url).await?;
    println!("✅ Connected to PumpPortal websocket");
    println!("📡 Response status: {}", response.status());

    let (mut write, mut read) = ws_stream.split();
    
    // Create channel for trade updates
    let (trade_sender, mut trade_receiver) = mpsc::unbounded_channel::<TradeUpdate>();
    
    // Create a channel for sending subscription requests
    let (_subscription_sender, mut subscription_receiver) = mpsc::unbounded_channel::<String>();
    
    // Initialize Parquet data writer
    let data_writer = Arc::new(Mutex::new(DataWriter::new("./data/pumpfun")));
    
    // Fetch SOL price
    {
        let mut terminal = terminal_arc.lock().await;
        terminal.update_sol_price().await;
    }
    
    // Hide cursor and clear screen
    execute!(io::stdout(), Hide, Clear(ClearType::All)).unwrap();
    {
        let terminal = terminal_arc.lock().await;
        terminal.render_header();
    }
    
    // Start background price monitoring
    TerminalInterface::start_price_monitor(terminal_arc.clone());

    // Subscribe to new token creations
    let new_token_sub = json!({
        "method": "subscribeNewToken"
    });

    // Subscribe to all token trades using the correct format
    let trade_sub = json!({
        "method": "pumpFunTradeSubscribe",
        "params": {
            "coinAddress": "all",
            "referenceId": "REF#1"
        }
    });

    // Also try the original subscription method
    let trade_sub_alt = json!({
        "method": "subscribeTokenTrade",
        "keys": []
    });
    
    println!("📤 Sending subscription messages...");
    write.send(Message::Text(new_token_sub.to_string())).await.expect("failed to send new token subscribe msg");
    write.send(Message::Text(trade_sub.to_string())).await.expect("failed to send trade subscribe msg");
    write.send(Message::Text(trade_sub_alt.to_string())).await.expect("failed to send trade subscribe alt msg");
    
    // Initialize tokens in shared state
    {
        let mut tokens_guard = tokens_arc.lock().await;
        *tokens_guard = tokens.clone();
    }
    
    // Resubscribe to existing tokens if this is a reconnection
    {
        let tokens_guard = tokens_arc.lock().await;
        if !tokens_guard.is_empty() {
            println!("🔄 Resubscribing to {} existing tokens...", tokens_guard.len());
            for (mint, token) in tokens_guard.iter() {
                if token.is_subscribed {
                    let token_sub = json!({
                        "method": "subscribeTokenTrade",
                        "keys": [mint]
                    });
                    if let Err(e) = write.send(Message::Text(token_sub.to_string())).await {
                        eprintln!("❌ Failed to resubscribe to {}: {}", token.symbol, e);
                    } else {
                        println!("✅ Resubscribed to {}", token.symbol);
                    }
                }
            }
        }
    }
    
    println!("✅ Subscriptions sent successfully");
    println!("🔍 Monitoring Pump Fun activity via PumpPortal...");
    println!("Press Ctrl+C to exit\n");

    let mut last_display = Instant::now();
    let mut message_count = 0;
    let tokens_for_trades = tokens_arc.clone();
    
    // Create channel for cleanup manager to send unsubscribe messages
    let (cleanup_sender, mut cleanup_receiver) = mpsc::unbounded_channel::<String>();
    
    // Create cleanup manager for inactive tokens
    let cleanup_manager = TokenCleanupManager::new(tokens_arc.clone(), cleanup_sender);
    cleanup_manager.start_cleanup();
    
    // Start background token price updater
    let tokens_arc_for_updater = tokens_arc.clone();
    let terminal_arc_for_updater = terminal_arc.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            {
                let mut tokens = tokens_arc_for_updater.lock().await;
                let terminal = terminal_arc_for_updater.lock().await;
                let sol_price = terminal.sol_price_usd;
                
                // Update all token USD values with current SOL price
                for token in tokens.values_mut() {
                    token.price_usd = token.price_sol * sol_price;
                    token.market_cap_usd = token.market_cap_sol * sol_price;
                    token.volume_24h_usd = token.volume_24h_sol * sol_price;
                }
            }
        }
    });
    
    let terminal_for_trades = terminal_arc.clone();
    
    tokio::spawn(async move {
        while let Some(trade_update) = trade_receiver.recv().await {
            let mut tokens = tokens_for_trades.lock().await;
            if let Some(token) = tokens.get_mut(&trade_update.mint) {
                let _old_price = token.price_usd;
                token.market_cap_sol = trade_update.market_cap;
                token.volume_24h_sol += trade_update.sol_amount;
                token.trades_24h += 1;
                token.last_trade_time = trade_update.timestamp;
                
                // Get SOL price from terminal
                let sol_price = {
                    let terminal_guard = terminal_for_trades.lock().await;
                    terminal_guard.sol_price_usd
                };
                
                // Price and price change are already updated in the main message handler
                // Just update volume here
                token.volume_24h_sol += trade_update.sol_amount;
                token.volume_24h_usd += trade_update.sol_amount * sol_price;
                
                // Add to transaction log
                {
                    let mut terminal = terminal_for_trades.lock().await;
                    terminal.add_transaction(
                        token.symbol.clone(),
                        trade_update.tx_type.clone(),
                        trade_update.sol_amount,
                        trade_update.market_cap
                    );
                }
            }
        }
    });
    
    // Handle subscriptions and messages in the main loop
    loop {
        tokio::select! {
            // Handle incoming WebSocket messages
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        message_count += 1;
                        let data: serde_json::Value = match serde_json::from_str(&text) {
                            Ok(parsed) => parsed,
                            Err(e) => {
                                eprintln!("❌ JSON parse error: {}", e);
                                continue;
                            }
                        };
                        
                        // Skip subscription confirmations
                        if data.get("message").is_some() {
                            continue;
                        }
                        
                        // Export trade data to Parquet
                        handle_message_for_export(&text, &data_writer, &tokens_arc);
                        
                        // Handle new token creation events
                        if data.get("txType") == Some(&serde_json::Value::String("create".to_string())) {
                            let name = data["name"].as_str().unwrap_or("Unknown");
                            let symbol = data["symbol"].as_str().unwrap_or("???");
                            let mint = data["mint"].as_str().unwrap_or("unknown");
                            let market_cap = data["marketCapSol"].as_f64().unwrap_or(0.0);
                            let sol_amount = data["solAmount"].as_f64().unwrap_or(0.0);
                            
                            // Calculate price from bonding curve data
                            let v_sol = data["vSolInBondingCurve"].as_f64().unwrap_or(0.0);
                            let v_tokens = data["vTokensInBondingCurve"].as_f64().unwrap_or(0.0);
                            
                            let price_sol = if v_tokens > 0.0 {
                                v_sol / v_tokens
                            } else {
                                0.0
                            };
                            
                            // Get SOL price from terminal
                            let sol_price = {
                                let terminal_guard = terminal_arc.lock().await;
                                terminal_guard.sol_price_usd
                            };
                            
                            let price_usd = price_sol * sol_price;
                            let market_cap_usd = market_cap * sol_price;
                            let volume_24h_usd = sol_amount * sol_price;
                            
                            let now = Instant::now();
                            let token_info = TokenInfo {
                                name: name.to_string(),
                                symbol: symbol.to_string(),
                                mint: mint.to_string(),
                                price_sol: price_sol,
                                price_usd: price_usd,
                                market_cap_sol: market_cap,
                                market_cap_usd: market_cap_usd,
                                volume_24h_sol: sol_amount,
                                volume_24h_usd: volume_24h_usd,
                                trades_24h: 1,
                                last_trade_time: now,
                                price_change_24h: 0.0,
                                total_trades: 1,
                                creation_time: now,
                                last_activity_time: now,
                                is_subscribed: false,
                                initial_price_usd: price_usd,
                                price_history: vec![(now, price_usd)],
                                subscription_start_time: None,
                                rolling_metrics: RollingMetrics::new(),
                            };
                            
                            {
                                let mut tokens_guard = tokens_arc.lock().await;
                                tokens_guard.insert(mint.to_string(), token_info);
                            }
                            
                            // Subscribe to trades for this token
                            let trade_sub = json!({
                                "method": "subscribeTokenTrade",
                                "keys": [mint]
                            });
                            
                            if let Err(e) = write.send(Message::Text(trade_sub.to_string())).await {
                                eprintln!("❌ Failed to send subscription: {}", e);
                            } else {
                                println!("🔍 Subscribed to trades for token: {}", &mint[..8]);
                                
                                // Mark token as subscribed and record subscription start time
                                {
                                    let mut tokens_guard = tokens_arc.lock().await;
                                    if let Some(token) = tokens_guard.get_mut(mint) {
                                        token.is_subscribed = true;
                                        token.subscription_start_time = Some(Instant::now());
                                    }
                                }
                            }
                            
                            // Add to transaction log as new token creation
                            {
                                let mut terminal = terminal_arc.lock().await;
                                terminal.add_transaction(
                                    symbol.to_string(),
                                    "CREATE".to_string(),
                                    sol_amount,
                                    market_cap
                                );
                            }
                        }
                        
                        // Handle trade events (buy/sell) - check for any transaction that's not create
                        else if data.get("txType").is_some() && 
                                data.get("txType") != Some(&serde_json::Value::String("create".to_string())) {
                            
                            let mint = data["mint"].as_str().unwrap_or("unknown");
                            let sol_amount = data["solAmount"].as_f64().unwrap_or(0.0);
                            let market_cap = data["marketCapSol"].as_f64().unwrap_or(0.0);
                            let tx_type = data["txType"].as_str().unwrap_or("unknown");
                            
                            // Update token activity
                            {
                                let mut tokens_guard = tokens_arc.lock().await;
                                if let Some(token) = tokens_guard.get_mut(mint) {
                                    token.total_trades += 1;
                                    token.last_activity_time = Instant::now();
                                    token.trades_24h += 1;
                                    token.last_trade_time = Instant::now();
                                    
                                    // Update price and market cap from trade data
                                    let v_sol = data["vSolInBondingCurve"].as_f64().unwrap_or(0.0);
                                    let v_tokens = data["vTokensInBondingCurve"].as_f64().unwrap_or(0.0);
                                    
                                    // Store old price for change calculation
                                    let old_price_usd = token.price_usd;
                                    
                                    if v_tokens > 0.0 {
                                        token.price_sol = v_sol / v_tokens;
                                    }
                                    token.market_cap_sol = market_cap;
                                    
                                    // Update USD values
                                    let sol_price = {
                                        let terminal_guard = terminal_arc.lock().await;
                                        terminal_guard.sol_price_usd
                                    };
                                    token.price_usd = token.price_sol * sol_price;
                                    token.market_cap_usd = token.market_cap_sol * sol_price;
                                    token.volume_24h_usd = token.volume_24h_sol * sol_price;
                                    
                                    // Add to price history
                                    token.price_history.push((Instant::now(), token.price_usd));
                                    
                                    // Keep only last 5 minutes of history to prevent memory bloat
                                    let cutoff_time = Instant::now() - Duration::from_secs(300);
                                    token.price_history.retain(|(timestamp, _)| *timestamp >= cutoff_time);
                                    
                                    // Calculate price change
                                    if old_price_usd > 0.0 {
                                        token.price_change_24h = ((token.price_usd - old_price_usd) / old_price_usd) * 100.0;
                                    } else if token.initial_price_usd > 0.0 {
                                        token.price_change_24h = ((token.price_usd - token.initial_price_usd) / token.initial_price_usd) * 100.0;
                                    }
                                }
                            }
                            
                            // Send trade update
                            let trade_update = TradeUpdate {
                                mint: mint.to_string(),
                                sol_amount: sol_amount,
                                market_cap: market_cap,
                                tx_type: tx_type.to_string(),
                                timestamp: Instant::now(),
                            };
                            
                            if let Err(_) = trade_sender.send(trade_update) {
                                eprintln!("❌ Failed to send trade update");
                            }
                        }
                        
                        // Display table every 250ms for more frequent updates
                        if last_display.elapsed() >= Duration::from_millis(250) {
                            let tokens_guard = tokens_arc.lock().await;
                            let terminal_guard = terminal_arc.lock().await;
                            
                            terminal_guard.render_table(&*tokens_guard, message_count);
                            terminal_guard.render_transaction_log();
                            
                            last_display = Instant::now();
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        println!("🔍 Binary message #{}: {} bytes", message_count, data.len());
                    }
                    Some(Ok(Message::Ping(data))) => {
                        println!("🔍 Ping message #{}: {} bytes", message_count, data.len());
                    }
                    Some(Ok(Message::Pong(data))) => {
                        println!("🔍 Pong message #{}: {} bytes", message_count, data.len());
                    }
                    Some(Ok(Message::Close(_))) => {
                        println!("🔍 Close message #{}", message_count);
                    }
                    Some(Ok(Message::Frame(_))) => {
                        println!("🔍 Frame message #{}", message_count);
                    }
                    Some(Err(e)) => {
                        eprintln!("❌ WebSocket error: {}", e);
                        return Err(e.into());
                    }
                    None => {
                        println!("🔍 WebSocket connection closed by server");
                        return Err("WebSocket connection closed by server".into());
                    }
                }
            }
            // Handle subscription requests
            mint = subscription_receiver.recv() => {
                match mint {
                    Some(mint) => {
                        let trade_sub = json!({
                            "method": "subscribeTokenTrade",
                            "keys": [mint]
                        });
                        
                        if let Err(e) = write.send(Message::Text(trade_sub.to_string())).await {
                            eprintln!("❌ Failed to send subscription: {}", e);
                        } else {
                            println!("🔍 Subscribed to trades for token: {}", &mint[..8]);
                        }
                    }
                    None => break,
                }
            }
            // Handle cleanup unsubscribe requests
            mint = cleanup_receiver.recv() => {
                match mint {
                    Some(mint) => {
                        let unsubscribe_msg = json!({
                            "method": "unsubscribeTokenTrade",
                            "keys": [mint]
                        });
                        
                        if let Err(e) = write.send(Message::Text(unsubscribe_msg.to_string())).await {
                            eprintln!("❌ Failed to send unsubscribe: {}", e);
                        } else {
                            println!("🔍 Unsubscribed from token: {}", &mint[..8]);
                        }
                    }
                    None => break,
                }
            }
        }
    }
    
    // Return the current token state for reconnection
    let tokens = tokens_arc.lock().await;
    Ok(tokens.clone())
}

#[tokio::main]
async fn main() {
    // Run token tracker with API server
        let mut retry_count = 0;
        let max_retries = 10;
        let mut tokens: HashMap<String, TokenInfo> = HashMap::new();
        
        // Create shared state for API
        let tokens_arc = Arc::new(Mutex::new(tokens.clone()));
        let terminal_arc = Arc::new(Mutex::new(TerminalInterface::new()));
        
        let api_state = AppState {
            tokens: tokens_arc.clone(),
            terminal: terminal_arc.clone(),
        };
        
        // Start API server in background
        let api_server_handle = {
            let api_state = api_state.clone();
            tokio::spawn(async move {
                if let Err(e) = start_api_server(api_state).await {
                    eprintln!("❌ API server error: {}", e);
                }
            })
        };
        
        // Run WebSocket connection with retry logic
        loop {
            match run_websocket_connection_with_api(tokens.clone(), tokens_arc.clone(), terminal_arc.clone()).await {
                Ok(returned_tokens) => {
                    tokens = returned_tokens;
                    println!("✅ WebSocket connection closed normally");
                    break;
                }
                Err(e) => {
                    retry_count += 1;
                    println!("❌ WebSocket error: {}", e);
                    
                    if retry_count > max_retries {
                        eprintln!("❌ Max retries ({}) exceeded. Exiting.", max_retries);
                        break;
                    }
                    
                    // Exponential backoff: 2^retry_count seconds, max 60 seconds
                    let delay_seconds = std::cmp::min(2_u64.pow(retry_count as u32), 60);
                    println!("🔄 Reconnecting in {} seconds... (attempt {}/{})", delay_seconds, retry_count, max_retries);
                    
                    tokio::time::sleep(Duration::from_secs(delay_seconds)).await;
                }
            }
        }
        
        // Shutdown API server
        api_server_handle.abort();
}

fn display_token_table(tokens: &HashMap<String, TokenInfo>, message_count: u32) {
    // Clear screen (works on Windows and Unix)
    print!("\x1B[2J\x1B[1;1H");
    
    println!("📊 PUMP FUN TOKEN TRACKER - Live Prices & Stats");
    println!("{}", "=".repeat(80));
    println!("{:<12} {:<15} {:<10} {:<12} {:<10} {:<8} {:<10}", 
             "SYMBOL", "NAME", "PRICE", "MARKET CAP", "VOLUME", "TRADES", "CHANGE");
    println!("{}", "=".repeat(80));
    
    if tokens.is_empty() {
        println!("⏳ Waiting for new tokens...");
        return;
    }
    
    // Sort tokens by market cap (highest first)
    let mut sorted_tokens: Vec<_> = tokens.values().collect();
    sorted_tokens.sort_by(|a, b| b.market_cap_usd.partial_cmp(&a.market_cap_usd).unwrap());
    
    // Show top 15 tokens to fit better on screen
    for token in sorted_tokens.iter().take(15) {
        let change_color = if token.price_change_24h > 0.0 { "��" } else { "🔴" };
        let change_text = format!("{}{:.1}%", change_color, token.price_change_24h);
        
        println!("{:<12} {:<15} {:<10} {:<12} {:<10} {:<8} {:<10}",
                 &token.symbol[..std::cmp::min(12, token.symbol.len())],
                 &token.name[..std::cmp::min(15, token.name.len())],
                 format!("${:.8}", token.price_usd),
                 format!("${:.2}", token.market_cap_usd),
                 format!("${:.2}", token.volume_24h_usd),
                 token.trades_24h,
                 change_text);
    }
    
    println!("{}", "=".repeat(80));
    println!("🔄 Last updated: {}", chrono::Utc::now().format("%H:%M:%S"));
    println!("📈 Total tokens tracked: {}", tokens.len());
    println!("📨 Messages received: {}", message_count);
}