# Subscription Opportunity Analysis — Online Retail II

## Overview

This project analyses two years of transactional data from a UK-based online retailer to answer a concrete business question: **can we offer a subscription model, and if so, to whom and for which products?**

The analysis is built entirely in SQL (BigQuery) and covers the full pipeline — from raw data ingestion and cleaning through to a scored, tiered output of subscription-ready customers and products. Results are visualised in Tableau Public.

---

## Dataset

**Source:** Online Retail II — UCI Machine Learning Repository  
**Period:** December 2009 – December 2011  
**Raw size:** 1,067,371 rows × 8 columns  
**Columns:** Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer ID, Country

---

## Data Cleaning

Five filters were applied to the raw data, each with a specific analytical reason:

| Filter | Rows removed | Reason |
|---|---|---|
| Missing Customer ID | ~243,007 | Anonymous transactions cannot be tracked across time |
| Cancelled invoices (C% prefix) | ~19,494 | Reversed transactions inflate frequency counts |
| Non-product stock codes | ~6,093 | Admin entries (POST, BANK CHARGES, etc.) are not purchasable products |
| Quantity ≤ 0 | ~3,457 | Orphaned returns and data entry errors |
| Price ≤ 0 | ~6,207 | Samples, write-offs, and system adjustments |

**Clean dataset: 802,632 rows across 5,852 customers.**

Missing product descriptions (4,382 rows) were recovered by looking up the most commonly used description for each StockCode across all other rows — preserving valid transaction data rather than dropping rows.

---

## Methodology

### Customer scoring

For each customer with 3+ orders, the following features were computed:

- `orders_per_month` — normalized order frequency
- `avg_days_between_orders` — mean inter-order gap using `LAG()`
- `cv_order_interval` — coefficient of variation of order gaps (stddev / avg), measuring how *consistent* the ordering rhythm is
- `recency_days` — days since last order relative to dataset end date
- `order_count` — total number of distinct invoices

These features were combined into a 10-point subscription score across four components — frequency, regularity, recency, and loyalty — and bucketed into three tiers: **HIGH** (7+), **MEDIUM** (4–6), **LOW** (<4).

### Product scoring

For each product with 5+ unique buyers:

- `repeat_buyer_rate` — percentage of buyers who purchased the product more than once
- `avg_reorder_days` — average gap between first and second purchase of the same product by the same customer
- `cv_reorder_interval` — consistency of the replenishment cycle

Products were scored on repeat rate, reorder cycle fit (ideal: 14–45 days), cycle consistency, and market scale, then tiered identically to customers.

---

## Key Findings

- **560 HIGH-tier customers** — averaging 24.7 orders and £14,665 in total spend, ordering every 32 days on average. Ideal for a monthly subscription.
- **1,759 MEDIUM-tier customers** — averaging 8.5 orders and £3,540 in spend. Worth targeting with a trial offer.
- **129 HIGH-tier products** — with an average reorder cycle of 43.8 days and meaningful repeat buyer rates. Top candidates include everyday gifting and home accessories.
- **Geographic concentration** — 500 of 560 HIGH-tier customers are based in the UK, making it the clear pilot market.
- **Seasonal signal** — revenue spikes sharply every October–November, suggesting subscription launches should target September to capture the gifting season.

---

## Tech Stack

- **SQL:** BigQuery (Standard SQL)
- **Visualisation:** Tableau Public
- **Data source:** UCI ML Repository — Online Retail II

---

## Structure

```
├── subscription_analysis_bigquery.sql   # Full SQL project (all views + result queries)
└── README.md
```

The SQL is structured as a chain of views — each layer builds on the one below it:

```
raw_retail
  └── vw_cleaned
        ├── vw_customer_orders
        │     └── vw_customer_intervals
        │           └── vw_customer_features
        │                 └── vw_customer_subscription_score
        └── vw_product_purchase_history
              └── vw_product_features
                    └── vw_product_subscription_score
```
