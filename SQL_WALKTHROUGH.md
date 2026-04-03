# SQL Walkthrough — Subscription Opportunity Analysis

This document walks through every SQL query in the project in the order they are run, explaining what each one does and why it was written that way.

The project is structured as a chain of **views** — saved queries that build on top of each other. No data is duplicated or stored. Each view is a logical layer that feeds the next one.

```
raw table (a)
  └── vw_canonical_descriptions      ← fixes missing product names
  └── vw_cleaned                     ← applies all 5 cleaning filters
        ├── vw_customer_orders        ← one row per order
        │     └── vw_customer_intervals   ← gaps between orders
        │           └── vw_customer_features  ← full customer profile
        │                 └── vw_customer_subscription_score  ← scored + tiered
        └── vw_product_purchase_history  ← reorder gaps per product
              └── vw_product_features    ← full product profile
                    └── vw_product_subscription_score  ← scored + tiered
```

---

## Part 1 — Data Cleaning

### Step 1A — Fix missing product descriptions

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_canonical_descriptions` AS
WITH ranked AS (
    SELECT
        StockCode                               AS stock_code,
        Description                             AS description,
        ROW_NUMBER() OVER (
            PARTITION BY StockCode
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM `my-project-8037-491318.a.a`
    WHERE Description IS NOT NULL
      AND TRIM(Description) <> ''
    GROUP BY StockCode, Description
)
SELECT
    stock_code,
    description AS canonical_description
FROM ranked
WHERE rn = 1;
```

**What it does:**
4,382 rows in the raw data have no product description. Rather than dropping those rows, this view builds a lookup table: for each `StockCode`, it finds the most commonly used description across all other rows that share that code.

`ROW_NUMBER() OVER (PARTITION BY StockCode ORDER BY COUNT(*) DESC)` ranks descriptions by how often they appear for the same product. We then take only rank 1 — the most frequent one. This gives us a clean, reliable description for every stock code that has ever had one written anywhere in the dataset.

---

### Step 1B — The cleaned base table

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_cleaned` AS
SELECT
    r.Invoice                                AS invoice,
    r.StockCode                              AS stock_code,
    COALESCE(
        NULLIF(TRIM(r.Description), ''),
        cd.canonical_description,
        'Unknown'
    )                                        AS description,
    r.Quantity                               AS quantity,
    r.InvoiceDate                            AS invoice_date,
    r.Price                                  AS price,
    CAST(r.`Customer ID` AS INT64)           AS customer_id,
    r.Country                                AS country,
    r.Quantity * r.Price                     AS line_revenue,
    DATE(r.InvoiceDate)                      AS order_date

FROM `my-project-8037-491318.a.a` r
LEFT JOIN `my-project-8037-491318.a.vw_canonical_descriptions` cd
       ON r.StockCode = cd.stock_code

WHERE
    r.`Customer ID` IS NOT NULL
    AND r.Invoice NOT LIKE 'C%'
    AND REGEXP_CONTAINS(r.StockCode, r'^\d{5}')
    AND r.Quantity > 0
    AND r.Price > 0;
```

**What it does:**
This is the foundation of the entire project. Every other view reads from this one. It does two things: renames and standardises all columns into clean snake_case, and applies five filters to remove bad data.

**The five filters and why each one exists:**

| Filter | What it removes | Why |
|---|---|---|
| `Customer ID IS NOT NULL` | 243,007 anonymous rows | No customer ID means we can't track repeat behaviour over time — useless for subscription analysis |
| `Invoice NOT LIKE 'C%'` | 19,494 cancellations | Reversed transactions inflate purchase frequency — a customer who buys and cancels looks like a repeat buyer |
| `REGEXP_CONTAINS(StockCode, r'^\d{5}')` | 6,093 admin entries | Codes like POST, BANK CHARGES, AMAZONFEE are internal fees, not products |
| `Quantity > 0` | 3,457 rows | Negative quantities after cancellations are removed are orphaned returns or data errors |
| `Price > 0` | 6,207 rows | Zero or negative prices are samples, write-offs, or system adjustments — not real sales |

`COALESCE(NULLIF(TRIM(...), ''), cd.canonical_description, 'Unknown')` fills missing descriptions: first try the row's own description, then the lookup from Step 1A, then fall back to 'Unknown'.

`CAST(r.'Customer ID' AS INT64)` converts the customer ID from float (how BigQuery auto-detected it from the CSV) to integer. The backticks around `Customer ID` are required because the column name contains a space.

**Result: 802,632 clean rows across 5,852 customers.**

---

## Part 2 — Customer Features

### Step 2A — Customer orders

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_customer_orders` AS
SELECT
    customer_id,
    invoice,
    DATE(MIN(invoice_date))    AS order_date,
    SUM(line_revenue)          AS order_revenue
FROM `my-project-8037-491318.a.vw_cleaned`
GROUP BY customer_id, invoice;
```

**What it does:**
The cleaned data has one row per product line per invoice. A single invoice (order) might have 20 product lines. This view collapses all those lines into one row per order, giving us the order date and total revenue for that order. This is the starting point for computing timing between orders.

---

### Step 2B — Customer intervals

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_customer_intervals` AS
SELECT
    customer_id,
    invoice,
    order_date,
    order_revenue,
    LAG(order_date) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    )                                                        AS prev_order_date,
    DATE_DIFF(
        order_date,
        LAG(order_date) OVER (
            PARTITION BY customer_id
            ORDER BY order_date
        ),
        DAY
    )                                                        AS days_since_prev_order
FROM `my-project-8037-491318.a.vw_customer_orders`;
```

**What it does:**
This is the most important step for understanding customer behaviour. `LAG()` is a window function that looks at the previous row within the same group — here, the previous order for the same customer ordered by date. `DATE_DIFF(..., DAY)` then converts the gap to a number of days.

For each customer's first order, `LAG()` returns NULL because there is no previous order. For every order after that, it returns the gap in days since the previous one. These gaps are what we use to compute how regular and frequent a customer's ordering pattern is.

---

### Step 2C — Customer features

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_customer_features` AS
WITH interval_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT invoice)                              AS order_count,
        ROUND(SUM(order_revenue), 2)                         AS total_spend,
        ROUND(AVG(order_revenue), 2)                         AS avg_order_value,
        MIN(order_date)                                      AS first_order_date,
        MAX(order_date)                                      AS last_order_date,
        ROUND(AVG(days_since_prev_order), 1)                 AS avg_days_between_orders,
        ROUND(STDDEV(days_since_prev_order), 1)              AS stddev_days_between_orders,
        CASE
            WHEN AVG(days_since_prev_order) > 0
            THEN ROUND(
                STDDEV(days_since_prev_order) / AVG(days_since_prev_order), 2
            )
            ELSE NULL
        END                                                  AS cv_order_interval
    FROM `my-project-8037-491318.a.vw_customer_intervals`
    GROUP BY customer_id
),
product_counts AS (
    SELECT
        customer_id,
        COUNT(DISTINCT stock_code)                           AS unique_products_bought
    FROM `my-project-8037-491318.a.vw_cleaned`
    GROUP BY customer_id
)
SELECT
    i.customer_id,
    i.order_count,
    p.unique_products_bought,
    i.total_spend,
    i.avg_order_value,
    i.first_order_date,
    i.last_order_date,
    DATE_DIFF(DATE('2011-12-09'), i.last_order_date, DAY)    AS recency_days,
    GREATEST(
        ROUND(DATE_DIFF(i.last_order_date, i.first_order_date, DAY) / 30.0, 1),
        1
    )                                                        AS months_active,
    ROUND(
        i.order_count /
        GREATEST(DATE_DIFF(i.last_order_date, i.first_order_date, DAY) / 30.0, 1),
        2
    )                                                        AS orders_per_month,
    i.avg_days_between_orders,
    i.stddev_days_between_orders,
    i.cv_order_interval
FROM interval_stats i
JOIN product_counts p ON i.customer_id = p.customer_id;
```

**What it does:**
Builds a complete behavioural profile for each customer. It uses two CTEs (WITH blocks) because the data needed comes from two different sources:

- `interval_stats` — reads from `vw_customer_intervals` to get all timing and spend metrics
- `product_counts` — reads from `vw_cleaned` directly to count unique products, because `vw_customer_intervals` only has order-level data and doesn't know which products were in each order

**The key metrics and what they mean:**

`cv_order_interval` (coefficient of variation) = `STDDEV / AVG` of the gaps between orders. This measures how *consistent* the ordering rhythm is, not just how frequent. A customer who orders every 28–32 days has a low CV (very consistent). A customer who orders randomly has a high CV. Low CV is the strongest signal that a customer would respond well to a fixed subscription cycle.

`orders_per_month` normalises order frequency by dividing the number of orders by months active. This makes a customer with 6 orders over 2 months comparable to one with 6 orders over 2 years — they are very different customers.

`recency_days` uses `DATE('2011-12-09')` (the last date in the dataset) as the reference point instead of `CURRENT_DATE`. The data is historical. Using today's date would make every customer look equally inactive.

`GREATEST(..., 1)` prevents division by zero for customers who made all their purchases on the same day.

---

## Part 3 — Product Features

### Step 3A — Product purchase history

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_product_purchase_history` AS
SELECT
    c.customer_id,
    c.stock_code,
    c.description,
    c.order_date,
    LAG(c.order_date) OVER (
        PARTITION BY c.customer_id, c.stock_code
        ORDER BY c.order_date
    )                                                    AS prev_purchase_date,
    DATE_DIFF(
        c.order_date,
        LAG(c.order_date) OVER (
            PARTITION BY c.customer_id, c.stock_code
            ORDER BY c.order_date
        ),
        DAY
    )                                                    AS days_to_reorder
FROM (
    SELECT DISTINCT
        customer_id,
        stock_code,
        description,
        order_date
    FROM `my-project-8037-491318.a.vw_cleaned`
) c;
```

**What it does:**
Same `LAG()` logic as Step 2B, but partitioned by both `customer_id` AND `stock_code`. Instead of asking "how long between this customer's orders?", we're asking "how long before this customer bought this specific product again?"

The inner `SELECT DISTINCT` deduplicates to one row per (customer, product, day) first. A single invoice can have multiple line items of the same product — without deduplication, those would create false zero-day reorder gaps.

---

### Step 3B — Product features

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_product_features` AS
WITH buyer_counts AS (
    SELECT
        stock_code,
        customer_id,
        COUNT(*) AS purchase_occasions
    FROM `my-project-8037-491318.a.vw_product_purchase_history`
    GROUP BY stock_code, customer_id
),
product_agg AS (
    SELECT
        stock_code,
        COUNT(DISTINCT customer_id)                         AS unique_buyers,
        COUNTIF(purchase_occasions >= 2)                    AS repeat_buyers
    FROM buyer_counts
    GROUP BY stock_code
),
reorder_stats AS (
    SELECT
        stock_code,
        ROUND(AVG(days_to_reorder), 1)                      AS avg_reorder_days,
        ROUND(STDDEV(days_to_reorder), 1)                   AS stddev_reorder_days,
        CASE
            WHEN AVG(days_to_reorder) > 0
            THEN ROUND(STDDEV(days_to_reorder) / AVG(days_to_reorder), 2)
            ELSE NULL
        END                                                 AS cv_reorder_interval
    FROM `my-project-8037-491318.a.vw_product_purchase_history`
    WHERE days_to_reorder IS NOT NULL
    GROUP BY stock_code
),
revenue_stats AS (
    SELECT
        stock_code,
        MAX(description)                                    AS description,
        SUM(quantity)                                       AS total_units_sold,
        ROUND(AVG(price), 2)                                AS avg_unit_price,
        ROUND(SUM(line_revenue), 2)                         AS total_revenue
    FROM `my-project-8037-491318.a.vw_cleaned`
    GROUP BY stock_code
)
SELECT
    rs.stock_code,
    rs.description,
    rs.total_units_sold,
    rs.avg_unit_price,
    rs.total_revenue,
    pa.unique_buyers,
    pa.repeat_buyers,
    ROUND(pa.repeat_buyers / NULLIF(pa.unique_buyers, 0), 3)   AS repeat_buyer_rate,
    ro.avg_reorder_days,
    ro.stddev_reorder_days,
    ro.cv_reorder_interval
FROM revenue_stats rs
JOIN product_agg pa     ON rs.stock_code = pa.stock_code
LEFT JOIN reorder_stats ro  ON rs.stock_code = ro.stock_code;
```

**What it does:**
Aggregates all product-level signals into one profile per product using four CTEs:

- `buyer_counts` — counts how many separate purchase occasions each customer had for each product
- `product_agg` — uses `COUNTIF(purchase_occasions >= 2)` to count how many buyers came back for a second purchase (repeat buyers)
- `reorder_stats` — computes average reorder gap and CV for products where reorder data exists
- `revenue_stats` — total units, average price, total revenue per product

`NULLIF(pa.unique_buyers, 0)` prevents division by zero when computing repeat buyer rate. `repeat_buyer_rate` is the core signal: a product where 40%+ of buyers return for a second purchase is a natural subscription product.

`reorder_stats` uses a `LEFT JOIN` (not inner join) because some products have no reorder data — they were only ever bought once by any customer. Those products still get a row, just with NULL reorder metrics.

---

## Part 4 — Customer Subscription Score

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_customer_subscription_score` AS
WITH scored AS (
    SELECT
        cf.*,
        CASE
            WHEN orders_per_month >= 2.0  THEN 3
            WHEN orders_per_month >= 0.75 THEN 2
            WHEN orders_per_month >= 0.33 THEN 1
            ELSE 0
        END AS score_frequency,
        CASE
            WHEN cv_order_interval < 0.4  THEN 3
            WHEN cv_order_interval < 0.7  THEN 2
            WHEN cv_order_interval < 1.0  THEN 1
            ELSE 0
        END AS score_regularity,
        CASE
            WHEN recency_days <= 60  THEN 2
            WHEN recency_days <= 180 THEN 1
            ELSE 0
        END AS score_recency,
        CASE
            WHEN order_count >= 10 THEN 2
            WHEN order_count >= 5  THEN 1
            ELSE 0
        END AS score_loyalty
    FROM `my-project-8037-491318.a.vw_customer_features` cf
    WHERE order_count >= 3
)
SELECT
    s.*,
    score_frequency + score_regularity + score_recency + score_loyalty AS total_score,
    CASE
        WHEN score_frequency + score_regularity + score_recency + score_loyalty >= 7 THEN 'HIGH'
        WHEN score_frequency + score_regularity + score_recency + score_loyalty >= 4 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS subscription_tier
FROM scored s;
```

**What it does:**
Converts each customer's feature profile into a single 10-point score, then assigns them to a tier.

**Scoring breakdown (max 10 points):**

| Component | Max pts | Signal |
|---|---|---|
| Frequency (`orders_per_month`) | 3 | How often do they buy? |
| Regularity (`cv_order_interval`) | 3 | How consistent is the pattern? |
| Recency (`recency_days`) | 2 | Are they still active? |
| Loyalty (`order_count`) | 2 | Do they have a proven history? |

Regularity gets the same weight as frequency because a customer who orders unpredictably would not respond well to a fixed subscription billing cycle — even if they order often.

`WHERE order_count >= 3` — a minimum of 3 orders is required to enter scoring. With fewer than 3 orders there are fewer than 2 gaps, making it impossible to compute a meaningful standard deviation or CV.

**Tiers:** HIGH (7–10) · MEDIUM (4–6) · LOW (0–3)

---

## Part 5 — Product Subscription Score

```sql
CREATE OR REPLACE VIEW `my-project-8037-491318.a.vw_product_subscription_score` AS
WITH scored AS (
    SELECT
        pf.*,
        CASE
            WHEN repeat_buyer_rate >= 0.60 THEN 4
            WHEN repeat_buyer_rate >= 0.40 THEN 3
            WHEN repeat_buyer_rate >= 0.25 THEN 2
            WHEN repeat_buyer_rate >= 0.10 THEN 1
            ELSE 0
        END AS score_repeat_rate,
        CASE
            WHEN avg_reorder_days BETWEEN 14 AND 45   THEN 3
            WHEN avg_reorder_days BETWEEN 46 AND 90   THEN 2
            WHEN avg_reorder_days BETWEEN  7 AND 13   THEN 2
            WHEN avg_reorder_days BETWEEN 91 AND 180  THEN 1
            ELSE 0
        END AS score_cycle,
        CASE
            WHEN cv_reorder_interval < 0.5  THEN 2
            WHEN cv_reorder_interval < 1.0  THEN 1
            ELSE 0
        END AS score_consistency,
        CASE
            WHEN unique_buyers >= 20 THEN 1
            ELSE 0
        END AS score_scale
    FROM `my-project-8037-491318.a.vw_product_features` pf
    WHERE unique_buyers >= 5
)
SELECT
    s.*,
    score_repeat_rate + score_cycle + score_consistency + score_scale AS total_score,
    CASE
        WHEN score_repeat_rate + score_cycle + score_consistency + score_scale >= 7 THEN 'HIGH'
        WHEN score_repeat_rate + score_cycle + score_consistency + score_scale >= 4 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS subscription_tier
FROM scored s;
```

**What it does:**
Same scoring logic applied at the product level. Four components, max 10 points.

**Scoring breakdown:**

| Component | Max pts | Signal |
|---|---|---|
| Repeat buyer rate | 4 | What % of buyers came back? (dominant signal — highest weight) |
| Reorder cycle | 3 | Does the repurchase interval fit a subscription cadence? |
| Cycle consistency | 2 | Is the reorder interval predictable? |
| Market scale | 1 | Are there enough buyers to be statistically reliable? |

Repeat rate gets the most weight (4 pts) because it is the most direct signal that a product is naturally repurchased. A product with 60%+ repeat buyers is practically telling you it belongs in a subscription.

The reorder cycle score peaks at 14–45 days (3 pts) because that maps cleanly to a monthly subscription. Very fast cycles (under 7 days) and very slow ones (over 180 days) score zero — they don't fit a practical subscription model.

`WHERE unique_buyers >= 5` — products with fewer than 5 unique buyers are excluded. A repeat rate calculated from 3 buyers is statistically meaningless. The market scale bonus point requires 20+ buyers for the same reason.

**Tiers:** HIGH (7–10) · MEDIUM (4–6) · LOW (0–3)

---

## Part 6 — Result Queries

### 6A — Data quality audit

```sql
SELECT 'Total raw rows'                       AS step, COUNT(*) AS row_count
FROM `my-project-8037-491318.a.a`
UNION ALL
SELECT 'After removing nulls customer_id',    COUNT(*)
FROM `my-project-8037-491318.a.a` WHERE `Customer ID` IS NOT NULL
UNION ALL
SELECT 'After removing cancellations',        COUNT(*)
FROM `my-project-8037-491318.a.a`
WHERE `Customer ID` IS NOT NULL AND Invoice NOT LIKE 'C%'
UNION ALL
SELECT 'After removing non-product codes',    COUNT(*)
FROM `my-project-8037-491318.a.a`
WHERE `Customer ID` IS NOT NULL
  AND Invoice NOT LIKE 'C%'
  AND REGEXP_CONTAINS(StockCode, r'^\d{5}')
UNION ALL
SELECT 'After removing qty and price <= 0',   COUNT(*)
FROM `my-project-8037-491318.a.a`
WHERE `Customer ID` IS NOT NULL
  AND Invoice NOT LIKE 'C%'
  AND REGEXP_CONTAINS(StockCode, r'^\d{5}')
  AND Quantity > 0
  AND Price > 0
ORDER BY row_count DESC;
```

**What it does:**
Runs five `SELECT COUNT(*)` queries stacked together with `UNION ALL` — each one applies one more cleaning filter than the one above it. The result is a single table showing exactly how many rows survive after each step, ordered from largest to smallest.

This is a documentation query — you run it once to verify and present the cleaning process. It reads directly from the raw table, not from `vw_cleaned`, so it re-applies the filters from scratch and gives you an honest audit trail.

**Sample result (4 rows):**

| step | row_count |
|---|---|
| Total raw rows | 1,067,371 |
| After removing nulls customer_id | 824,364 |
| After removing cancellations | 804,870 |
| After removing non-product codes | 798,777 |
| After removing qty and price <= 0 | 802,632 |

---

### 6B — Customer subscription candidates

```sql
SELECT
    customer_id,
    subscription_tier,
    total_score,
    order_count,
    ROUND(orders_per_month, 2)           AS orders_per_month,
    avg_days_between_orders,
    cv_order_interval,
    recency_days,
    unique_products_bought,
    total_spend,
    avg_order_value,
    first_order_date,
    last_order_date,
    CASE
        WHEN avg_days_between_orders IS NULL    THEN 'No interval data'
        WHEN avg_days_between_orders <= 7       THEN 'Weekly buyer'
        WHEN avg_days_between_orders <= 30      THEN 'Monthly buyer'
        WHEN avg_days_between_orders <= 90      THEN 'Quarterly buyer'
        ELSE 'Low-frequency buyer'
    END AS cadence_label
FROM `my-project-8037-491318.a.vw_customer_subscription_score`
WHERE subscription_tier IN ('HIGH', 'MEDIUM')
ORDER BY
    CASE subscription_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 END,
    total_score DESC,
    total_spend DESC;
```

**What it does:**
The main customer output. Reads from `vw_customer_subscription_score` and returns every HIGH and MEDIUM tier customer with their full profile and score breakdown.

`WHERE subscription_tier IN ('HIGH', 'MEDIUM')` — LOW tier customers are excluded because they are not actionable subscription targets.

`ORDER BY CASE subscription_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 END` — sorts HIGH customers before MEDIUM ones. Within each tier, customers are ordered by score descending, then by total spend descending. This puts the most valuable, highest-scoring customers at the top.

`cadence_label` translates `avg_days_between_orders` into plain English. This makes the results readable without needing to interpret numbers.

---

### 6C — Customer tier summary

```sql
SELECT
    subscription_tier,
    COUNT(*)                                  AS customer_count,
    ROUND(AVG(order_count), 1)                AS avg_order_count,
    ROUND(AVG(orders_per_month), 2)           AS avg_orders_per_month,
    ROUND(AVG(avg_days_between_orders), 1)    AS avg_days_between_orders,
    ROUND(AVG(cv_order_interval), 2)          AS avg_cv,
    ROUND(AVG(total_spend), 2)                AS avg_total_spend,
    ROUND(AVG(recency_days), 0)               AS avg_recency_days
FROM `my-project-8037-491318.a.vw_customer_subscription_score`
GROUP BY subscription_tier
ORDER BY
    CASE subscription_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END;
```

**What it does:**
Collapses all customers into a three-row summary — one row per tier — showing average metrics for each group. This is a presentation query: it gives you the headline numbers to describe each tier at a glance.

`GROUP BY subscription_tier` collapses all individual customer rows into one row per tier. `AVG()` on each metric gives the typical customer profile for that tier.

**Sample result (4 rows):**

| subscription_tier | customer_count | avg_order_count | avg_orders_per_month | avg_days_between_orders | avg_cv | avg_total_spend | avg_recency_days |
|---|---|---|---|---|---|---|---|
| HIGH | 560 | 24.7 | 1.79 | 32.5 | 0.70 | £14,665 | 21 |
| MEDIUM | 1,759 | 8.5 | 0.64 | 79.3 | 0.80 | £3,540 | 91 |
| LOW | 970 | 4.3 | 0.39 | 123.9 | 1.00 | £1,663 | 206 |

---

### 6D — Product subscription candidates

```sql
SELECT
    stock_code,
    description,
    subscription_tier,
    total_score,
    unique_buyers,
    repeat_buyers,
    ROUND(repeat_buyer_rate * 100, 1)         AS repeat_buyer_pct,
    avg_reorder_days,
    stddev_reorder_days,
    cv_reorder_interval,
    total_units_sold,
    avg_unit_price,
    total_revenue,
    CASE
        WHEN avg_reorder_days IS NULL           THEN 'Insufficient reorder data'
        WHEN avg_reorder_days <= 13             THEN 'Weekly subscription'
        WHEN avg_reorder_days <= 45             THEN 'Monthly subscription'
        WHEN avg_reorder_days <= 90             THEN 'Bi-monthly / quarterly'
        WHEN avg_reorder_days <= 180            THEN 'Semi-annual subscription'
        ELSE 'Annual or irregular — not advised'
    END AS suggested_cadence
FROM `my-project-8037-491318.a.vw_product_subscription_score`
WHERE subscription_tier IN ('HIGH', 'MEDIUM')
ORDER BY
    CASE subscription_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 END,
    total_score DESC,
    repeat_buyer_rate DESC;
```

**What it does:**
The main product output — mirrors the structure of 6B but for products. Returns every HIGH and MEDIUM tier product with its full profile.

`repeat_buyer_rate * 100` converts the rate from a decimal (0.43) to a percentage (43.0%) for readability.

`suggested_cadence` translates `avg_reorder_days` into a recommended subscription interval. A product with an average reorder cycle of 28 days maps to a monthly subscription. A product at 70 days maps to bi-monthly. This turns a raw number into an actionable business recommendation.

`ORDER BY ... repeat_buyer_rate DESC` — within the same score, products with higher repeat rates are shown first because repeat rate is the strongest individual signal.

**Sample result (4 rows):**

| stock_code | description | subscription_tier | total_score | unique_buyers | repeat_buyers | repeat_buyer_pct | avg_reorder_days | suggested_cadence |
|---|---|---|---|---|---|---|---|---|
| 84705A | English Rose Photo Frame | HIGH | 9 | 5 | 3 | 60.0% | 28.0 | Monthly subscription |
| 21702 | Set 5 Mini Gateaux Fridge Magnets | HIGH | 8 | 5 | 2 | 40.0% | 31.0 | Monthly subscription |
| 22837 | Hot Water Bottle Babushka | HIGH | 8 | 221 | 96 | 43.4% | 21.8 | Monthly subscription |
| 23209 | Lunch Bag Vintage Doily | HIGH | 8 | 469 | 210 | 44.8% | 44.6 | Monthly subscription |

**Sample result (4 rows):**

| customer_id | subscription_tier | total_score | order_count | orders_per_month | avg_days_between_orders | cv_order_interval | recency_days | total_spend | cadence_label |
|---|---|---|---|---|---|---|---|---|---|
| 14096 | HIGH | 10 | 17 | 5.26 | 6.06 | 0.36 | 4 | £53,258 | Weekly buyer |
| 12841 | HIGH | 9 | 40 | 2.33 | 13.21 | 0.65 | 4 | £7,541 | Monthly buyer |
| 12856 | HIGH | 9 | 6 | 3.21 | 11.20 | 0.32 | 7 | £2,180 | Monthly buyer |
| 13078 | HIGH | 9 | 57 | 2.33 | 13.12 | 0.53 | 3 | £29,532 | Monthly buyer |

---

### 6E — Product tier summary

```sql
SELECT
    subscription_tier,
    COUNT(*)                                  AS product_count,
    ROUND(AVG(repeat_buyer_rate) * 100, 1)    AS avg_repeat_buyer_pct,
    ROUND(AVG(avg_reorder_days), 1)           AS avg_reorder_days,
    ROUND(AVG(unique_buyers), 0)              AS avg_unique_buyers,
    ROUND(AVG(avg_unit_price), 2)             AS avg_unit_price,
    ROUND(SUM(total_revenue), 2)              AS combined_revenue
FROM `my-project-8037-491318.a.vw_product_subscription_score`
GROUP BY subscription_tier
ORDER BY
    CASE subscription_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END;
```

**What it does:**
Same idea as 6C but for products. Three rows — one per tier — summarising the typical product profile in each group.

Note that `combined_revenue` uses `SUM` rather than `AVG` — this gives you the total revenue contribution from all products in each tier, which is useful for understanding the business value of subscription-candidate products as a group.

**Sample result (4 rows):**

| subscription_tier | product_count | avg_repeat_buyer_pct | avg_reorder_days | avg_unique_buyers | avg_unit_price | combined_revenue |
|---|---|---|---|---|---|---|
| HIGH | 129 | 31.8% | 43.8 | 139 | £3.72 | £771,307 |
| MEDIUM | 2,628 | 24.0% | 87.3 | 145 | £3.37 | £14,704,819 |
| LOW | 1,442 | 12.6% | 143.1 | 57 | £3.89 | £1,748,483 |

---

### 6F — Warmest leads

```sql
SELECT
    css.customer_id,
    css.subscription_tier                         AS customer_tier,
    css.total_score                               AS customer_score,
    ROUND(css.orders_per_month, 2)                AS orders_per_month,
    css.avg_days_between_orders,
    css.total_spend,
    COUNT(DISTINCT pss.stock_code)                AS high_tier_products_bought,
    STRING_AGG(DISTINCT pss.description,
               ' | ' ORDER BY pss.description)    AS high_tier_product_names
FROM `my-project-8037-491318.a.vw_customer_subscription_score` css
JOIN `my-project-8037-491318.a.vw_cleaned` c
    ON css.customer_id = c.customer_id
JOIN `my-project-8037-491318.a.vw_product_subscription_score` pss
    ON c.stock_code = pss.stock_code
   AND pss.subscription_tier = 'HIGH'
WHERE css.subscription_tier IN ('HIGH', 'MEDIUM')
GROUP BY
    css.customer_id, css.subscription_tier, css.total_score,
    css.orders_per_month, css.avg_days_between_orders, css.total_spend
ORDER BY
    CASE css.subscription_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 END,
    high_tier_products_bought DESC,
    css.total_score DESC;
```

**What it does:**
The most actionable query in the project. It finds customers who satisfy two conditions at once: they have a subscription-friendly ordering pattern (HIGH or MEDIUM tier) AND they have already purchased at least one HIGH-tier subscription product.

It does this with two JOINs — first joining to `vw_cleaned` to get which products each customer has bought, then joining to `vw_product_subscription_score` filtered to `subscription_tier = 'HIGH'` to keep only the purchases of top-tier products.

`COUNT(DISTINCT pss.stock_code)` counts how many different HIGH-tier products each customer has bought. A customer who has bought five subscription-friendly products is a warmer lead than one who has bought just one.

`STRING_AGG(DISTINCT pss.description, ' | ' ORDER BY pss.description)` concatenates all the HIGH-tier product names into a single readable string per customer, separated by pipes. This lets you see at a glance what subscription-friendly products each customer already buys.

**Sample result (4 rows):**

| customer_id | customer_tier | customer_score | orders_per_month | avg_days_between_orders | total_spend | high_tier_products_bought |
|---|---|---|---|---|---|---|
| 14911 | HIGH | 7 | 15.18 | 1.98 | £276,655 | 84 |
| 17841 | HIGH | 9 | 8.60 | 3.50 | £70,847 | 73 |
| 12748 | HIGH | 7 | 13.14 | 2.29 | £52,191 | 68 |
| 14298 | HIGH | 7 | 3.42 | 8.89 | £91,194 | 62 |

---

### 6G — Country breakdown

```sql
SELECT
    c.country,
    COUNT(DISTINCT css.customer_id)               AS candidate_customers,
    COUNTIF(css.subscription_tier = 'HIGH')       AS high_tier,
    COUNTIF(css.subscription_tier = 'MEDIUM')     AS medium_tier,
    ROUND(AVG(css.total_spend), 2)                AS avg_spend_per_customer
FROM `my-project-8037-491318.a.vw_customer_subscription_score` css
JOIN `my-project-8037-491318.a.vw_cleaned` c
    ON css.customer_id = c.customer_id
WHERE css.subscription_tier IN ('HIGH', 'MEDIUM')
GROUP BY c.country
ORDER BY candidate_customers DESC;
```

**What it does:**
Groups subscription candidates by country to identify the best geography for a pilot launch.

It joins `vw_customer_subscription_score` to `vw_cleaned` to get the country for each customer. `COUNTIF(css.subscription_tier = 'HIGH')` is a BigQuery shorthand that counts only the rows matching a condition — equivalent to `COUNT(CASE WHEN ... THEN 1 END)` in standard SQL. It lets us split the count into HIGH and MEDIUM in the same row without a subquery.

`GROUP BY c.country` produces one row per country. Ordered by total candidate count descending so the most promising markets appear first.

**Sample result (4 rows):**

| country | candidate_customers | high_tier | medium_tier | avg_spend_per_customer |
|---|---|---|---|---|
| United Kingdom | 2,113 | 500 | 1,613 | £5,678 |
| Germany | 49 | 21 | 28 | £6,584 |
| France | 47 | 17 | 30 | £6,167 |
| Belgium | 14 | 5 | 9 | £3,585 |
