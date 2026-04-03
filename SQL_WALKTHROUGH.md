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

Shows exactly how many rows each cleaning filter removed, in a single stacked `UNION ALL` query. Useful for documenting and presenting the cleaning process.

### 6B — Customer subscription candidates

Returns all HIGH and MEDIUM tier customers ranked by score and spend. Includes a `cadence_label` column that translates the average days between orders into plain English (Weekly, Monthly, Quarterly buyer).

### 6C — Customer tier summary

Three-row summary table — one row per tier — with averages for order count, spend, order gap, CV, and recency. Good for a dashboard headline or presentation slide.

### 6D — Product subscription candidates

Returns all HIGH and MEDIUM tier products with a `suggested_cadence` column that recommends a subscription interval based on the average reorder days.

### 6E — Product tier summary

Three-row summary table — one row per tier — with averages for repeat buyer rate, reorder days, unique buyers, price, and combined revenue.

### 6F — Warmest leads

Joins customer scores and product scores to find customers who have a repeat-purchase pattern *and* have already bought HIGH-tier products. These are the most actionable targets for a subscription pilot — they don't need to be convinced, they're already behaving like subscribers.

### 6G — Country breakdown

Groups subscription candidates by country to identify where to run a geographic pilot. 500 of 560 HIGH-tier customers are in the UK.
