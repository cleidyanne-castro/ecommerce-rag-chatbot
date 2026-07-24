# E-commerce RAG Chatbot

Privacy-safe e-commerce assistant using RAG (Retrieval Augmented Generation), ready to answer product questions in Portuguese and English.

## Project Goals

- [x] Answer technical questions about products (price, dimensions, specifications)
- [ ] Recommend products based on customer needs
- [ ] Compare products across marketplaces
- [ ] Check order status
- [ ] Display tracking information
- [ ] Integrate with Shopify API
- [x] Use RAG for product knowledge with searchable embeddings
- [ ] Use APIs for real-time transactional data

**Legend:** [x] Implemented | [ ] In Progress/Planned

---

## Data Architecture - Medallion

This project follows the **Medallion Architecture** (Bronze → Silver → Gold) using **Databricks Delta Lake**.

### Bronze Layer (Raw Data)
**Schema:** `workspace.ecommerce_bronze`

#### Olist Dataset (Brazilian E-commerce)
- `olist_customers` - Customer data (99,441 rows)
- `olist_orders` - Order transactions (99,441 rows)
- `olist_order_items` - Order line items (112,650 rows)
- `olist_order_payments` - Payment records (103,886 rows)
- `olist_order_reviews` - Customer reviews (99,224 rows)
- `olist_products` - Product catalog (32,951 rows)
- `olist_sellers` - Seller information (3,095 rows)
- `product_category_translation` - PT→EN category translations (71 rows)

#### Amazon Dataset (International Products)
- `amazon_products` - Product catalog from Amazon US (10,002 rows)
  - Fields: product_name, brand_name, selling_price, category, dimensions, about_product, product_description, etc.
  - Technical metadata: `_ingested_at`, `_source_file_path`, `_source_file_name`

### Silver Layer (Clean & Validated)
**Schema:** `workspace.ecommerce_silver`

#### Olist Tables
- `customers` - Deduplicated customers with normalized addresses
- `orders` - Validated orders with timestamp fields
- `order_items` - Clean order line items with pricing
- `payments` - Individual payment records
- `payments_by_order` - Aggregated payments per order
- `products` - Products with PT/EN categories and dimensions
- `sellers` - Validated seller locations
- `order_reviews` - Customer reviews with scores (1-5)

#### Amazon Tables
- `amazon_products` (9,524 rows)
  - Cleaned, typed fields (price as DOUBLE, dimensions as STRING)
  - Deduplication by `product_id`
  - Quality filters: requires product_id, product_name, valid price
  
- `amazon_products_for_rag` (8,728 rows) **RAG-Ready**
  - Optimized for vector embeddings and semantic search
  - `searchable_text` field: concatenated product info in Portuguese
  - Format: `"Nome | Marca: X | Preco: US$ Y | Categoria: Z | Dimensoes: W | Descrição"`
  - `data_completeness_score` (1-5): quality metric for ranking
  - Only includes products with category information

### Gold Layer (Business Metrics)
**Schema:** `workspace.ecommerce_gold`

#### Planned Aggregations
- `amazon_products_by_category` - Product counts, avg/min/max prices per category
- `amazon_top_complete_products` - Top 100 products ranked by data completeness
- `amazon_products_by_price_range` - Products grouped by price tiers (Low/Medium/High/Premium)
- More Olist business metrics...

---

## Notebook Structure

### Data Engineering Pipelines

1. **00_setup_project.py** - Initial project configuration
2. **01_ingest_olist_bronze.py** - Ingest Olist CSVs to Bronze
3. **02_transform_olist_silver.py** - Clean & validate Olist → Silver
4. **03_build_olist_gold.py** - Create Olist business aggregations
5. **04_ingest_amazon_bronze.py** - Ingest Amazon CSVs to Bronze
6. **05_transform_amazon_silver.py** - Clean & validate Amazon → Silver + RAG prep
7. **06_gold_amazon.py** - Create Amazon business aggregations (practice notebook)

---

## RAG Integration

### Searchable Text Format

The `amazon_products_for_rag` table includes a `searchable_text` field optimized for embeddings:

```
Premier Energizer HardCase iPhone Charger | Marca: desconhecida | Preco: US$ 9.71 | Categoria: Sports & Outdoors | Dimensoes: 1.8 x 4.5 x 8.2 inches | Make sure this fits by entering your model number. | Energizer Official...
```

**Key Features:**
- Labels in Portuguese for Brazilian audience
- Prices clearly marked as **US$** (USD currency)
- Concatenated product name, brand, price, category, dimensions, and full description
- Quality score (1-5) for ranking search results

### Sample RAG Queries

```sql
-- Find iPhone products
SELECT product_name, price, searchable_text
FROM workspace.ecommerce_silver.amazon_products_for_rag
WHERE LOWER(product_name) LIKE '%iphone%';

-- Get high-quality products (completeness score ≥ 3)
SELECT product_name, category, data_completeness_score
FROM workspace.ecommerce_silver.amazon_products_for_rag
WHERE data_completeness_score >= 3
ORDER BY data_completeness_score DESC, price DESC;
```

---

## Business Rules

### Currency Handling
- **Amazon products**: Prices in **US Dollars (US$)**
  - Original source: Amazon US marketplace
  - Clearly labeled as "US$" in RAG text to avoid confusion with Brazilian Real (R$)
- **Olist products**: Prices in **Brazilian Real (R$)**
  - Original source: Brazilian marketplace

### Data Quality Standards

#### Bronze → Silver Transformation
1. **Deduplication**: Window functions partition by ID, keep most recent `_ingested_at`
2. **Type casting**: String prices → DOUBLE (using `try_cast` to handle malformed values)
3. **Null handling**: Drop rows missing critical fields (product_id, price, etc.)
4. **Column naming**: Snake_case, lowercase, no spaces (Delta Lake compatibility)

#### Silver → RAG Optimization
1. **Category requirement**: Only products with valid categories
2. **Completeness scoring**: 
   - Score 1: Basic info (name, price)
   - Score 2: + Category
   - Score 3: + Dimensions
   - Score 4: + Brand
   - Score 5: + Full description
3. **Text concatenation**: Pipe-separated fields for embedding models

---

## Tech Stack

### Current Implementation
- **Platform**: Databricks (Serverless CPU compute)
- **Storage**: Delta Lake (ACID transactions, time travel)
- **Processing**: PySpark (distributed data processing)
- **Query**: Databricks SQL
- **Format**: Delta tables with schema evolution

### Planned Integrations
- **Vector Database**: For product embeddings
- **LLM**: Amazon Bedrock (RAG retrieval + generation)
- **API**: FastAPI (product search endpoints)
- **Frontend**: Streamlit (chatbot interface)
- **E-commerce**: Shopify API (inventory sync)

---

## Datasets

### 1. Olist Brazilian E-Commerce (Public Dataset)
- **Source**: [Kaggle - Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- **Period**: 2016-2018
- **Size**: ~100k orders, 33k products
- **Language**: Portuguese (with English translations)

### 2. Amazon Products Sample (Marketing Sample)
- **Source**: Marketing sample dataset (Amazon US)
- **Period**: January 2020
- **Size**: 10k products
- **Language**: English (product names/descriptions)

---

## Getting Started

### Prerequisites
- Databricks workspace (AWS or Azure)
- Access to sample datasets (Olist + Amazon CSVs)
- Unity Catalog enabled

### Setup
1. Upload CSV files to `/Volumes/workspace/default/raw_data/`
2. Run notebooks in sequence (00 → 01 → 02 → ...)
3. Query Gold tables for business insights
4. Use `amazon_products_for_rag` table for RAG integration

---

## Data Statistics

| Dataset | Bronze | Silver | Gold (Planned) |
|---------|--------|--------|----------------|
| Olist Products | 32,951 | 32,157 | TBD |
| Amazon Products | 10,002 | 9,524 | TBD |
| Amazon RAG-Ready | - | 8,728 | - |
| Olist Orders | 99,441 | 99,209 | TBD |
| Olist Reviews | 99,224 | 98,673 | TBD |

**Data Quality**: ~95% retention rate from Bronze → Silver (quality filters applied)

---

## Next Steps

- [ ] Complete Gold layer aggregations for Amazon products
- [ ] Implement vector embeddings for `searchable_text` field
- [ ] Build FastAPI endpoints for product search
- [ ] Create RAG pipeline with Amazon Bedrock
- [ ] Develop Streamlit chatbot UI
- [ ] Integrate Shopify API for inventory sync
- [ ] Add multi-language support (PT-BR ↔ EN)

---

## Contributing

This is a learning project for RAG + E-commerce integration. Feedback and suggestions are welcome!

## License

MIT License - Free to use for educational purposes.
