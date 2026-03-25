terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

locals {
  notebook_base = "/Users/afonsomenesesrocha@gmail.com/notebooks"
}

resource "databricks_job" "olist_pipeline" {
  name = "olist_pipeline"

  queue {
    enabled = true
  }

  # ── SILVER ──────────────────────────────────────────────
  task {
    task_key = "silver_orders"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/01_silver_orders"
    }
  }

  task {
    task_key = "silver_products"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/02_silver_products"
    }
  }

  task {
    task_key = "silver_order_items"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/03_silver_order_items"
    }
  }

  task {
    task_key = "silver_reviews"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/04_silver_reviews"
    }
  }

  task {
    task_key = "silver_customers"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/05_silver_customers"
    }
  }

  task {
    task_key = "silver_sellers"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/06_silver_sellers"
    }
  }

  task {
    task_key = "silver_payments"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/07_silver_payments"
    }
  }

  task {
    task_key = "silver_geolocation"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/08_silver_geolocation"
    }
  }

  task {
    task_key = "silver_category"
    notebook_task {
      notebook_path = "${local.notebook_base}/silver/09_silver_product_category_name_translation"
    }
  }

  # ── DIMENSÕES GOLD ───────────────────────────────────────
  task {
    task_key = "gold_dim_tempo"
    depends_on { task_key = "silver_orders" }
    notebook_task {
      notebook_path = "${local.notebook_base}/gold/01_gold_dim_tempo"
    }
  }

  task {
    task_key = "gold_dim_cliente"
    depends_on { task_key = "silver_orders" }
    depends_on { task_key = "silver_customers" }
    notebook_task {
      notebook_path = "${local.notebook_base}/gold/02_gold_dim_cliente"
    }
  }

  task {
    task_key = "gold_dim_produto"
    depends_on { task_key = "silver_products" }
    notebook_task {
      notebook_path = "${local.notebook_base}/gold/03_gold_dim_produto"
    }
  }

  task {
    task_key = "gold_dim_seller"
    depends_on { task_key = "silver_sellers" }
    notebook_task {
      notebook_path = "${local.notebook_base}/gold/04_gold_dim_seller"
    }
  }

  # ── FATOS GOLD ───────────────────────────────────────────
  task {
    task_key = "gold_fato_itens_pedido"
    depends_on { task_key = "gold_dim_tempo" }
    depends_on { task_key = "gold_dim_cliente" }
    depends_on { task_key = "gold_dim_produto" }
    depends_on { task_key = "gold_dim_seller" }
    notebook_task {
      notebook_path = "${local.notebook_base}/gold/05_gold_fato_itens_pedido"
    }
  }

  task {
    task_key = "gold_fato_pagamentos"
    depends_on { task_key = "gold_dim_tempo" }
    depends_on { task_key = "gold_dim_cliente" }
    notebook_task {
      notebook_path = "${local.notebook_base}/gold/06_gold_fato_pagamentos"
    }
  }

  task {
    task_key = "gold_fato_avaliacoes"
    depends_on { task_key = "gold_dim_tempo" }
    depends_on { task_key = "gold_dim_cliente" }
    notebook_task {
      notebook_path = "${local.notebook_base}/gold/07_gold_fato_avaliacoes"
    }
  }
}