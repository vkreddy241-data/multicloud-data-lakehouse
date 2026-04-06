terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.0" }
  }
  backend "azurerm" {
    resource_group_name  = "vkreddy-tf-state-rg"
    storage_account_name = "vkreddy-tf-state"
    container_name       = "tfstate"
    key                  = "multicloud-lakehouse/azure/terraform.tfstate"
  }
}

provider "azurerm" { features {} }

locals {
  tags = { Project = "multicloud-lakehouse", Owner = "vikas-reddy", ManagedBy = "terraform" }
}

resource "azurerm_resource_group" "lakehouse" {
  name     = "vkreddy-lakehouse-rg"
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------
# ADLS Gen2 storage account
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "lakehouse" {
  name                     = "vkreddylakehouse"
  resource_group_name      = azurerm_resource_group.lakehouse.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true   # hierarchical namespace = ADLS Gen2
  tags                     = local.tags
}

resource "azurerm_storage_container" "raw"    {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.lakehouse.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.lakehouse.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold"   {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.lakehouse.name
  container_access_type = "private"
}

# ---------------------------------------------------------------------------
# Azure Databricks workspace (for Spark/Delta on Azure)
# ---------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "lakehouse" {
  name                = "vkreddy-lakehouse-adb"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  sku                 = "premium"
  tags                = local.tags
}

# ---------------------------------------------------------------------------
# Azure Data Factory (for ADF pipeline triggering Bronze ingestion)
# ---------------------------------------------------------------------------
resource "azurerm_data_factory" "lakehouse" {
  name                = "vkreddy-lakehouse-adf"
  resource_group_name = azurerm_resource_group.lakehouse.name
  location            = var.location
  tags                = local.tags
  identity { type = "SystemAssigned" }
}
