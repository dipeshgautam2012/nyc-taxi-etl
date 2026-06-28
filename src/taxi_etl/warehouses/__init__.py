"""Warehouse backends — swap via config [warehouse] backend."""

from taxi_etl.warehouses.base_warehouse import BaseWarehouse
from taxi_etl.warehouses.warehouse_factory import make_warehouse

__all__ = ["BaseWarehouse", "make_warehouse"]
