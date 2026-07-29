-- SPQR sharding configuration example
-- E-commerce application with sharded and reference tables

-- Distribution for user-scoped data, sharded by user_id
CREATE DISTRIBUTION ds_users COLUMN TYPES integer;

-- Distribution for product orders, sharded by order_id
CREATE DISTRIBUTION ds_orders COLUMN TYPES integer;

-- ==============================
-- Sharded relations (ds_users)
-- ==============================
ALTER DISTRIBUTION ds_users ATTACH RELATION users DISTRIBUTION KEY id;
ALTER DISTRIBUTION ds_users ATTACH RELATION user_addresses DISTRIBUTION KEY user_id;
ALTER DISTRIBUTION ds_users ATTACH RELATION user_sessions DISTRIBUTION KEY user_id;
ALTER DISTRIBUTION ds_users ATTACH RELATION shopping_carts DISTRIBUTION KEY user_id;
ALTER DISTRIBUTION ds_users ATTACH RELATION wishlists DISTRIBUTION KEY user_id;

-- ==============================
-- Sharded relations (ds_orders)
-- ==============================
ALTER DISTRIBUTION ds_orders ATTACH RELATION orders DISTRIBUTION KEY id;
ALTER DISTRIBUTION ds_orders ATTACH RELATION order_items DISTRIBUTION KEY order_id;
ALTER DISTRIBUTION ds_orders ATTACH RELATION payments DISTRIBUTION KEY order_id;
ALTER DISTRIBUTION ds_orders ATTACH RELATION shipments DISTRIBUTION KEY order_id;

-- ==============================
-- Reference tables (replicated to every shard)
-- ==============================
CREATE REFERENCE TABLE countries;
CREATE REFERENCE TABLE product_categories;
CREATE REFERENCE TABLE products;
CREATE REFERENCE TABLE currencies;

-- ==============================
-- Key ranges for ds_users
-- ==============================
CREATE KEY RANGE kru1 FROM 0     ROUTE TO sh1 FOR DISTRIBUTION ds_users;
CREATE KEY RANGE kru2 FROM 50000 ROUTE TO sh2 FOR DISTRIBUTION ds_users;

-- ==============================
-- Key ranges for ds_orders
-- ==============================
CREATE KEY RANGE kro1 FROM 0      ROUTE TO sh1 FOR DISTRIBUTION ds_orders;
CREATE KEY RANGE kro2 FROM 100000 ROUTE TO sh2 FOR DISTRIBUTION ds_orders;
