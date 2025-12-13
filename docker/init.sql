-- Star Schema Benchmark (SSB) Schema
-- Adapted from TiDB Bench

USE ssb;

-- Dimension: Part
CREATE TABLE IF NOT EXISTS part (
    p_partkey   BIGINT NOT NULL,
    p_name      VARCHAR(30) NOT NULL,
    p_mfgr      CHAR(10) NOT NULL,
    p_category  CHAR(10) NOT NULL,
    p_brand1    CHAR(10) NOT NULL,
    p_color     VARCHAR(20) NOT NULL,
    p_type      VARCHAR(30) NOT NULL,
    p_size      BIGINT NOT NULL,
    p_container CHAR(10) NOT NULL,
    PRIMARY KEY (p_partkey)
) ENGINE=InnoDB;

-- Dimension: Supplier
CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey BIGINT NOT NULL,
    s_name    CHAR(30) NOT NULL,
    s_address VARCHAR(30) NOT NULL,
    s_city    CHAR(20) NOT NULL,
    s_nation  CHAR(20) NOT NULL,
    s_region  CHAR(20) NOT NULL,
    s_phone   CHAR(20) NOT NULL,
    PRIMARY KEY (s_suppkey)
) ENGINE=InnoDB;

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS customer (
    c_custkey    BIGINT NOT NULL,
    c_name       VARCHAR(30) NOT NULL,
    c_address    VARCHAR(30) NOT NULL,
    c_city       CHAR(20) NOT NULL,
    c_nation     CHAR(20) NOT NULL,
    c_region     CHAR(20) NOT NULL,
    c_phone      CHAR(20) NOT NULL,
    c_mktsegment CHAR(20) NOT NULL,
    PRIMARY KEY (c_custkey)
) ENGINE=InnoDB;

-- Dimension: Date
CREATE TABLE IF NOT EXISTS `date` (
    d_datekey        BIGINT NOT NULL,
    d_date           CHAR(20) NOT NULL,
    d_dayofweek      CHAR(10) NOT NULL,
    d_month          CHAR(10) NOT NULL,
    d_year           BIGINT NOT NULL,
    d_yearmonthnum   BIGINT NOT NULL,
    d_yearmonth      CHAR(10) NOT NULL,
    d_daynuminmonth  BIGINT NOT NULL,
    d_daynuminyear   BIGINT NOT NULL,
    d_monthnuminyear BIGINT NOT NULL,
    d_weeknuminyear  BIGINT NOT NULL,
    d_sellingseason  CHAR(20) NOT NULL,
    d_lastdayinweekfl  BIGINT NOT NULL,
    d_lastdayinmonthfl BIGINT NOT NULL,
    d_holidayfl      BIGINT NOT NULL,
    d_weekdayfl      BIGINT NOT NULL,
    PRIMARY KEY (d_datekey)
) ENGINE=InnoDB;

-- Fact: Lineorder (main table for queries)
CREATE TABLE IF NOT EXISTS lineorder (
    lo_orderkey      BIGINT NOT NULL,
    lo_linenumber    BIGINT NOT NULL,
    lo_custkey       BIGINT NOT NULL,
    lo_partkey       BIGINT NOT NULL,
    lo_suppkey       BIGINT NOT NULL,
    lo_orderdate     BIGINT NOT NULL,
    lo_orderpriority CHAR(20) NOT NULL,
    lo_shippriority  CHAR(1) NOT NULL,
    lo_quantity      BIGINT NOT NULL,
    lo_extendedprice BIGINT NOT NULL,
    lo_ordtotalprice BIGINT NOT NULL,
    lo_discount      BIGINT NOT NULL,
    lo_revenue       BIGINT NOT NULL,
    lo_supplycost    BIGINT NOT NULL,
    lo_tax           BIGINT NOT NULL,
    lo_commitdate    BIGINT NOT NULL,
    lo_shipmode      CHAR(10) NOT NULL,
    PRIMARY KEY (lo_orderkey, lo_linenumber)
) ENGINE=InnoDB;

-- Indexes for better query performance

-- Supplier indexes (for region/nation filtering)
CREATE INDEX idx_supplier_region ON supplier(s_region);
CREATE INDEX idx_supplier_nation ON supplier(s_nation);

-- Customer indexes (for region/nation filtering)
CREATE INDEX idx_customer_region ON customer(c_region);
CREATE INDEX idx_customer_nation ON customer(c_nation);

-- Date indexes (for year filtering)
CREATE INDEX idx_date_year ON `date`(d_year);
CREATE INDEX idx_date_yearmonth ON `date`(d_yearmonthnum);

-- Part indexes (for category/brand filtering)
CREATE INDEX idx_part_category ON part(p_category);
CREATE INDEX idx_part_brand ON part(p_brand1);
CREATE INDEX idx_part_mfgr ON part(p_mfgr);

-- Lineorder indexes (for joins and filtering)
CREATE INDEX idx_lo_orderdate ON lineorder(lo_orderdate);
CREATE INDEX idx_lo_custkey ON lineorder(lo_custkey);
CREATE INDEX idx_lo_suppkey ON lineorder(lo_suppkey);
CREATE INDEX idx_lo_partkey ON lineorder(lo_partkey);
CREATE INDEX idx_lo_discount ON lineorder(lo_discount);
CREATE INDEX idx_lo_quantity ON lineorder(lo_quantity);
