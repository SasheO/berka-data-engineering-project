-- "card_id" int;"disp_id" int;"type" low cardinality string;"issued" date time yymmdd hh:mm:ss
CREATE TABLE IF NOT EXISTS src_cards (
    card_id UInt64,
    disp_id UInt64,
    type LowCardinality(String),
    issued DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(issued)
ORDER BY (disp_id, issued, card_id);