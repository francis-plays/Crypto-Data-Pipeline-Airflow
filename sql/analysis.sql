-- 1. Latest prices for all coins
SELECT coin, price_usd, change_24h, pulled_at
FROM crypto_market.raw.prices
ORDER BY pulled_at DESC;

-- 2. Average price per coin across all pulls
SELECT coin,
    ROUND(AVG(price_usd), 2) AS avg_price,
    ROUND(MIN(price_usd), 2) AS min_price,
    ROUND(MAX(price_usd), 2) AS max_price
FROM crypto_market.raw.prices
GROUP BY coin;

-- 3. Price change over time per coin
SELECT coin, price_usd, change_24h, pulled_at
FROM crypto_market.raw.prices
ORDER BY coin, pulled_at ASC;

-- 4. Most volatile coin (biggest 24h swings)
SELECT coin,
    ROUND(AVG(ABS(change_24h)), 4) AS avg_volatility
FROM crypto_market.raw.prices
GROUP BY coin
ORDER BY avg_volatility DESC;