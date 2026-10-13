-- Names for the symbols the pipeline streams (adjust to the symbols you subscribe to).
-- On the Twelve Data Basic (free) plan only trial symbols stream; these three were accepted.
INSERT INTO crypto_assets (symbol, "name") VALUES
    ('BTC/USD', 'Bitcoin / US Dollar'),
    ('EUR/USD', 'Euro / US Dollar'),
    ('AAPL',    'Apple Inc')
ON CONFLICT (symbol) DO NOTHING;
