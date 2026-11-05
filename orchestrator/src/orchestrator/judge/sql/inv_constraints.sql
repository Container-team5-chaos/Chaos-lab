-- inv_constraints (설계 5.4). I2·I4를 강제하는 제약이 없으면 행을 낸다(CONSTRAINT_MISSING).
-- CHECK 3개: accounts.cash >= 0, positions.qty >= 0, positions.cost_basis >= 0(검증된 것만).
-- UNIQUE 2개: orders(account_id, idem_key), fills(order_id). 부분·식 인덱스는 인정하지 않는다.
-- 테이블이 없어도 to_regclass가 NULL을 줘서 오류 없이 "없음" 행이 나온다.
-- 위반 행만 낸다. 빈 결과 = PASS.
WITH expected_check (table_name, column_name) AS (
    VALUES ('accounts', 'cash'), ('positions', 'qty'), ('positions', 'cost_basis')
),
expected_unique (table_name, columns) AS (
    VALUES ('orders', ARRAY['account_id', 'idem_key']), ('fills', ARRAY['order_id'])
),
check_found AS (
    SELECT e.table_name, e.column_name,
           EXISTS (
               SELECT 1 FROM pg_constraint c
               WHERE c.conrelid = to_regclass(e.table_name)
                 AND c.contype = 'c'
                 AND c.convalidated
                 AND regexp_replace(pg_get_constraintdef(c.oid), '[[:space:]()]', '', 'g')
                     ~ ('^CHECK(' || e.column_name || '>=0(::[a-z]+)?|0(::[a-z]+)?<=' || e.column_name || ')$')
           ) AS found
    FROM expected_check e
),
unique_found AS (
    SELECT u.table_name, u.columns,
           EXISTS (
               SELECT 1 FROM pg_index i
               WHERE i.indrelid = to_regclass(u.table_name)
                 AND i.indisunique
                 AND i.indisvalid
                 AND i.indpred IS NULL
                 AND i.indexprs IS NULL
                 AND ARRAY(
                         SELECT a.attname::text
                         FROM unnest(i.indkey::int2[]) AS k (attnum)
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                         ORDER BY 1
                     ) = ARRAY(SELECT x FROM unnest(u.columns) AS x ORDER BY 1)
           ) AS found
    FROM expected_unique u
)
SELECT 'check' AS kind, table_name, column_name AS columns
FROM check_found
WHERE NOT found
UNION ALL
SELECT 'unique', table_name, array_to_string(columns, ',')
FROM unique_found
WHERE NOT found
ORDER BY 1, 2, 3
