-- snapshot_schema (계약 정렬 명세 6, 7 constraints). 서비스 테이블의 열·제약 메타데이터.
-- kind
--   table       테이블이 있다(name = 테이블 이름)
--   column      열(detail = format_type + 'not null' 여부, 예: numeric(24,4) not null)
--   constraint  제약(name = 제약 이름, detail = pg_get_constraintdef, 검증 안 된 제약은 끝에 NOT VALID)
-- 테이블은 to_regclass로 찾는다(앱과 같은 search_path 이름 해석). 없는 테이블은 행이 없다(오류 없음).
-- 판정(기대 스키마와 비교)은 invariants.check_constraints가 한다.
-- psql 변수 schema_tables는 db.build_bundle이 정한다.
WITH t AS (
    SELECT to_regclass(x) AS oid
    FROM unnest(:'schema_tables'::text[]) AS x
)
SELECT 'table' AS kind, c.relname::text AS table_name, c.relname::text AS name,
       NULL::text AS detail
FROM pg_class c
JOIN t ON t.oid = c.oid
UNION ALL
SELECT 'column', c.relname::text, a.attname::text,
       format_type(a.atttypid, a.atttypmod)
       || CASE WHEN a.attnotnull THEN ' not null' ELSE '' END
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN t ON t.oid = c.oid
WHERE a.attnum > 0 AND NOT a.attisdropped
UNION ALL
SELECT 'constraint', c.relname::text, con.conname::text, pg_get_constraintdef(con.oid)
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN t ON t.oid = c.oid
ORDER BY 1, 2, 3
