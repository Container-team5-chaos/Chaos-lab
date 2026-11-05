-- inv_privileges (설계 5.4 선택 검사, 명세 6절 I9). 앱 계정 pt_app에 orders, fills의 UPDATE, DELETE 권한이 있으면 행을 낸다.
-- 역할이나 테이블이 없어도 to_regrole, to_regclass가 NULL을 줘서 오류가 나지 않는다.
-- pt_app이 없으면 확인할 수 없어 ROLE_MISSING 행을 낸다(조용히 PASS하지 않는다).
-- 위반 행만 낸다. 빈 결과 = PASS.
SELECT 'pt_app' AS role_name, t.table_name, p.privilege
FROM (VALUES ('orders'), ('fills')) AS t (table_name)
CROSS JOIN (VALUES ('UPDATE'), ('DELETE')) AS p (privilege)
WHERE to_regrole('pt_app') IS NOT NULL
  AND to_regclass(t.table_name) IS NOT NULL
  AND has_table_privilege(to_regrole('pt_app')::oid, to_regclass(t.table_name)::oid, p.privilege)
UNION ALL
SELECT 'pt_app', NULL, 'ROLE_MISSING'
WHERE to_regrole('pt_app') IS NULL
ORDER BY 1, 2, 3
