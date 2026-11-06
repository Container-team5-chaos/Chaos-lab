-- 실험 시작 전 초기화: 모든 데이터를 지우고 테스트 사용자 1,000명에게 1,000만 원씩 지급

TRUNCATE orders, positions, accounts RESTART IDENTITY CASCADE;

INSERT INTO accounts (user_id, krw)
SELECT 'user-' || lpad(i::text, 4, '0'), 10000000
FROM generate_series(1, 1000) AS i;
