-- 최근 X일 동안의 데이터만 가져오는 쿼리
SELECT *
FROM TB
WHERE date BETWEEN %s AND %s;
