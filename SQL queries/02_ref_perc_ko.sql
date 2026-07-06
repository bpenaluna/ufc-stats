SELECT
	ref,
	COUNT(*) AS num_all,
	SUM(CASE WHEN method = 'KO/TKO' THEN 1 ELSE 0 END) AS num_ko,
	SUM(CASE WHEN method = 'KO/TKO' THEN 1 ELSE 0 END) / (1.0 * NULLIF(COUNT(*), 0)) AS pct
FROM fight_stats
WHERE ref IS NOT NULL
GROUP BY ref
ORDER BY num_all DESC, pct DESC;