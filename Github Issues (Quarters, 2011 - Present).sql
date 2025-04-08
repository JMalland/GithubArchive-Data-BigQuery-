WITH gh_lang AS (
  SELECT
    gh_language.name AS language,
    EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', JSON_EXTRACT_SCALAR(payload, '$.issue.created_at'))) AS year,
    EXTRACT(QUARTER FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', JSON_EXTRACT_SCALAR(payload, '$.issue.created_at'))) AS quarter,
    JSON_EXTRACT_SCALAR(events.payload, '$.issue.id') AS issue_id
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `bigquery-public-data.github_repos.languages` AS gh
    ON events.repo.name = gh.repo_name,
    UNNEST(gh.language) AS gh_language
  WHERE
    events.type = 'IssuesEvent' AND
    JSON_EXTRACT_SCALAR(events.payload, '$.action') = 'opened'
  GROUP BY language, year, quarter, issue_id
),

ght_lang AS (
  SELECT
    ght.language AS language,
    EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', JSON_EXTRACT_SCALAR(payload, '$.issue.created_at'))) AS year,
    EXTRACT(QUARTER FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', JSON_EXTRACT_SCALAR(payload, '$.issue.created_at'))) AS quarter,
    JSON_EXTRACT_SCALAR(events.payload, '$.issue.id') AS issue_id
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `ghtorrent-bq.ght.project_languages` AS ght
    ON events.repo.id = ght.project_id
  WHERE
    events.type = 'IssuesEvent' AND
    JSON_EXTRACT_SCALAR(events.payload, '$.action') = 'opened'
  GROUP BY language, year, quarter, issue_id
),

combined_issues AS (
  SELECT * FROM gh_lang
  UNION DISTINCT
  SELECT * FROM ght_lang
)

SELECT
  language,
  year,
  quarter,
  COUNT(DISTINCT issue_id) AS issues
FROM combined_issues
GROUP BY language, year, quarter
ORDER BY year DESC, quarter DESC, issues DESC;
