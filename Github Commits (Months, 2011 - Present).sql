WITH gh_lang AS (
  SELECT
    gh_language.name AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(MONTH FROM events.created_at) AS month,
    JSON_EXTRACT_SCALAR(events.payload, '$.commit.sha') AS commit_id
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `bigquery-public-data.github_repos.languages` AS gh
    ON events.repo.name = gh.repo_name,
    UNNEST(gh.language) AS gh_language
  WHERE
    events.type = 'IssuesEvent' AND
    JSON_EXTRACT_SCALAR(events.payload, '$.action') = 'opened'
  GROUP BY language, year, month, commit_id
),
ght_lang AS (
  SELECT
    ght.language AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(MONTH FROM events.created_at) AS month,
    JSON_EXTRACT_SCALAR(events.payload, '$.commit.sha') AS commit_id
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `ghtorrent-bq.ght.project_languages` AS ght
    ON events.repo.id = ght.project_id
  WHERE
    events.type = 'IssuesEvent' AND
    JSON_EXTRACT_SCALAR(events.payload, '$.action') = 'opened'
  GROUP BY language, year, month, commit_id
),
combined_commits AS (
  SELECT * FROM gh_lang
  UNION DISTINCT
  SELECT * FROM ght_lang
)

SELECT
  language,
  year,
  month,
  COUNT(DISTINCT commit_id) AS commits
FROM combined_commits
GROUP BY language, year, month
ORDER BY year DESC, month DESC, commits DESC;
