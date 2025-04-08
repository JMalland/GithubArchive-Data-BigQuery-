WITH gh_lang AS (
  SELECT
    gh_language.name AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(QUARTER FROM events.created_at) AS quarter,
    JSON_EXTRACT_SCALAR(events.payload, '$.pull_request.user.id') AS pull_request_creator_id
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `bigquery-public-data.github_repos.languages` AS gh
    ON events.repo.name = gh.repo_name,
    UNNEST(gh.language) AS gh_language
  WHERE
    events.type = 'PullRequestEvent'
  GROUP BY language, year, quarter, pull_request_creator_id
),
ght_lang AS (
  SELECT
    ght.language AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(QUARTER FROM events.created_at) AS quarter,
    JSON_EXTRACT_SCALAR(events.payload, '$.pull_request.user.id') AS pull_request_creator_id
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `ghtorrent-bq.ght.project_languages` AS ght
    ON events.repo.id = ght.project_id
  WHERE
    events.type = 'PullRequestEvent'
  GROUP BY language, year, quarter, pull_request_creator_id
),
combined_pull_request_creators AS (
  SELECT * FROM gh_lang
  UNION DISTINCT
  SELECT * FROM ght_lang
)

SELECT
  language,
  year,
  quarter,
  COUNT(DISTINCT pull_request_creator_id) AS pull_request_creators
FROM combined_pull_request_creators
GROUP BY language, year, quarter
ORDER BY year DESC, quarter DESC, pull_request_creators DESC;
