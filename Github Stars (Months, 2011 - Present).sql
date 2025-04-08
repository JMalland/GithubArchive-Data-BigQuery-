WITH gh_lang AS (
  SELECT
    gh_language.name AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(MONTH FROM events.created_at) AS month,
    events.repo.id AS star_id
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `bigquery-public-data.github_repos.languages` AS gh
    ON events.repo.name = gh.repo_name,
    UNNEST(gh.language) AS gh_language
  WHERE
    events.type = 'WatchEvent'
  GROUP BY language, year, month, star_id
),
ght_lang AS (
  SELECT
    ght.language AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(MONTH FROM events.created_at) AS month,
    events.repo.id AS star_id
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `ghtorrent-bq.ght.project_languages` AS ght
    ON events.repo.id = ght.project_id
  WHERE
    events.type = 'WatchEvent'
  GROUP BY language, year, month, star_id
),
combined_stars AS (
  SELECT * FROM gh_lang
  UNION DISTINCT
  SELECT * FROM ght_lang
)

SELECT
  language,
  year,
  month,
  COUNT(DISTINCT star_id) AS stars
FROM combined_stars
GROUP BY language, year, month
ORDER BY year DESC, month DESC, stars DESC;
