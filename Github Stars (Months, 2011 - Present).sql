WITH github_stars AS (
  SELECT
    -- language,
    COALESCE(gh_language.name, ght.language) AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(MONTH FROM events.created_at) AS month,
    COUNT(DISTINCT events.repo.id) AS stars
  FROM
    `githubarchive.year.20*` AS events
  JOIN
    `bigquery-public-data.github_repos.languages` AS gh
    ON
      gh.repo_name = events.repo.name,
      UNNEST(gh.language) AS gh_language
  LEFT OUTER JOIN
    `ghtorrent-bq.ght.project_languages` AS ght
  ON
    events.repo.id = ght.project_id
  WHERE
    events.type = 'WatchEvent' -- Only get Stars for this table
  GROUP BY
    language, year, month
)

SELECT
  language,
  year,
  month,
  stars
FROM 
  github_stars
GROUP BY
  language, year, month, stars
ORDER BY
  year DESC, month DESC, stars DESC