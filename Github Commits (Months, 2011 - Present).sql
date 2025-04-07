WITH github_commits AS (
  SELECT
    COALESCE(ght.language, gh_language.name) AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(MONTH FROM events.created_at) AS month,
    COUNT(DISTINCT events.id) AS commits,
  FROM
    `githubarchive.year.20*` AS events
  JOIN -- Find the repo's language with public github data
    `bigquery-public-data.github_repos.languages` AS gh
    ON
      events.repo.name = gh.repo_name,
      UNNEST(gh.language) AS gh_language
  LEFT OUTER JOIN -- Resolve not-found repos ghtorrent
    `ghtorrent-bq.ght.project_languages` AS ght
    ON
      events.repo.id = ght.project_id
  WHERE
    events.type = 'PushEvent' -- Only get Commits for this table
  GROUP BY
    language, year, month
)

SELECT
  language,
  year,
  month,
  commits
FROM 
  github_commits
GROUP BY
  language, year, month, commits
ORDER BY
  year DESC, month DESC, commits DESC