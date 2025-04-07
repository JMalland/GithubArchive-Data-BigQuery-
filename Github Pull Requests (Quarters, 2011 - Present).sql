WITH github_pull_requests AS (
  SELECT
    COALESCE(ght.language, gh_language.name) AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(QUARTER FROM events.created_at) AS quarter,
    COUNT(DISTINCT events.id) AS pull_requests,
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
    events.type = 'PullRequestEvent' -- Only get Pull Requests for this table
  GROUP BY
    language, year, quarter
)

SELECT
  language,
  year,
  quarter,
  pull_requests,
FROM 
  github_pull_requests
GROUP BY
  language, year, quarter, pull_requests
ORDER BY
  year DESC, quarter DESC, pull_requests DESC