# Untested -- Ran out of free trial :P
WITH github_pull_request_creators AS (
  SELECT
    COALESCE(ght.language, gh_language.name) AS language,
    EXTRACT(YEAR FROM events.created_at) AS year,
    EXTRACT(MONTH FROM events.created_at) AS month,
    COUNT(DISTINCT JSON_EXTRACT_SCALAR(events.payload, '$.pull_request.user.id')) AS pull_request_creators,
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
    language, year, month
)

SELECT
  language,
  year,
  month,
  pull_request_creators,
FROM 
  github_pull_request_creators
GROUP BY
  language, year, month, pull_request_creators
ORDER BY
  year DESC, month DESC, pull_request_creators DESC
