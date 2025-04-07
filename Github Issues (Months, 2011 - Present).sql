WITH github_issues AS (
  SELECT
    COALESCE(ght.language, gh_language.name) AS language,
    EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', JSON_EXTRACT_SCALAR(payload, '$.issue.created_at'))) AS year,
    EXTRACT(MONTH FROM PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', JSON_EXTRACT_SCALAR(payload, '$.issue.created_at'))) AS month,
    COUNT(DISTINCT events.id) AS issues,
  FROM
    `githubarchive.day.20240101*` AS events
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
    events.type = 'IssuesEvent' -- Only get Issues for this table
    AND
    JSON_EXTRACT_SCALAR(events.payload, '$.action') = 'opened' -- Only select Issues that were Opened
  GROUP BY
    language, year, month
)

SELECT
  language,
  year,
  month,
  issues
FROM 
  github_issues
GROUP BY
  language, year, month, issues
ORDER BY
  year DESC, month DESC, issues DESC
