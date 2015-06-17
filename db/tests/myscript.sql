
SELECT
  a.Title,
  t.Name,
  t.UnitPrice
FROM
  Album a
INNER JOIN
  Track t
    on a.AlbumId = t.AlbumId;
