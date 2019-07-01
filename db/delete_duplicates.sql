-- deletes items with duplicate ids found in reddit data archive
-- from: https://stackoverflow.com/a/12963112

DELETE FROM "comments" a USING (
      SELECT MIN(ctid) as ctid, id
        FROM "comments" 
        GROUP BY id HAVING COUNT(*) > 1
      ) b
      WHERE a.id = b.id 
      AND a.ctid <> b.ctid; 
      
DELETE FROM "posts" a USING (
      SELECT MIN(ctid) as ctid, id
        FROM "posts" 
        GROUP BY id HAVING COUNT(*) > 1
      ) b
      WHERE a.id = b.id 
      AND a.ctid <> b.ctid; 