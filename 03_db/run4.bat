sqlite3 "..\commentary.db" "select count(*) from verse_mentions;"
sqlite3 "..\commentary.db" "select book_osis, count(*) from verse_mentions group by book_osis order by count(*) desc limit 20;"
sqlite3 "..\commentary.db" "select raw_match, book_osis, chapter, verse_start, verse_end, parse_status from verse_mentions limit 30;"
