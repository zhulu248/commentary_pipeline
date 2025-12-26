sqlite3 "..\commentary.db" "select count(*) as docs from documents;"
sqlite3 "..\commentary.db" "select count(*) as paras from paragraphs;"
sqlite3 "..\commentary.db" "select doc_type, count(*) from documents group by doc_type;"
