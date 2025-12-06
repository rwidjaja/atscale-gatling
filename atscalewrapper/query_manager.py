"""
query_manager.py
Handles base query SQL file creation and management.
"""

import os
from pathlib import Path


BASE_SQL_QUERY = """SELECT
    q.service,
    q.query_language,
    q.query_text AS inbound_text,
    MAX(s.subquery_text) AS outbound_text,
    p.cube_name,
    p.project_id,
    CASE WHEN MAX(s.subquery_text) LIKE '%as_agg_%' THEN TRUE ELSE FALSE END AS used_agg,
    COUNT(*)                             AS num_times,
    AVG(r.finished - p.planning_started) AS elapsed_time_in_seconds,
    AVG(r.result_size)                   AS avg_result_size
FROM
    atscale.queries q
INNER JOIN
    atscale.query_results r ON q.query_id = r.query_id
INNER JOIN
    atscale.queries_planned p ON q.query_id = p.query_id
INNER JOIN
    atscale.subqueries s ON q.query_id = s.query_id
WHERE
    q.query_language = ?
    AND p.planning_started > CURRENT_TIMESTAMP - INTERVAL '60 day'
    AND p.cube_name = ?
    AND q.service = 'user-query'
    AND r.succeeded = TRUE
    AND LENGTH(q.query_text) > 100
    AND q.query_text NOT LIKE '/* Virtual query to get the members of a level */%'
    AND q.query_text NOT LIKE '-- statement does not return rows%'
GROUP BY
    1, 2, 3, 5, 6
ORDER BY 3;
"""


class QueryManager:
    def __init__(self, config_dir: str = "working_dir/config"):
        self.config_dir = config_dir
        self.base_query_path = os.path.join(config_dir, "base_query.sql")
    
    def create_base_query_file(self) -> bool:
        """Ensure working_dir/config/base_query.sql exists."""
        try:
            os.makedirs(self.config_dir, exist_ok=True)
            
            if not os.path.exists(self.base_query_path):
                print(f"📝 Creating base_query.sql in {self.config_dir}")
                with open(self.base_query_path, "w") as f:
                    f.write(BASE_SQL_QUERY)
                print("✅ base_query.sql created successfully")
                return True
            else:
                print(f"✔ base_query.sql already exists at {self.base_query_path}")
                return True
                
        except Exception as e:
            print(f"❌ Failed to create base_query.sql: {e}")
            return False