select
    website_session_id,
    session_started_at
from {{ ref('fct_sessions') }}
where landing_page is null
