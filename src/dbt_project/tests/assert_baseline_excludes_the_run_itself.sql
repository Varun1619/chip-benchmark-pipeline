-- A run must never contribute to the baseline it is judged against, or a
-- regression would partly cancel itself out and read as smaller than it is.
--
-- With a window of N rows ending one row before the current one, the sample
-- count can never exceed N. If it does, the frame is wrong.

select
    run_id,
    baseline_sample_count
from {{ ref('fct_run_baselines') }}
where baseline_sample_count > {{ var('baseline_window') }}
