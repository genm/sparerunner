-- Node-reported runner isolation mode, adopted as observed state.
--
-- A node started with the opt-in shared runner identity mode executes jobs
-- under the agent's own uid instead of a dedicated per-runner uid, so the uid
-- isolation between the agent and the job it runs is absent. The controller
-- only records what an authenticated agent reported; it never infers or
-- overrides the value.
--
-- NULL means the node has never reported the property (an agent too old to know
-- it). NULL is deliberately not "isolated": silently rendering an unreported
-- node as the stronger mode is the one wrong answer an operator cannot detect,
-- so the management API leaves the field absent instead of defaulting it.
--
-- This is observation only. Capacity remains gated by native_runner_ready and
-- the node owner's availability intent, so no value here can admit or withhold
-- work.
ALTER TABLE agent_session_snapshots
    ADD COLUMN shared_runner_identity INTEGER
        CHECK (shared_runner_identity IS NULL
            OR shared_runner_identity IN (0, 1));
