CREATE SCHEMA pgmasq;

CREATE FUNCTION pgmasq.reauthenticate(rolename text)
RETURNS void LANGUAGE C
AS 'MODULE_PATHNAME', $$pgmasq_reauthenticate$$;
COMMENT ON FUNCTION pgmasq.reauthenticate(text)
IS 'reauthenticate as if we connected as the user';
